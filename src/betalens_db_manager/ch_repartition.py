"""Explicit maintenance migration for trading-date ClickHouse partitions."""

import re
import time
from collections import defaultdict

from .ch_partition import desired_partition_field, legacy_partition_field, normalize_partition, partition_expression
from .execution_policy import maintenance
from .util import digest


MIGRATIONS = 'ch_repartition'
TARGET = 'toYYYYMM(trade_date)'
FINAL_SETTINGS = 'do_not_merge_across_partitions_select_final=0,optimize_move_to_prewhere_if_final=0'
# Repartition runs beside the ClickHouse server and must leave headroom for
# background merges, part metadata, and the server itself.  The previous
# full-row tuple hash could otherwise consume the entire 32 GiB container
# before ClickHouse's own memory tracker stopped it.  External aggregation
# keeps the validation bounded and spills large hash states to the data disk.
REPARTITION_SETTINGS = (
    'max_threads=1,max_memory_usage=8589934592,'
    'max_bytes_before_external_group_by=1073741824,'
    'max_bytes_before_external_sort=1073741824,'
    'distributed_aggregation_memory_efficient=1'
)
INDEX_EVIDENCE = frozenset({'snapshot_complete', 'snapshot_member_count', 'downloaded_at',
                            'ingested_at', 'import_task_id', 'source_row_no'})


def _tables(db):
    return {row['name']: row for row in db._json_rows(db.execute(
        'SELECT name,toString(uuid) AS uuid,engine,engine_full,sorting_key,partition_key,create_table_query FROM system.tables '
        f'WHERE database={db._literal(db.settings.ch_database)} FORMAT JSONEachRow'))}


def _create_sql(db, table, candidate, expression, statement):
    header = re.compile(r'\ACREATE TABLE\s+(?:`?\w+`?\.)?`?' + re.escape(table) + r'`?\s*(?=\()')
    statement, count = header.subn(f'CREATE TABLE {db.table(candidate)} ', statement, count=1)
    if not count:
        raise ValueError(f'{table}: 无法保留源表 DDL，请检查建表语句')
    if expression != 'tuple()':
        field = re.fullmatch(r'toYYYYMM\((event_date|trade_date)\)', expression)
        if not field:
            raise ValueError(f'{table}: 不支持的源分区键')
        statement, count = re.subn(r'\bPARTITION BY\s+toYYYYMM\s*\(\s*`?' + field[1] + r'`?\s*\)',
                                   'PARTITION BY ' + TARGET, statement, count=1)
    else:
        statement, count = re.subn(r'\bPARTITION BY\s+tuple\s*\(\s*\)', 'PARTITION BY ' + TARGET, statement, count=1)
        if not count:
            statement, count = re.subn(r'(\bENGINE\s*=\s*ReplacingMergeTree(?:\s*\([^)]*\))?)',
                                       r'\1 PARTITION BY ' + TARGET, statement, count=1)
    if not count:
        raise ValueError(f'{table}: 无法替换分区表达式')
    return statement


def preview(service):
    db = service.ch
    actual = _tables(db)
    supports_exchange = db.execute('SELECT engine FROM system.databases WHERE '
                                   f'name={db._literal(db.settings.ch_database)} FORMAT TSVRaw') == 'Atomic'
    columns = defaultdict(dict)
    for row in db._json_rows(db.execute('SELECT table,name,type FROM system.columns WHERE '
                                       f'database={db._literal(db.settings.ch_database)} ORDER BY table,position FORMAT JSONEachRow')):
        columns[row['table']][row['name']] = row['type']
    size = defaultdict(int)
    for row in db._json_rows(db.execute('SELECT table,sum(bytes_on_disk) AS size FROM system.parts WHERE active AND '
                                       f'database={db._literal(db.settings.ch_database)} GROUP BY table FORMAT JSONEachRow')):
        size[row['table']] = int(row['size'])
    targets, blocked = {}, []
    for name, spec in sorted(service.registry.catalog().items()):
        if desired_partition_field(spec) != 'trade_date' or name not in actual:
            continue
        row = actual[name]
        partition = normalize_partition(row['partition_key'])
        if partition == TARGET:
            continue
        sorting = tuple(k.strip().strip('`') for k in row['sorting_key'].split(','))
        if (row['engine'] != 'ReplacingMergeTree' or sorting != tuple(spec.keys)
                or not re.match(r'^ReplacingMergeTree(?:\(\))?\s', row['engine_full'])
                or partition != partition_expression(legacy_partition_field(spec))
                or columns[name].get('trade_date') not in {'Date', 'Date32'} or not row['uuid'].strip('0-')):
            blocked.append(f'{name}: 源表布局不受支持')
            continue
        targets[name] = {'uuid': row['uuid'], 'partition_key': partition, 'ddl_hash': digest(row['create_table_query']),
                         'columns': list(columns[name]), 'keys': list(spec.keys), 'bytes': size[name]}
    if not supports_exchange:
        blocked.append('目标库须为 Atomic')
    if targets and db.execute(f'CHECK GRANT SYSTEM MERGES ON `{db.settings.ch_database}`.*') != '1':
        blocked.append('维护账号缺少当前库的 SYSTEM MERGES 权限')
    # Require space even if all new parts land on one disk.
    disks = db._json_rows(db.execute('SELECT name,free_space FROM system.disks FORMAT JSONEachRow'))
    total = sum(row['bytes'] for row in targets.values())
    needed = (total * 13 + 9) // 10
    if targets and (not disks or min(int(row['free_space']) for row in disks) < needed):
        blocked.append('可用空间不足，候选表暂按在用数据的 130% 预留')
    frozen = {'version': 2, 'fingerprint': db.settings.fingerprint,
              'tables': {name: {k: v for k, v in row.items() if k != 'bytes'} for name, row in targets.items()}}
    token = digest(frozen)
    while (service.state.ledger_get(MIGRATIONS, token) or {}).get('status') == 'rolled_back':
        frozen['previous_rollback'] = token
        token = digest(frozen)
    return {**frozen, 'token': token, 'source_bytes': total, 'required_free_bytes': needed, 'blocked': blocked}


def _assert_drained(service, tables):
    with service.state.connect() as conn:
        if conn.execute("SELECT 1 FROM tasks WHERE status='running' LIMIT 1").fetchone():
            raise ValueError('管理器仍有运行中的任务')
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='pipeline_receipts'").fetchone():
            return
        pending = conn.execute(
            "SELECT r.job,r.batch,r.status,j.status AS job_status,u.status AS inverse_status "
            "FROM pipeline_receipts r LEFT JOIN jobs j ON j.id=r.job "
            "LEFT JOIN pipeline_receipts u ON u.job=r.job AND u.target=r.target "
            "AND u.batch='rollback:'||r.batch "
            "WHERE r.target='clickhouse' AND r.status!='completed' "
            "AND json_extract(r.manifest,'$.table') IN (" + ','.join('?' for _ in tables) + ')', tuple(tables)).fetchall()
        unresolved = [row for row in pending if not (
            row['status'] == 'failed' and row['job_status'] in {'rolled_back', 'partially_rolled_back'}
            and row['inverse_status'] == 'completed')]
        if unresolved:
            raise ValueError('迁移涉及未决的 ClickHouse 提交或回退回执，须先恢复原任务')


def _freeze_merges(db, names):
    for name in names:
        db.execute('SYSTEM STOP MERGES ' + db.table(name))
    for _ in range(150):
        active = db.execute(f'SELECT count() FROM system.merges WHERE database={db._literal(db.settings.ch_database)} '
                            f'AND table IN ({",".join(db._literal(n) for n in names)})')
        if int(active) == 0:
            return
        time.sleep(.2)
    raise ValueError('等待 ClickHouse 后台合并停止超时')


def _fields(columns):
    return ','.join('`'+col.replace('`', '``')+'`' for col in columns)


def _month_where(db, month, partitions=()):
    where = f' WHERE toYYYYMM(trade_date)={int(month)}'
    if partitions:
        where += ' AND _partition_id IN (' + ','.join(db._literal(p) for p in partitions) + ')'
    return where


def _parts(db, name, columns, *, final=False, month=None, partitions=()):
    source = db.table(name) + (' FINAL' if final else '')
    fields = _fields(columns)
    # Variadic hash calls avoid materialising a wide tuple for every row.
    hashed = f'cityHash64({fields})'
    check = f'sipHash64({fields})'
    where = _month_where(db, month, partitions) if month is not None else ''
    settings = f'SETTINGS {FINAL_SETTINGS},{REPARTITION_SETTINGS} ' if final else f'SETTINGS {REPARTITION_SETTINGS} '
    return db._json_rows(db.execute(
        f'SELECT toYYYYMM(trade_date) AS month,count() AS rows,sumWithOverflow({hashed}) AS checksum, '
        f'groupBitXor({check}) AS check_xor FROM {source}{where} '
        f'GROUP BY month ORDER BY month {settings}FORMAT JSONEachRow', timeout=1800))


def _partition_map(db, name):
    partitions = defaultdict(list)
    for row in db._json_rows(db.execute(
        f'SELECT toYYYYMM(trade_date) AS month,_partition_id FROM {db.table(name)} '
        'GROUP BY month,_partition_id ORDER BY month,_partition_id FORMAT JSONEachRow', timeout=1800)):
        partitions[int(row['month'])].append(row['_partition_id'])
    return partitions


def _conflict_count(db, name, entry, month, where):
    fields = _fields(entry['columns'])
    keys = _fields(entry['keys'])
    is_index = name == 'index_membership' and {'snapshot_complete', 'snapshot_member_count'} <= set(entry['columns'])
    stable = _fields(c for c in entry['columns'] if c not in INDEX_EVIDENCE) if is_index else fields
    # A key split across old partitions can have multiple winners under partition-local FINAL.
    rows = db._json_rows(db.execute(
        'SELECT countIf(variants>1) AS differing,countIf(stable_variants>1 OR source_partitions>1) AS blocked '
        f'FROM (SELECT uniqUpTo(1)(tuple({fields})) AS variants, '
        f'uniqUpTo(1)(tuple({stable})) AS stable_variants,uniqUpTo(1)(_partition_id) AS source_partitions '
        f'FROM {db.table(name)}{where} GROUP BY {keys} HAVING count()>1) '
        f'SETTINGS {REPARTITION_SETTINGS} FORMAT JSONEachRow', timeout=1800))
    if int(rows[0]['blocked']):
        raise ValueError(f'{name}: {month} 存在冲突的物理版本或跨源分区的重复键，未发布')
    return int(rows[0]['differing'])


def _copy_month(db, name, candidate, columns, where, differing):
    fields = _fields(columns)
    insert = f'INSERT INTO {db.table(candidate)} ({fields}) '
    select = f'SELECT {fields} FROM {db.table(name)}'
    if not differing:
        db.execute(insert + select + where + ' SETTINGS optimize_on_insert=0', timeout=1800)
        return
    final = select + ' FINAL' + where
    settings = f'SETTINGS {FINAL_SETTINGS},{REPARTITION_SETTINGS},max_insert_threads=1,optimize_on_insert=0'
    # Preserve the complete physical multiset, then put each existing FINAL winner in a newer part.
    values = ','.join(f'tupleElement(full_row,{i})' for i in range(1, len(columns) + 1))
    rows = f'SELECT tuple({fields}) AS full_row, {{delta}} AS delta FROM {db.table(name)}'
    difference = (
        f'SELECT full_row,sum(delta) AS copies FROM ({rows.format(delta=1)}{where} '
        f'UNION ALL {rows.format(delta=-1)} FINAL{where}) GROUP BY full_row HAVING copies>0'
    )
    db.execute(f'{insert}SELECT {values} FROM ({difference}) ARRAY JOIN range(toUInt64(copies)) {settings}', timeout=1800)
    db.execute(f'{insert}{final} {settings}', timeout=1800)


def _prepare(db, name, entry, save):
    candidate = entry['candidate']
    tables = _tables(db)
    if tables[name]['uuid'] != entry['uuid'] or digest(tables[name]['create_table_query']) != entry['ddl_hash']:
        raise ValueError(f'{name}: 源表身份或结构已改变')
    expected = _create_sql(db, name, candidate, entry['partition_key'], tables[name]['create_table_query'])
    if candidate not in tables:
        db.execute(expected)
        tables = _tables(db)
    candidate_row = tables[candidate]
    if (not candidate_row['create_table_query'].replace('`', '').split() == expected.replace('`', '').split()
            or (entry.get('candidate_uuid') and entry['candidate_uuid'] != candidate_row['uuid'])):
        raise ValueError(f'{name}: 候选表身份或结构不符')
    entry['candidate_uuid'] = candidate_row['uuid']
    save()
    _freeze_merges(db, [name, candidate])
    cols = entry['columns']
    partitions = _partition_map(db, name)
    physical = _parts(db, name, cols)
    copied = {row['month']: row for row in _parts(db, candidate, cols)}
    # Keep logical summaries month-scoped.  A single full-table FINAL query is
    # too wide for duplicate-heavy production tables, while month-scoped
    # source/candidate comparisons preserve the same end-to-end proof.
    logical_by_month = {row['month']: row for row in physical}
    replayed = entry.setdefault('replayed_conflicts', {})
    if set(copied) - {row['month'] for row in physical}:
        raise ValueError(f'{name}: 候选表有未知月份')
    for row in physical:
        month = int(row['month'])
        if not 101 <= month <= 999912 or not 1 <= month % 100 <= 12:
            raise ValueError(f'{name}: 无效分区月份 {month}')
        if not partitions.get(month):
            raise ValueError(f'{name}: {month} 找不到源分区')
        where = _month_where(db, month, partitions[month])
        differing = _conflict_count(db, name, entry, month, where)
        source_final = _parts(db, name, cols, final=True, month=month, partitions=partitions[month])
        if len(source_final) != 1:
            raise ValueError(f'{name}: {month} FINAL 摘要不唯一，未发布')
        logical_by_month[month] = source_final[0]
        if copied.get(row['month']) == row and _parts(db, candidate, cols, final=True, month=month) == source_final:
            if differing:
                replayed[str(month)] = differing
            continue
        if row['month'] in copied:
            db.execute(f'ALTER TABLE {db.table(candidate)} DROP PARTITION {month}')
        _copy_month(db, name, candidate, cols, where, differing)
        staged = _parts(db, candidate, cols, month=month)
        if staged != [row]:
            raise ValueError(f'{name}: {month} 物理行摘要不符 (源 {row["rows"]} 行，候选 {staged[0]["rows"] if staged else 0} 行)')
        if _parts(db, candidate, cols, final=True, month=month) != source_final:
            raise ValueError(f'{name}: {month} FINAL 版本不符，未发布')
        if differing:
            replayed[str(month)] = differing
    logical = [logical_by_month[month] for month in sorted(logical_by_month)]
    if _parts(db, candidate, cols) != physical:
        raise ValueError(f'{name}: 候选表核验失败，未发布')
    entry.update(physical=physical, logical=logical, ready=True)
    save()


def execute(service, token, *, rollback=False):
    if not maintenance(service.settings.state_dir):
        raise ValueError('ClickHouse 重分区只允许在维护模式下执行')
    state, db = service.state, service.ch
    with db.write_window('ch-repartition:' + token):
        run = state.ledger_get(MIGRATIONS, token)
        if not run:
            if rollback:
                raise ValueError('没有对应的迁移记录')
            plan = preview(service)
            if plan['token'] != token or plan['blocked']:
                raise ValueError('迁移预检无效或被阻止：' + '；'.join(plan['blocked']))
            if not plan['tables']:
                return {'status': 'unchanged', 'tables': []}
            run = {'status': 'staging', 'fingerprint': plan['fingerprint'], 'tables': {
                name: {**row, 'candidate': 'repart_' + digest((token, name))[:24]}
                for name, row in plan['tables'].items()}}
            if set(run['tables']) & set().union(*(set(r['tables']) for r in state.ledger_rows(MIGRATIONS)
                                                if r['status'] not in {'completed', 'rolled_back'})):
                raise ValueError('这些表已有未完成的重分区，请使用原令牌恢复')
        if run['fingerprint'] != db.settings.fingerprint:
            raise ValueError('迁移记录不属于当前目标数据库')
        if run['status'] in {'completed', 'rolled_back'}:
            if rollback and run['status'] == 'completed':
                raise ValueError('迁移已完成；须先制定切换后增量的恢复方案')
            return run
        _assert_drained(service, run['tables'])

        def save():
            state.ledger_put(MIGRATIONS, token, run)

        save()
        names = list(run['tables'])
        if not rollback and run['status'] == 'staging':
            for name in names:
                _prepare(db, name, run['tables'][name], save)
            run['status'] = 'publishing'
            save()
        if rollback:
            run['status'] = 'rolling_back'
            save()
        reverse = run['status'] == 'rolling_back'
        for name in reversed(names) if reverse else names:
            entry = run['tables'][name]
            candidate = entry['candidate']
            tables = _tables(db)
            current = tables[name]['uuid']
            old, new = entry['uuid'], entry.get('candidate_uuid')
            if not new and reverse and current == old:
                continue
            if {current, tables[candidate]['uuid']} != {old, new}:
                raise ValueError(f'{name}: 表身份改变，停止发布')
            if current != (old if reverse else new):
                db.execute(f'EXCHANGE TABLES {db.table(name)} AND {db.table(candidate)}')
            confirmed = _tables(db)[name]
            if (confirmed['uuid'] != (old if reverse else new) or normalize_partition(confirmed['partition_key']) !=
                    (entry['partition_key'] if reverse else TARGET)):
                raise ValueError(f'{name}: 交换后物理结构不符')
            entry['exchanged'] = not reverse
            save()
        for name, entry in run['tables'].items():
            db.execute('SYSTEM START MERGES ' + db.table(name))
            if entry.get('candidate_uuid'):
                db.execute('SYSTEM START MERGES ' + db.table(entry['candidate']))
        run['status'] = 'rolled_back' if reverse else 'completed'
        save()
        return run
