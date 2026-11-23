from dataclasses import replace
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest

from betalens_db_manager import ch_repartition as migration
from betalens_db_manager.canonical import builtin_catalog
from betalens_db_manager.ch_partition import desired_partition_field, live_partition
from betalens_db_manager.clickhouse import ClickHouse
from betalens_db_manager.comparison import compare
from betalens_db_manager.config import Settings
from betalens_db_manager.pipeline_store import PipelineStore
from betalens_db_manager.pipeline_writer import ch_apply, ch_insert
from test_batched_writes import key_spec
from test_manager import live_manager


def test_partition_contract_is_based_on_the_date_business_key():
    spec = key_spec()
    db = ClickHouse(Settings())
    assert desired_partition_field(spec) == 'trade_date'
    assert 'PARTITION BY toYYYYMM(trade_date)' in db.ddl(spec)
    actual = {'engine': 'ReplacingMergeTree', 'partition_key': 'toYYYYMM(event_date)'}
    assert live_partition(db, spec, actual=actual) == 'event_date'
    with pytest.raises(ValueError, match='不符'):
        live_partition(db, spec, actual={**actual, 'partition_key': 'toYYYYMM(downloaded_at)'})


@pytest.mark.parametrize('resume', [False, True])
def test_migration_preserves_rows_and_recovers_exchange(live_manager, monkeypatch, resume):
    service = live_manager
    db = service.ch
    spec = key_spec()
    monkeypatch.setattr(service, 'registry', SimpleNamespace(catalog=lambda: {spec.name: spec}))
    db.execute(db.ddl(spec, partition_key='toYYYYMM(event_date)'))
    PipelineStore(service.state)
    old = {'code': 'A', 'trade_date': date(2025, 1, 31), 'event_date': date(2024, 11, 5), 'amount': Decimal('3.14')}
    other = {'code': 'B', 'trade_date': date(2025, 2, 1), 'event_date': date(2024, 11, 5), 'amount': Decimal('2')}
    change = compare(spec, [old, other], [], 'fill_missing')
    assert ch_apply(db, service.state, 'seed', 'first', spec, change)['applied'] == 2
    preview = migration.preview(service)
    assert preview['blocked'] == []
    token = preview['token']
    db_execute = db.execute
    calls = []

    def lose_reply(sql, *args, **kwargs):
        answer = db_execute(sql, *args, **kwargs)
        if sql.startswith('EXCHANGE TABLES') and not calls:
            calls.append(sql)
            raise RuntimeError('simulated lost exchange reply')
        return answer

    with pytest.raises(ValueError, match='维护模式'):
        migration.execute(service, token)
    (service.settings.state_dir / 'maintenance.json').write_text('{"enabled":true}')
    monkeypatch.setattr(db, 'execute', lose_reply)
    with pytest.raises(RuntimeError, match='lost exchange'):
        migration.execute(service, token)
    result = migration.execute(service, token, rollback=not resume)
    assert result['status'] == ('completed' if resume else 'rolled_back')
    assert len(db._find_existing(spec, [old, other])) == 2
    if resume:
        actual = migration._tables(db)
        assert actual[spec.name]['partition_key'] == 'toYYYYMM(trade_date)'
        assert actual[result['tables'][spec.name]['candidate']]['partition_key'] == 'toYYYYMM(event_date)'
        changed = {**old, 'event_date': date(2025, 3, 1), 'amount': Decimal('4')}
        comparison = compare(spec, [changed], [old], 'refresh_source')
        assert ch_apply(db, service.state, 'update', 'batch', spec, comparison)['applied'] == 1
        journal = change['changes'][1]
        assert journal['after']['code'] == 'B'
        inverse = {'changes': [{**journal, 'before': journal['after'], 'after': None,
                                'before_hash': journal['after_hash'], 'after_hash': None}]}
        assert ch_apply(db, service.state, 'seed', 'inverse', spec, inverse, inverse=True)['verified']
        assert list(db._find_existing(spec, [old, other])) == [('A', old['trade_date'])]
    else:
        assert migration._tables(db)[spec.name]['partition_key'] == 'toYYYYMM(event_date)'


def test_reverses_partial_exchange_including_unpartitioned_table(live_manager, monkeypatch):
    service, db = live_manager, live_manager.ch
    base = key_spec()
    spare = replace(base, name='backup_probe', fields=tuple(
        {**f, 'nullable': True} if f['name'] in {'trade_date', 'event_date'} else f for f in base.fields))
    specs = {spec.name: spec for spec in (base, spare)}
    monkeypatch.setattr(service, 'registry', SimpleNamespace(catalog=lambda: specs))
    for spec, partition in ((base, 'toYYYYMM(event_date)'), (spare, 'tuple()')):
        sql = db.ddl(spec, partition_key=partition).replace(' PARTITION BY tuple()', '')
        db.execute(sql)
        ch_insert(db, spec, db.table(spec.name), [{'code': 'A', 'trade_date': date(2025, 1, 1),
                  'event_date': date(2024, 12, 31), 'amount': Decimal('1')}])
    plan = migration.preview(service)
    assert plan['blocked'] == []
    assert plan['tables'][spare.name]['partition_key'] == 'tuple()'
    (service.settings.state_dir / 'maintenance.json').write_text('{"enabled":true}')
    actual_execute = db.execute

    def interrupt(sql, *args, **kwargs):
        if sql.startswith(f'EXCHANGE TABLES {db.table(base.name)} '):
            raise RuntimeError('stop before second exchange')
        return actual_execute(sql, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(db, 'execute', interrupt)
        with pytest.raises(RuntimeError, match='second exchange'):
            migration.execute(service, plan['token'])
    tables = migration._tables(db)
    assert tables[spare.name]['uuid'] != plan['tables'][spare.name]['uuid']
    assert tables[base.name]['uuid'] == plan['tables'][base.name]['uuid']
    assert migration.execute(service, plan['token'], rollback=True)['status'] == 'rolled_back'
    assert {name: migration._tables(db)[name]['uuid'] for name in specs} == {
        name: row['uuid'] for name, row in plan['tables'].items()}


def test_conflicting_physical_versions_block_publication(live_manager, monkeypatch):
    service, db, spec = live_manager, live_manager.ch, key_spec()
    monkeypatch.setattr(service, 'registry', SimpleNamespace(catalog=lambda: {spec.name: spec}))
    db.execute(db.ddl(spec, partition_key='toYYYYMM(event_date)'))
    old = {'code': 'A', 'trade_date': date(2025, 1, 1), 'event_date': date(2024, 12, 31), 'amount': Decimal('1')}
    ch_insert(db, spec, db.table(spec.name), [old, {**old, 'event_date': date(2025, 1, 1), 'amount': Decimal('2')}])
    plan = migration.preview(service)
    (service.settings.state_dir / 'maintenance.json').write_text('{"enabled":true}')
    with pytest.raises(ValueError, match='冲突的物理版本'):
        migration.execute(service, plan['token'])
    assert migration._tables(db)[spec.name]['uuid'] == plan['tables'][spec.name]['uuid']
    assert migration.execute(service, plan['token'], rollback=True)['status'] == 'rolled_back'
    retry = migration.preview(service)
    assert retry['previous_rollback'] == plan['token'] and retry['token'] != plan['token']


def test_repartition_replays_existing_index_winner_without_deduplicating(live_manager, monkeypatch):
    service, db = live_manager, live_manager.ch
    spec = builtin_catalog()['index_membership']
    monkeypatch.setattr(service, 'registry', SimpleNamespace(catalog=lambda: {spec.name: spec}))
    db.execute(db.ddl(spec, partition_key='toYYYYMM(event_date)'))
    migration._freeze_merges(db, [spec.name])
    table = db.table(spec.name)
    fields = 'index_code,security_code,trade_date,event_date,source_record_hash,weight,snapshot_complete,snapshot_member_count'
    for evidence in ('1,1', '1,1', 'NULL,NULL'):
        db.execute(f"INSERT INTO {table} ({fields}) VALUES "
                   f"('I','A','2025-01-31','2025-02-01','same',1,{evidence})")
    before_final = db.execute(f'SELECT snapshot_complete FROM {table} FINAL FORMAT TSV')
    assert before_final == r'\N'
    plan = migration.preview(service)
    (service.settings.state_dir / 'maintenance.json').write_text('{"enabled":true}')
    # Capture the physical row count while merges are still stopped.
    actual_execute = db.execute
    physical = []

    def count_before_merges(sql, *args, **kwargs):
        if sql == 'SYSTEM START MERGES ' + table:
            physical.append(actual_execute(f'SELECT count() FROM {table}'))
        return actual_execute(sql, *args, **kwargs)

    monkeypatch.setattr(db, 'execute', count_before_merges)
    run = migration.execute(service, plan['token'])
    assert run['status'] == 'completed'
    assert physical == ['3']
    assert run['tables'][spec.name]['replayed_conflicts'] == {'202501': 1}
    assert db.execute(f'SELECT snapshot_complete FROM {table} FINAL FORMAT TSV') == before_final


def test_completed_inverse_makes_failed_forward_receipt_drained(live_manager):
    service = live_manager
    from betalens_db_manager.pipeline_store import PipelineStore
    store = PipelineStore(service.state)
    with service.state.connect() as conn:
        conn.execute("INSERT INTO jobs(id,created,updated,status,target,plan) VALUES(?,?,?,?,?,?)",
                     ('rollback-job', '2026-01-01', '2026-01-01', 'rolled_back', 'target', '{}'))
    manifest = {'table': 'trade_status_daily'}
    store.prepare('rollback-job', 'batch', 'clickhouse', manifest)
    store.acknowledge('rollback-job', 'batch', 'clickhouse', {}, 'memory limit')
    store.prepare('rollback-job', 'rollback:batch', 'clickhouse', manifest)
    store.acknowledge('rollback-job', 'rollback:batch', 'clickhouse', {'verified': True})
    migration._assert_drained(service, {'trade_status_daily'})
