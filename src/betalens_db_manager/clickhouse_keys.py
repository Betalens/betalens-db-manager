"""Query-scoped typed key tables; no persistent DDL or writer lock for lookups."""
from dataclasses import replace
from .comparison import typed
from .resource_budget import BUDGET
from .ch_partition import live_partition, month_id


def partition_filter(spec,rows,field):
    if field not in spec.keys:
        return ''
    dates=[r.get(field) for r in rows]
    if not dates or any(d is None for d in dates):return ''
    months=sorted({month_id(d) for d in dates})
    return f'toYYYYMM(`{field}`) IN ('+','.join(months)+') AND '


def existing_partitions(db,spec,key_source,external_tables=None,table=None):
    keys=','.join('`'+k+'`' for k in spec.keys)
    query=f'SELECT DISTINCT _partition_id AS partition FROM {db.table(table or spec.name)} WHERE ({keys}) IN (SELECT {keys} FROM {key_source}) SETTINGS max_threads={BUDGET.ch_threads} FORMAT JSONEachRow'
    kwargs={'external_tables':external_tables} if external_tables else {}
    return {row['partition'] for row in db._json_rows(db.execute(query,**kwargs))}


def scoped_filter(db,spec,rows,key_source,field,external_tables=None,table=None):
    direct=partition_filter(spec,rows,field)
    if direct:return direct
    if field is None:return ''
    # The usual daily tables key on trade_date, while physically partitioning
    # on event_date. Discover actual partitions from key columns only; never
    # assume the two dates match, which could hide an old/corrupt version.
    parts=existing_partitions(db,spec,key_source,external_tables,table)
    if not parts:return '0 AND '
    return '_partition_id IN ('+','.join(db._literal(p) for p in sorted(parts))+') AND '


def find_existing(db,spec,rows,projection=None):
    from .pipeline_writer import arrow_bytes
    fields=tuple(f for f in spec.fields if f['name'] in spec.keys)
    key_spec=replace(spec,fields=fields)
    field=live_partition(db,spec)
    unique={}
    for row in rows:
        key=typed(spec,row,keys_only=True)
        if any(key[k] is None for k in spec.keys):raise ValueError(spec.name+': 查重主键不能为空')
        unique[tuple(key[k] for k in spec.keys)]=key
    keys=list(unique.values());result={};name='_betalens_keys'
    columns=','.join('`'+k+'`' for k in spec.keys)
    structure=','.join('`'+f['name']+'` '+db._type(f,True) for f in fields)
    # Bound rows and Arrow bytes independently. SQL text no longer scales with
    # key count. Each key packet serves a single, automatically cleaned query.
    selected_fields='*' if projection is None else ','.join('`'+f['name']+'`' for f in spec.fields if f['name'] in projection or f['name'] in spec.keys)
    result_spec=spec if projection is None else replace(spec,fields=tuple(f for f in spec.fields if f['name'] in projection or f['name'] in spec.keys))
    pending=[keys[i:i+BUDGET.ch_key_rows] for i in range(0,len(keys),BUDGET.ch_key_rows)]
    while pending:
        packet=pending.pop(0);data=arrow_bytes(key_spec,packet)
        if len(data)>BUDGET.ch_key_bytes and len(packet)>1:
            middle=len(packet)//2;pending[:0]=[packet[:middle],packet[middle:]];continue
        if len(data)>BUDGET.ch_key_bytes:raise ValueError('单个查重键超过大小上限')
        external={name:{'format':'ArrowStream','structure':structure,'data':data}}
        predicate=scoped_filter(db,spec,packet,name,field,external)
        if predicate=='0 AND ':continue
        selected=f'SELECT {selected_fields} FROM {db.table(spec.name)} FINAL WHERE {predicate}({columns}) IN (SELECT {columns} FROM {name})'
        query=f'SELECT t.* FROM ({selected}) AS t INNER JOIN {name} AS k USING ({columns}) SETTINGS max_threads={BUDGET.ch_threads} FORMAT JSONEachRow'
        for row in db._json_rows(db.execute(query,external_tables=external)):
            row=typed(result_spec,row);result[tuple(row[k] for k in spec.keys)]=row
    return result
