"""ClickHouse physical partition contracts shared by DDL and writers."""

from .util import parse_date


def partition_expression(field):
    return f"toYYYYMM({field})" if field else "tuple()"


def normalize_partition(expression):
    return ''.join((expression or 'tuple()').replace('`', '').split())


def legacy_partition_field(spec):
    return next((f['name'] for f in spec.fields if f['name'] == 'event_date'
                 and not f.get('nullable', True)), None)


def desired_partition_field(spec):
    if 'trade_date' in spec.keys and any(f['name'] == 'trade_date' and f['type'] == 'date'
                                        for f in spec.fields):
        # The DDL makes every business key non-null, even if a source field is nullable.
        return 'trade_date'
    return legacy_partition_field(spec)


def month_id(value):
    if value is None:
        raise ValueError('ClickHouse 分区日期不能为空')
    day = parse_date(value)
    return f'{day.year:04d}{day.month:02d}'


def layout(db, name):
    rows = db._json_rows(db.execute(
        'SELECT toString(uuid) AS uuid,engine,partition_key FROM system.tables '
        f'WHERE database={db._literal(db.settings.ch_database)} AND name={db._literal(name)} FORMAT JSONEachRow'))
    if len(rows) != 1:
        raise ValueError(f'{name}: 无法确认 ClickHouse 物理表')
    return {**rows[0], 'partition_key': normalize_partition(rows[0]['partition_key'])}


def live_partition(db, spec, table=None, actual=None):
    actual = layout(db, table or spec.name) if actual is None else actual
    for field in (desired_partition_field(spec), legacy_partition_field(spec)):
        if actual['engine'] == 'ReplacingMergeTree' and actual['partition_key'] == partition_expression(field):
            return field
    raise ValueError(f'{spec.name}: ClickHouse 物理分区与受支持的布局不符')
