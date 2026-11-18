"""Independent direct-field checks against fresh, archived source controls."""
import argparse
from datetime import date
from decimal import Decimal, localcontext
import json
from pathlib import Path

from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from betalens_db_manager import DatabaseManagerService, Settings
from betalens_db_manager.util import atomic_json, parse_date


CONTROLS = [
    ('daily', 'market_daily_raw', {'ts_code': '000001.SZ', 'trade_date': '20240102'}, 'stock'),
    ('daily_basic', 'market_daily_raw', {'ts_code': '000001.SZ', 'trade_date': '20240102'}, 'stock'),
    ('fund_daily', 'market_daily_raw', {'ts_code': '510300.SH', 'trade_date': '20240102'}, 'fund'),
    ('index_daily', 'market_daily_raw', {'ts_code': '000906.SH', 'start_date': '20240102', 'end_date': '20240102'}, 'index'),
    ('fund_share', 'fund_shares_daily', {'ts_code': '510300.SH', 'trade_date': '20240102'}, 'fund'),
    ('stock_basic', 'stock_profile', {'ts_code': '000001.SZ', 'list_status': 'L'}, 'stock'),
    *[(endpoint + '_vip', 'fundamental_periodic', {'ts_code': '000001.SZ', 'period': '20231231'}, 'stock')
      for endpoint in ['income', 'balancesheet', 'cashflow', 'fina_indicator']],
]


def normalized(value, field):
    if value is None or value == '':
        return None
    if field['type'] == 'numeric':
        with localcontext() as ctx:
            ctx.prec = 80
            return Decimal(str(value)).quantize(Decimal(1).scaleb(-field.get('scale', 12)))
    if field['type'] == 'integer':
        return int(value)
    if field['type'] == 'date':
        return parse_date(value)
    return str(value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    service = DatabaseManagerService(Settings.from_env(args.env))
    report = {'controls': [], 'status': 'running', 'basis': 'fresh_source_controls; research historical revisions may differ'}
    for number, (endpoint, table, params, asset) in enumerate(CONTROLS):
        path = args.output / (str(number) + '-' + endpoint + '.json')
        if path.exists():
            report['controls'].append(json.loads(path.read_text()))
            continue
        result = {'endpoint': endpoint, 'table': table, 'parameters': params, 'comparisons': 0, 'rows': [], 'passed': False}
        print('source', endpoint, flush=True)
        try:
            fields = [f['name'] for f in service.sources.endpoint('tushare', endpoint)['fields']]
            data, sha = service.source.fetch(endpoint, params, fields, path.with_suffix('.json.gz'))
            result['sha256'] = sha
            spec = service.registry.spec(table)
            types = {f['name']: f for f in spec.fields}
            binding = next(b for b in service.mappings.get(table)['bindings'] if b['source_id'] == 'tushare' and b['endpoint'] == endpoint)
            actuals = []
            for raw_values in data['items'][:10]:
                raw = dict(zip(data['fields'], raw_values))
                expected = {}
                for rule in binding['fields']:
                    column = rule['column']
                    if endpoint == 'daily_basic' and column == 'close':
                        continue  # The declared daily quote source owns close.
                    value = raw.get(rule['source_field'])
                    if value in (None, '') or rule.get('builtin') or rule.get('formula') or column not in types:
                        continue
                    if types[column]['type'] == 'numeric':
                        with localcontext() as ctx:
                            ctx.prec = 80
                            value = Decimal(str(value)) * Decimal(str(rule.get('multiplier', '1')))
                    expected[column] = normalized(value, types[column])
                conditions = {'security_code': raw.get('ts_code')}
                if table == 'market_daily_raw':
                    conditions.update(asset_type=asset, trade_date=parse_date(raw['trade_date']))
                elif table in {'stock_quote_supplement_daily', 'fund_shares_daily'}:
                    conditions['trade_date'] = parse_date(raw['trade_date'])
                elif table == 'fundamental_periodic':
                    conditions.update(period_end=parse_date(raw['end_date']), statement_type=endpoint.removesuffix('_vip'),
                                      report_type=str(raw.get('report_type') or 'unspecified'))
                columns = list(expected)
                sample = {'key': conditions, 'expected_fields': len(expected), 'targets': {}}
                for target in ['postgres', 'clickhouse']:
                    if target == 'postgres':
                        with service.pg.connect(readonly=True) as connection, connection.cursor(cursor_factory=RealDictCursor) as cursor:
                            query = sql.SQL('SELECT {} FROM {} WHERE {}').format(
                                sql.SQL(',').join(map(sql.Identifier, columns)), sql.Identifier(service.settings.pg_schema, table),
                                sql.SQL(' AND ').join(sql.SQL('{}=%s').format(sql.Identifier(c)) for c in conditions))
                            cursor.execute(query, list(conditions.values()))
                            rows = [dict(row) for row in cursor.fetchall()]
                    else:
                        ch = service.ch
                        predicate = ' AND '.join('`' + c + '`=' + ch._literal(v) for c, v in conditions.items())
                        query = 'SELECT ' + ','.join('`' + c + '`' for c in columns) + ' FROM ' + ch.table(table) + ' FINAL WHERE ' + predicate
                        rows = [json.loads(line, parse_float=Decimal) for line in ch.execute(query + ' FORMAT JSONEachRow').splitlines() if line]
                    mismatches = [[c for c in expected if normalized(row.get(c), types[c]) != expected[c]] for row in rows]
                    matched = any(not values for values in mismatches)
                    sample['targets'][target] = {'candidate_versions': len(rows), 'matched': matched,
                                                  'mismatch_fields': min(mismatches, key=len) if mismatches else columns}
                    result['comparisons'] += len(expected)
                result['rows'].append(sample)
            result['passed'] = bool(result['rows']) and all(all(t['matched'] for t in r['targets'].values()) for r in result['rows'])
        except Exception as exc:
            result['error'] = service.safe_error(exc)
        atomic_json(path, result)
        report['controls'].append(result)
        atomic_json(args.output / 'source-audit.json', report)
    report['status'] = 'passed' if all(r['passed'] for r in report['controls']) else 'failed'
    atomic_json(args.output / 'source-audit.json', report)


if __name__ == '__main__':
    main()
