"""Refresh the two stale fund profile snapshots identified by the business audit."""
import argparse
import json
from pathlib import Path

from betalens_db_manager import DatabaseManagerService, Settings
from betalens_db_manager.pipeline import Pipeline
from betalens_db_manager.publishing import existing
from betalens_db_manager.util import atomic_json, digest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    service = DatabaseManagerService(Settings.from_env(args.env))
    spec = service.registry.spec('fund_profile')
    report = {'cases': []}
    for code, column in [('016537.OF', 'c_fee'), ('005778.OF', 'benchmark')]:
        previous = args.output / (code + '.repair.json')
        if previous.exists():
            old = json.loads(previous.read_text())
            if old.get('verified'):
                report['cases'].append(old)
                continue
        service.source._download_stop = None
        service.source.active_job = None
        fields = [f['name'] for f in service.sources.endpoint('tushare', 'fund_basic')['fields']]
        path = args.output / (code + '.json.gz')
        data, sha = service.source.fetch('fund_basic', {'ts_code': code, 'market': 'O', 'status': 'L'}, fields, path)
        values = [dict(zip(data['fields'], row)) for row in data['items']]
        selected = [row for row in values if row['ts_code'] == code]
        if len(selected) != 1:
            raise ValueError('Source control does not identify exactly one requested fund')
        case = {'code': code, 'column': column, 'control_sha256': sha, 'control': selected[0], 'before': {}}
        for target in ['postgres', 'clickhouse']:
            adapter = service.pg if target == 'postgres' else service.ch
            case['before'][target] = list(existing(adapter, spec, [{'security_code': code}]).values())
        # Refresh only a proven discrepancy, through the standard journaled pipeline.
        current = selected[0].get(column)
        def equal(a, b):
            from decimal import Decimal
            if column == 'c_fee':
                return Decimal(str(a)) == Decimal(str(b))
            return a == b
        changed = any(not rows or not equal(rows[0].get(column), current) for rows in case['before'].values())
        if changed:
            item = service.plan({'tables': ['fund_profile'], 'source_id': 'tushare', 'endpoints': ['fund_basic'],
                                 'codes': [code], 'parameters': {'fund_basic': {'ts_code': code, 'market': 'O', 'status': 'L'}},
                                 'start_date': '2005-01-01', 'end_date': '2026-11-15', 'targets': ['postgres', 'clickhouse'],
                                 'mode': 'refresh_source', 'auto_commit': False, 'download_workers': 1,
                                 'normalization_workers': 1, 'post_check_rules': []})
            case['job'] = item['id']
            atomic_json(args.output / (code + '.repair.json'), case)
            pipeline = Pipeline(service)
            result = pipeline.run(item['id'])
            if result['status'] != 'ready_to_commit':
                raise ValueError('Fund refresh failed its source/normalization audit')
            result = pipeline.commit(item['id'])
            case['status'] = result['status']
        else:
            case['status'] = 'already_current'
        case['after'] = {}
        for target in ['postgres', 'clickhouse']:
            adapter = service.pg if target == 'postgres' else service.ch
            case['after'][target] = list(existing(adapter, spec, [{'security_code': code}]).values())
        case['verified'] = all(len(rows) == 1 and equal(rows[0].get(column), current) for rows in case['after'].values())
        atomic_json(args.output / (code + '.repair.json'), case)
        report['cases'].append(case)
        atomic_json(args.output / 'fund-profiles.json', report)
        print(code, case['status'], case['verified'], flush=True)
    if not all(c['verified'] for c in report['cases']):
        raise ValueError('Fund profile readback mismatch')


if __name__ == '__main__':
    main()
