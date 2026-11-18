"""Audited, resumable source refresh for index snapshots required by acceptance."""
import argparse
import json
from pathlib import Path

from betalens_db_manager import DatabaseManagerService, Settings
from betalens_db_manager.canonical import builtin_catalog
from betalens_db_manager.pipeline import Pipeline
from betalens_db_manager.util import atomic_json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--index', default='000906.SH')
    parser.add_argument('--start', default='2005-01-01')
    parser.add_argument('--end', default='2026-11-06')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    service = DatabaseManagerService(Settings.from_env(args.env))
    saved = args.output / 'repair.json'
    report = json.loads(saved.read_text()) if saved.exists() else {'index_code': args.index}
    # Register the two already-present physical columns, without rebuilding tables.
    original = service.registry.get('index_membership')
    definition = json.loads(json.dumps(original))
    names = {f['name'] for f in definition['fields']}
    definition['fields'].extend(f for f in builtin_catalog()['index_membership'].fields
                                if f['name'] in {'snapshot_complete', 'snapshot_member_count'} and f['name'] not in names)
    report['definition_before'] = report.get('definition_before', original)
    report['definition_after'] = definition
    atomic_json(saved, report)
    if not args.execute:
        return
    if definition != original:
        service.registry.save(definition)
    from betalens_db_manager.field_lineage import producers_for
    document = service.mappings.document('index_membership')
    mapping = document['mapping']
    proof_columns = {'snapshot_complete', 'snapshot_member_count'}
    mapping['producers'] = [p for p in mapping.get('producers', []) if p['column'] not in proof_columns]
    mapping['producers'].extend(p for p in producers_for(service.registry.spec('index_membership'), mapping)
                                if p['column'] in proof_columns)
    service.mappings.save(mapping, document['file_hash'])
    plan = service.schema.plan('index_membership')
    report['schema_preview'] = plan
    atomic_json(saved, report)
    if any(t['blocked'] or t['requires_rebuild'] or t['removed'] or t['key_change'] for t in plan['targets']):
        raise ValueError('Unexpected destructive schema change; repair stopped')
    report['schema_result'] = service.schema.apply(plan['token'])
    atomic_json(saved, report)
    if report['schema_result']['status'] != 'completed':
        raise RuntimeError('Schema registration did not complete on both targets')
    if 'job' not in report:
        item = service.plan({'tables': ['index_membership'], 'source_id': 'tushare',
                             'endpoints': ['index_weight'], 'codes': [args.index],
                             'start_date': args.start, 'end_date': args.end,
                             'targets': ['postgres', 'clickhouse'], 'mode': 'refresh_source',
                             'auto_commit': False, 'download_workers': 2,
                             'normalization_workers': 1, 'validation_profile': 'light',
                             'post_check_rules': ['values', 'readback']})
        report['job'] = item['id']
        atomic_json(saved, report)
    pipeline = Pipeline(service)
    current = service.state.job(report['job'])
    if current['status'] not in {'ready_to_commit', 'completed'}:
        pipeline.run(report['job'])
    current = service.state.job(report['job'])
    report['download_audit_status'] = current['status']
    atomic_json(saved, report)
    if current['status'] == 'ready_to_commit':
        # The user's approved acceptance plan authorizes this targeted repair.
        # Pipeline commit consumes the verified immutable journals and performs
        # per-target conflict checks/readback; it does not bypass the audit stage.
        pipeline.commit(report['job'])
    result = pipeline.detail(report['job'], compact=True)
    report['result'] = result
    atomic_json(saved, report)
    print(json.dumps({'job': report['job'], 'status': result['status']}), flush=True)
    if result['status'] != 'completed':
        raise RuntimeError('Index proof repair did not complete')


if __name__ == '__main__':
    main()
