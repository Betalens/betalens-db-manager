"""Publish archived industry projections through audited db-manager file tasks."""
import argparse
import csv
import hashlib
import json
from pathlib import Path

from betalens_db_manager import DatabaseManagerService, Settings
from betalens_db_manager.files import import_file
from betalens_db_manager.industry_history import RESEARCH_FIELDS, project, scheme_windows
from betalens_db_manager.pipeline import Pipeline
from betalens_db_manager.registry import GENERATED
from betalens_db_manager.source import read_archive
from betalens_db_manager.util import atomic_json, digest, now


def source_inputs(extraction):
    for chain in extraction['chains']:
        if not chain.get('complete') or not chain['pages'] or chain['pages'][-1]['rows'] != 0:
            raise ValueError('Source chain has no verified terminal empty page')
        offset, seen = 0, set()
        for page in chain['pages']:
            if page['parameters']['offset'] != offset:
                raise ValueError('Source page offsets are not contiguous')
            data = read_archive(Path(page['archive']), page['sha256'])
            if len(data['items']) != page['rows']:
                raise ValueError('Source page row count changed')
            for values in data['items']:
                raw = dict(zip(data['fields'], values))
                key = digest(raw)
                if key in seen:
                    raise ValueError('Repeated source page record')
                seen.add(key)
                yield {'scheme': chain['scheme'], 'endpoint': chain['endpoint'], 'row': raw,
                       'archive': page['archive'], 'archive_sha256': page['sha256']}
            offset += len(data['items'])
        if offset != chain['rows']:
            raise ValueError('Source chain row total changed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    root = args.output
    source_root = root / 'extraction'
    extraction = json.loads((source_root / 'extraction.json').read_text())
    if extraction['status'] != 'completed' or extraction['failures']:
        raise ValueError('Source extraction did not complete')
    dictionaries = json.loads((source_root / 'dictionaries.json').read_text())
    inputs = list(source_inputs(extraction))
    # Independent, unfiltered responses agree with the union of all complete
    # first-level page chains. Do not use one large response as paging proof.
    global_checks = []
    for endpoint in ['index_member_all','ci_index_member']:
        for flag in ['Y','N']:
            path = root / f'{endpoint}-global-{flag}.json.gz'
            metadata = json.loads(path.with_suffix('.meta.json').read_text())
            data = read_archive(path,metadata['sha256'])
            if metadata['params'] != {'is_new':flag}:
                raise ValueError('Independent global check has unexpected filters')
            global_rows = {digest(dict(zip(data['fields'],v))) for v in data['items']}
            partition_rows = {digest(v['row']) for v in inputs if v['endpoint']==endpoint and v['row']['is_new']==flag}
            check = dict(endpoint=endpoint,is_new=flag,global_rows=len(global_rows),partition_rows=len(partition_rows),
                         missing_from_partitions=sorted(global_rows-partition_rows),extra_in_partitions=sorted(partition_rows-global_rows),
                         archive=str(path),sha256=metadata['sha256'])
            global_checks.append(check)
    atomic_json(root/'partition-crosscheck.json',global_checks)
    if any(c['missing_from_partitions'] or c['extra_in_partitions'] for c in global_checks):
        raise ValueError('Complete industry partitions differ from independent global source responses')
    projected = project(dictionaries, inputs, extraction['start_date'], extraction['end_date'])
    atomic_json(root / 'projection-audit.json', {k: v for k, v in projected.items()
                                                if k not in {'industry_dim', 'security_industry_membership', 'lineage'}})
    atomic_json(root / 'lineage.json', projected['lineage'])
    if projected['rejected']:
        raise ValueError('Industry source projection has rejected rows; inspect projection-audit.json')
    service = DatabaseManagerService(Settings.from_env(args.env))
    receipt_path = root / 'import.json'
    receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else {'created_at': now(), 'tasks': {}}
    with service.pg.connect(readonly=True) as connection, connection.cursor() as cursor:
        cursor.execute(f'SELECT * FROM "{service.settings.pg_schema}".industry_scheme_dim ORDER BY scheme_code')
        names = [column.name for column in cursor.description]
        existing = [dict(zip(names, row)) for row in cursor.fetchall()]
    if 'scheme_metadata_before' not in receipt:
        receipt['scheme_metadata_before'] = existing
    # Applicability is an explicit research choice, kept separate from the
    # existing publication/event metadata rather than moving those dates.
    windows = scheme_windows(receipt['scheme_metadata_before'], extraction['start_date'])
    projected['industry_scheme_dim'] = windows
    receipt.update(source_manifest_sha256=hashlib.sha256((source_root / 'extraction.json').read_bytes()).hexdigest(),
                   source_chains=len(extraction['chains']), membership_counts=projected['membership_counts'],
                   version_policy='SW2014 before 2021-12-13; SW2021 from 2021-12-13; research backcast from 2005-01-01')
    atomic_json(receipt_path, receipt)
    if not args.execute:
        print(json.dumps({'status': 'projected', 'membership_counts': projected['membership_counts'],
                          'dimensions': len(projected['industry_dim']), 'rejected': 0,
                          'quarantined':len(projected['quarantined']),'source_overlaps':len(projected['overlaps'])}), flush=True)
        return

    # Add only nullable research policy columns; retain keys and all old fields.
    definition = service.registry.get('industry_scheme_dim')
    known = {field['name'] for field in definition['fields']}
    if any(field['name'] not in known for field in RESEARCH_FIELDS):
        definition['fields'].extend(field for field in RESEARCH_FIELDS if field['name'] not in known)
        service.registry.save(definition)
    schema = service.schema.plan('industry_scheme_dim')
    receipt['schema_preview'] = schema
    atomic_json(receipt_path, receipt)
    if any(target['requires_rebuild'] or target['removed'] or target['key_change'] or target['blocked']
           for target in schema['targets']):
        raise ValueError('Unexpected structural change; industry import stopped')
    receipt['schema_result'] = service.schema.apply(schema['token'])
    atomic_json(receipt_path, receipt)
    if receipt['schema_result']['status'] != 'completed':
        raise ValueError('Research policy columns were not deployed to both targets')

    pipeline = Pipeline(service)
    for table in ['industry_dim', 'security_industry_membership', 'industry_scheme_dim']:
        fields = [field['name'] for field in service.registry.spec(table).fields if field['name'] not in GENERATED]
        rows = projected[table]
        if not rows:
            raise ValueError('No rows projected for ' + table)
        directory = root / 'staged'
        directory.mkdir(exist_ok=True)
        source = directory / (table + '.csv')
        with source.open('w', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for row in sorted(rows, key=lambda value: tuple(str(value.get(k)) for k in service.registry.spec(table).keys)):
                writer.writerow({field: row.get(field) for field in fields})
        record = receipt['tasks'].get(table)
        if record is None:
            item = import_file(service, table, source, {field: field for field in fields},
                               mode='refresh_source', targets=['postgres', 'clickhouse'])
            record = {'task_id': item['id'], 'projected_rows': len(rows), 'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest()}
            receipt['tasks'][table] = record
            atomic_json(receipt_path, receipt)
            service.state.ledger_put('industry_history_source', item['id'], {
                'task_id': item['id'], 'source_manifest_sha256': receipt['source_manifest_sha256'],
                'source_manifest_path': str((source_root / 'extraction.json').resolve()),
                'lineage_path': str((root / 'lineage.json').resolve()),
                'lineage_sha256': hashlib.sha256((root / 'lineage.json').read_bytes()).hexdigest(),
                'projection': 'scripts/import_industry_history.py', 'research_only': True})
        elif record['source_sha256'] != hashlib.sha256(source.read_bytes()).hexdigest():
            raise ValueError('Staged source changed; create a linked fresh import task')
        status = service.state.job(record['task_id'], metadata_only=True)['status']
        if status not in {'completed', 'ready_to_commit'}:
            result = pipeline.run(record['task_id'])
            status = result['status']
        record['audited_status'] = status
        atomic_json(receipt_path, receipt)
        if status == 'ready_to_commit':
            result = pipeline.commit(record['task_id'])
            status = result['status']
        record.update(status=status, updated_at=now())
        atomic_json(receipt_path, receipt)
        print(table, status, len(rows), flush=True)
        if status != 'completed':
            raise ValueError('Audited industry task did not finish: ' + table)
    receipt.update(status='completed', updated_at=now())
    atomic_json(receipt_path, receipt)


if __name__ == '__main__':
    main()
