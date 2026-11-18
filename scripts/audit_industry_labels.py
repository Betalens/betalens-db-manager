"""Read-only, full-table version audit before an industry repair."""
import argparse
import gzip
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from betalens_db_manager import DatabaseManagerService, Settings
from betalens_db_manager.industry_codes import normalize_parents
from betalens_db_manager.util import atomic_json, dumps, now, digest
from betalens_db_manager.comparison import typed, PROVENANCE
from betalens_db_manager.reference_storage import ReferenceStore, _normal
from betalens_db_manager.reference_contracts import TABLES


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--env', required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--phase', default='before')
    a = p.parse_args()
    s = DatabaseManagerService(Settings.from_env(a.env))
    root = a.output / ('labels-' + a.phase)
    root.mkdir(parents=True, exist_ok=True)
    dictionaries = json.loads((a.output / 'extraction/dictionaries.json').read_text())
    dims = []
    for scheme, rows in dictionaries.items():
        for r in rows:
            dims.append(dict(scheme_code=scheme, industry_code=r['index_code'],
                             industry_name=r['industry_name'], level=r['level'],
                             classification_code=r['industry_code'], parent_code=r['parent_code'],
                             source_id='tushare', source_endpoint='index_classify'))
    normalize_parents(dims)
    lookup = {(r['scheme_code'], r['industry_code']): r for r in dims}
    report = {'queried_at': now(), 'targets': {}, 'findings': []}
    task_ids = set()
    logical = {}
    for target in ['postgres', 'clickhouse']:
        results = {}
        for table in ['industry_scheme_dim', 'industry_dim', 'industry_membership_record',
                      'security_industry_membership', 'stock_industry_membership_versions']:
            if target == 'postgres':
                with s.pg.connect(readonly=True) as c, c.cursor() as q:
                    q.execute(f'SELECT * FROM "{s.settings.pg_schema}"."{table}"')
                    names = [col.name for col in q.description]
                    rows = [dict(zip(names, row)) for row in q.fetchall()]
            else:
                rows = s.ch._json_rows(s.ch.execute(f'SELECT * FROM {s.ch.table(table)} FINAL FORMAT JSONEachRow'))
            with gzip.open(root / (target + '-' + table + '.json.gz'), 'wt') as f:
                f.write(dumps(rows))
            issues = []
            for row in rows:
                task_ids.add(row['import_task_id'])
                scheme = row['scheme_code']
                if not scheme.startswith('SW'):
                    continue
                if table == 'industry_membership_record':
                    for level in [1,2,3]:
                        expected=lookup.get((scheme,row.get(f'l{level}_code')))
                        if expected is None or expected['industry_name']!=row.get(f'l{level}_name'):
                            issues.append({'reason':'raw_membership_dictionary_mismatch','level':level,'row':row})
                if table in ['industry_dim', 'security_industry_membership', 'stock_industry_membership_versions']:
                    dim = lookup.get((scheme, row.get('industry_code')))
                    if dim is None:
                        issues.append({'reason': 'code_missing_from_version_dictionary', 'row': row})
                    elif table == 'industry_dim' and any(row.get(k) != dim[k] for k in ['industry_name', 'level', 'parent_code']):
                        issues.append({'reason': 'dictionary_value_mismatch', 'expected': dim, 'row': row})
                    elif table == 'stock_industry_membership_versions' and row.get('industry_name') != dim['industry_name']:
                        issues.append({'reason': 'version_dictionary_name_mismatch', 'expected': dim, 'row': row})
                if table == 'stock_industry_membership_versions' and scheme == 'SW2014' and row['source_endpoint'] == 'index_member_all' and row['membership_status'] == 'mapped':
                    issues.append({'reason': 'legacy_version_inferred_from_unversioned_endpoint', 'row': row})
            atomic_json(root / (target + '-' + table + '-findings.json'), issues)
            results[table] = {'rows': len(rows), 'schemes': dict(Counter(r['scheme_code'] for r in rows)),
                              'findings': dict(Counter(r['reason'] for r in issues))}
            if table != 'stock_industry_membership_versions':
                spec=s.registry.spec(table)
                fields=[f['name'] for f in spec.fields if f['name'] not in PROVENANCE or f['name'] in {'available_at','availability_basis','data_quality_status'}]
                logical.setdefault(table,{})[target]=digest(sorted(dumps({k:r.get(k) for k in fields})
                    for r in (typed(spec,row) for row in rows)))
            elif a.phase == 'after':
                # Audit-only old revisions remain physical. Verify the visible
                # scope heads separately, without touching unrelated modules.
                scope_ids={r['scope_id'] for r in rows}
                current=ReferenceStore(s,target).snapshot(scope_ids=scope_ids)
                with gzip.open(root/(target+'-trusted-industry.json.gz'),'wt') as f:f.write(dumps(current))
                with gzip.open(a.output/('labels-before/'+target+'-stock_industry_membership_versions.json.gz'),'rt') as f:before=json.load(f)
                spec=TABLES[table]
                fields=['security_code','scheme_code','classification_version','level','industry_code','industry_name',
                        'valid_from','valid_to','boundary_basis','membership_status','raw_record','source_record_hash',
                        'available_at','availability_basis','downloaded_at','data_quality_status']
                def normalize_stored(row):
                    row={k:v for k,v in row.items() if k!='module'}
                    # These are database snapshots, whose timestamp columns
                    # explicitly use UTC. CH JSONEachRow omits that suffix.
                    if target == 'clickhouse':
                        for field in spec.fields:
                            value=row.get(field['name'])
                            if field['type']=='timestamp' and value is not None:
                                parsed=datetime.fromisoformat(str(value).replace('Z','+00:00'))
                                if parsed.tzinfo is None:row[field['name']]=parsed.replace(tzinfo=timezone.utc)
                    return _normal(spec,row)
                def business(values,schemes,*,cross_target=False):
                    selected=[k for k in fields if not (cross_target and k=='downloaded_at')]
                    return sorted(dumps({k:r.get(k) for k in selected}) for r in (normalize_stored(x) for x in values)
                                  if r['scheme_code'] in schemes)
                results['trusted_industry']={'rows':len(current['rows']),'scopes':len(current['scopes']),
                    'unsupported_legacy_mapped':sum(r['scheme_code']=='SW2014' and r['source_endpoint']=='index_member_all' and r['membership_status']=='mapped' for r in current['rows']),
                    'unsupported_legacy_named':sum(r['scheme_code']=='SW2014' and r['source_endpoint']=='index_member_all' and (r['classification_version'] or r['industry_code'] or r['industry_name']) is not None for r in current['rows']),
                    'other_schemes_unchanged':business(before,{'CITICS','SW2021'})==business(current['rows'],{'CITICS','SW2021'}),
                    'all_visible_revisions_verified':all(v['verified'] for v in current.get('verification',[]))}
                logical.setdefault('trusted_industry',{})[target]=digest(business(current['rows'],{'SW2014','SW2021','CITICS'},cross_target=True))
        report['targets'][target] = results
    # Only summarize request metadata; credentials and entire plans stay private.
    tasks = []
    for task in sorted(task_ids):
        try:
            item = s.state.job(task, metadata_only=True)
        except ValueError:
            continue
        request = item['plan'].get('request', {})
        tasks.append({'id': task, 'status': item['status'], 'revision': item['plan'].get('revision'),
                      'request': {k: request.get(k) for k in ['modules', 'schemes', 'levels', 'security_codes', 'parameters']}})
    atomic_json(root / 'tasks.json', tasks)
    report['cross_target_business_equal']={table:len(set(values.values()))==1 for table,values in logical.items()}
    atomic_json(root / 'audit.json', report)
    print(dumps(report), flush=True)


if __name__ == '__main__':
    main()
