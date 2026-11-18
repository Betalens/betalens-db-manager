"""Reassert complete archived rows after fixing false mixed-source detection."""
import argparse
import csv
import json
from pathlib import Path

from betalens_db_manager import DatabaseManagerService, Settings
from betalens_db_manager.comparison import compare, row_key
from betalens_db_manager.files import import_file
from betalens_db_manager.industry_history import project, scheme_windows
from betalens_db_manager.pipeline import Pipeline
from betalens_db_manager.registry import GENERATED
from betalens_db_manager.util import atomic_json, digest
from import_industry_history import source_inputs


def main():
    p=argparse.ArgumentParser();p.add_argument('--env',required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();s=DatabaseManagerService(Settings.from_env(a.env));root=a.output
    manifest=json.loads((root/'extraction/extraction.json').read_text())
    projected=project(json.loads((root/'extraction/dictionaries.json').read_text()),source_inputs(manifest))
    original=json.loads((root/'import.json').read_text())
    projected['industry_scheme_dim']=scheme_windows(original['scheme_metadata_before'])
    path=root/'quality-repair.json'
    receipt=json.loads(path.read_text()) if path.exists() else {'tasks':{}}
    pipeline=Pipeline(s)
    for table in ['industry_dim','security_industry_membership','industry_scheme_dim']:
        if receipt['tasks'].get(table,{}).get('status')=='completed':continue
        spec=s.registry.spec(table);fields=[f['name'] for f in spec.fields if f['name'] not in GENERATED]
        with s.pg.connect(readonly=True) as c,c.cursor() as q:
            q.execute(f'SELECT * FROM "{s.settings.pg_schema}"."{table}" WHERE data_quality_status=\'mixed_sources\'')
            names=[col.name for col in q.description];old=[dict(zip(names,v)) for v in q.fetchall()]
        keys={row_key(spec,row) for row in old}
        rows=[row for row in projected[table] if row_key(spec,row) in keys]
        if not rows:
            receipt['tasks'][table]={'status':'completed','rows':0};atomic_json(path,receipt);continue
        # Prove this is a complete reassertion, not acceptance of genuinely
        # retained values from a different source.
        incoming=[dict(row,source_name='file',data_quality_status='accepted') for row in rows]
        diff=compare(spec,incoming,old,'refresh_source')
        if len(diff['changes'])!=len(rows) or any(c['after']['data_quality_status']!='accepted' for c in diff['changes']):
            raise ValueError('Source projection does not fully affirm every proposed quality correction')
        source=root/'staged'/(table+'-quality-repair.csv')
        with source.open('w',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader()
            writer.writerows({k:row.get(k) for k in fields} for row in rows)
        record=receipt['tasks'].get(table)
        if record is None:
            item=import_file(s,table,source,{k:k for k in fields},mode='refresh_source',targets=['postgres','clickhouse'])
            record={'task_id':item['id'],'rows':len(rows),'parent_import_task':original['tasks'][table]['task_id']}
            receipt['tasks'][table]=record;atomic_json(path,receipt)
            s.state.ledger_put('industry_history_source',item['id'],{'task_id':item['id'],
                'parent_task_id':record['parent_import_task'],'source_manifest_sha256':original['source_manifest_sha256'],
                'lineage_path':str((root/'lineage.json').resolve()),'quality_correction':'full_source_reassertion'})
        status=s.state.job_header(record['task_id'])['status']
        if status!='ready_to_commit':status=pipeline.run(record['task_id'])['status']
        if status=='ready_to_commit':status=pipeline.commit(record['task_id'])['status']
        record['status']=status;atomic_json(path,receipt);print(table,status,len(rows),flush=True)
        if status!='completed':raise ValueError('Industry source reassertion did not complete')
    receipt['status']='completed';atomic_json(path,receipt)


if __name__=='__main__':main()
