"""Append corrective SW2014 reference scopes; never rewrite published history."""
import argparse
import gzip
import json
from collections import defaultdict
from pathlib import Path

from betalens_db_manager import DatabaseManagerService, Settings
from betalens_db_manager.reference_service import ReferenceWorkflow
from betalens_db_manager.industry_version_repair import repair
from betalens_db_manager.util import atomic_json, dumps, now


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--env',required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--execute',action='store_true')
    a=p.parse_args()
    with gzip.open(a.output/'labels-before/postgres-stock_industry_membership_versions.json.gz','rt') as f:
        before=json.load(f)
    affected=defaultdict(set)
    for row in before:
        if row['scheme_code']=='SW2014' and row['source_endpoint']=='index_member_all':
            affected[row['import_task_id']].add(row['security_code'])
    s=DatabaseManagerService(Settings.from_env(a.env));workflow=ReferenceWorkflow(s)
    path=a.output/'version-label-repair.json'
    receipt=json.loads(path.read_text()) if path.exists() else {'created_at':now(),'tasks':{}}
    receipt.update(affected_source_tasks=len(affected),
                   correction='Unversioned tiered records cannot prove SW2014. Publish explicit unknown labels; raw source and prior revisions remain immutable.')
    atomic_json(path,receipt)
    if not a.execute:
        print(dumps({'affected_tasks':len(affected),'affected_securities':len(set.union(*affected.values()))}),flush=True)
        return
    for original,codes in sorted(affected.items()):
        record=receipt['tasks'].get(original)
        if record and record.get('status')=='completed':continue
        if record and s.state.job(record['task_id'],metadata_only=True)['plan'].get('revision')=='reference-v1':
            # The original transport archives have been removed. Do not
            # manufacture a successful reacquisition or restore those files.
            s.state.update_job(record['task_id'],status='cancelled',error='Superseded: transport archive unavailable; use verified-publication quality correction')
            receipt.setdefault('superseded_baseline_tasks',[]).append(record['task_id'])
            record=None
        if record is None:
            record={**repair(s,original),'source_task_id':original,'securities':len(codes)}
            receipt['tasks'][original]=record;atomic_json(path,receipt)
        result=repair(s,original,record['task_id'])
        record.update(**result,updated_at=now());atomic_json(path,receipt)
        print('corrected',len([r for r in receipt['tasks'].values() if r['status']=='completed']),
              '/',len(affected),record['task_id'],record['status'],flush=True)
        if record['status']!='completed':raise ValueError('Corrective reference publication incomplete')
    receipt.update(status='completed',updated_at=now());atomic_json(path,receipt)


if __name__=='__main__':main()
