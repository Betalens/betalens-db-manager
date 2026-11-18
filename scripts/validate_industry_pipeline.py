"""Real, noncommitting check of the generic industry's source conversion."""
import argparse
import json
from pathlib import Path

from betalens_db_manager import DatabaseManagerService, Settings
from betalens_db_manager.pipeline import Pipeline
from betalens_db_manager.pipeline_cache import read_rows
from betalens_db_manager.util import atomic_json


def main():
    p=argparse.ArgumentParser();p.add_argument('--env',required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();s=DatabaseManagerService(Settings.from_env(a.env))
    path=a.output/'generic-pipeline-validation.json'
    if path.exists():
        saved=json.loads(path.read_text());task=saved['task_id']
    else:
        item=s.plan({'tables':['industry_dim','industry_membership_record','security_industry_membership'],
                     'source_id':'tushare','endpoints':['index_member_all','index_classify'],
                     'codes':['000001.SZ'],'start_date':'2005-01-01','end_date':'2026-11-17',
                     'targets':['postgres','clickhouse'],'auto_commit':False,'download_workers':2,
                     'normalization_workers':1,'requests_per_minute':200,'validation_profile':'strict'})
        task=item['id'];atomic_json(path,{'task_id':task,'status':'planned'})
    result=Pipeline(s).run(task)
    rows={}
    for unit in result['pipeline']['units']:
        for table,records in (unit.get('result') or {}).get('tables',{}).items():
            for record in records:
                rows.setdefault(table,[]).extend(read_rows(record,s.registry.spec(table)))
    checks={'ready_without_commit':result['status']=='ready_to_commit',
            'all_three_outputs':all(rows.get(t) for t in ['industry_dim','industry_membership_record','security_industry_membership']),
            'no_legacy_relabel':all(r['scheme_code']=='SW2021' for values in rows.values() for r in values)}
    report={'task_id':task,'status':result['status'],'checks':checks,
            'rows':{k:len(v) for k,v in rows.items()},'committed':False}
    atomic_json(path,report);print(json.dumps(report),flush=True)
    if not all(checks.values()):raise ValueError('Real generic industry pipeline validation failed')


if __name__=='__main__':main()
