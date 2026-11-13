from datetime import date
from decimal import Decimal
from pathlib import Path
import gzip
import hashlib

import pytest
from fastapi.testclient import TestClient

from test_manager import manager,table_definition
from test_download_cache_cleanup import task,payload
from betalens_db_manager.api import create_app
from betalens_db_manager.cache_retention import cleanup_task_caches,cleanup_derived_caches,POLICY_VERSION,LOADED_SHA256
from betalens_db_manager.cache_retirement import retire_rolled_back_tasks
from betalens_db_manager.canonical import registry_context
from betalens_db_manager.comparison import compare
from betalens_db_manager.pipeline import Pipeline
from betalens_db_manager.pipeline_cache import (save_journal,load_journal,read_journal_metadata,
    compress_journal_manifest,write_rows)
from betalens_db_manager.pipeline_store import PipelineStore
from betalens_db_manager.worker import heartbeat
from betalens_db_manager.worker_supervisor import worker_status


def journal(service,job,child=None,legacy=False):
    child=child or job
    service.registry.save(table_definition('cache_prices'))
    with registry_context(service.registry):
        spec=service.registry.spec('cache_prices')
        rows=[{'code':'a','business_date':date(2026,10,20),'price':Decimal('12.00000001')}]
        root=service.settings.state_dir/'pipeline'/job/'journals'/'postgres'/'batch'
        record=save_journal(root,spec,compare(spec,rows,[],'fill_missing'),rows)
        normalized=write_rows(service.settings.state_dir/'pipeline'/child/'cache_prices'/'0.parquet',spec,rows)
    if legacy:
        zipped=Path(record['path']);original=gzip.decompress(zipped.read_bytes())
        path=zipped.with_suffix('');path.write_bytes(original);zipped.unlink()
        record={**record,'path':str(path),'sha256':hashlib.sha256(original).hexdigest()}
    store=PipelineStore(service.state)
    batch=child+':cache_prices:0'
    store.prepare(job,batch,'postgres',record);store.acknowledge(job,batch,'postgres',{})
    store.put(job,'cohort','normalize',{})
    store.finish(job,'cohort',{'child_id':child,'tables':{'cache_prices':[normalized]},'rejected':0})
    return spec,record,normalized,batch


def test_new_compressed_journal_and_legacy_migration_preserve_audit(tmp_path):
    s=manager(tmp_path);job=task(s)
    spec,record,normalized,batch=journal(s,job,legacy=True)
    before=load_journal(record,spec)
    original=dict(record)
    result=compress_journal_manifest(record)
    assert result['bytes']>0 and record==original
    assert not Path(record['path']).exists()
    assert Path(record['path']+'.gz').exists()
    assert load_journal(record,spec)==before
    assert compress_journal_manifest(record) is None
    assert Pipeline(s).audit(job)['total']==1
    Path(record['path']+'.gz').write_bytes(gzip.compress(b'{"changed":true}'))
    with pytest.raises(ValueError,match='哈希'):load_journal(record,spec)


def test_compression_failure_preserves_original_journal(tmp_path,monkeypatch):
    s=manager(tmp_path);job=task(s)
    spec,record,_,_=journal(s,job,legacy=True)
    original=Path(record['path']).read_bytes()
    def fail(*args):raise OSError('disk full')
    monkeypatch.setattr('betalens_db_manager.pipeline_cache._atomic_gzip',fail)
    with pytest.raises(OSError):compress_journal_manifest(record)
    assert Path(record['path']).read_bytes()==original
    assert load_journal(record,spec)['changes']


def test_completed_normalized_packets_retire_but_rollback_images_remain(tmp_path):
    s=manager(tmp_path);job=task(s)
    spec,record,normalized,_=journal(s,job)
    metadata=read_journal_metadata(record)
    preview=cleanup_derived_caches(s,dry_run=True)
    assert preview['files']==1 and Path(normalized['path']).exists()
    result=cleanup_derived_caches(s)
    assert result['files']==1 and not Path(normalized['path']).exists()
    assert all(Path(metadata[key]['path']).exists() for key in ('before','after','expected'))
    assert load_journal(record,spec)['changes']
    assert s.state.job(job)['derived_cache_cleanup']['status']=='cleaned'
    assert cleanup_derived_caches(s)['files']==0


@pytest.mark.parametrize('status',['running','paused','partial','ready_to_commit','interrupted'])
def test_recovery_packets_remain_while_task_is_unfinished(tmp_path,status):
    s=manager(tmp_path);job=task(s,status)
    _,record,normalized,_=journal(s,job,legacy=True)
    result=cleanup_derived_caches(s)
    assert result['files']==0 and result['manifests']==0
    assert Path(normalized['path']).exists() and Path(record['path']).exists()


def test_no_expected_image_or_incomplete_target_receipt_retains_normalized_data(tmp_path):
    s=manager(tmp_path);job=task(s)
    _,record,normalized,batch=journal(s,job)
    store=PipelineStore(s.state)
    store.acknowledge(job,batch,'postgres',{},error='unconfirmed write')
    assert cleanup_derived_caches(s)['files']==0
    assert Path(normalized['path']).exists()
    store.acknowledge(job,batch,'postgres',{})
    Path(read_journal_metadata(record)['expected']['path']).unlink()
    assert cleanup_derived_caches(s)['files']==0 and Path(normalized['path']).exists()


def test_full_rollback_retirement_is_explicit_and_covers_entire_family(tmp_path):
    s=manager(tmp_path);job=task(s,'rolled_back');child=task(s,'downloaded',job)
    raw=payload(s,job)
    _,record,normalized,batch=journal(s,job,child)
    store=PipelineStore(s.state)
    store.prepare(job,'rollback:'+batch,'postgres',record)
    store.acknowledge(job,'rollback:'+batch,'postgres',{})
    audit=s.settings.state_dir/'reference-cache'/job/'audit.json'
    audit.parent.mkdir(parents=True);audit.write_text('{}')
    assert retire_rolled_back_tasks(s)['files']==0
    preview=retire_rolled_back_tasks(s,include_new=True,dry_run=True)
    assert preview['files']>3 and raw.exists()
    result=retire_rolled_back_tasks(s,include_new=True)
    assert result['bytes']==preview['bytes']
    assert not raw.exists() and not Path(record['path']).exists() and not Path(normalized['path']).exists()
    assert not audit.exists() and s.state.job(child)['cache_retirement']['status']=='retired'
    assert s.state.job(job)['status']=='rolled_back'
    with pytest.raises(ValueError,match='退役'):s.state.enqueue(job,'pipeline_validate')
    assert retire_rolled_back_tasks(s)['files']==0


def test_status_label_cannot_retire_a_write_without_its_rollback_receipt(tmp_path):
    s=manager(tmp_path);job=task(s,'rolled_back')
    _,record,normalized,_=journal(s,job)
    assert retire_rolled_back_tasks(s,include_new=True)['files']==0
    assert Path(record['path']).exists() and Path(normalized['path']).exists()
    assert s.state.job(job)['cache_retirement'] is None


def test_retirement_preserves_shared_inputs_and_shared_journals(tmp_path):
    s=manager(tmp_path);job=task(s,'rolled_back');consumer=task(s,'running')
    raw=payload(s,job)
    _,record,_,batch=journal(s,job)
    store=PipelineStore(s.state)
    store.prepare(job,'rollback:'+batch,'postgres',record);store.acknowledge(job,'rollback:'+batch,'postgres',{})
    s.state.slice(consumer,'shared','daily',{},archive=str(raw),status='downloaded')
    store.prepare(consumer,'borrowed','postgres',record)
    result=retire_rolled_back_tasks(s,include_new=True)
    assert result['retained_files']>=4 and raw.exists() and Path(record['path']).exists()
    assert s.state.job(job)['cache_retirement']['status']=='retired_shared'
    assert s.state.job(consumer)['status']=='running'


def test_retired_api_exposes_tombstone_and_rejects_deleted_artifact_operations(tmp_path):
    s=manager(tmp_path);job=s.plan({'tables':['market_daily_raw'],'start_date':'2026-10-20','end_date':'2026-10-20'})['id']
    payload(s,job);s.state.update_job(job,status='rolled_back')
    retire_rolled_back_tasks(s,include_new=True)
    with TestClient(create_app(s)) as client:
        assert client.get('/api/tasks/'+job).status_code==200
        assert client.get('/api/tasks/'+job+'/cache').json()['cache_purged']
        for endpoint in ('audit','archive/sample','reviews'):
            response=client.get('/api/tasks/'+job+'/'+endpoint)
            assert response.status_code==410,(endpoint,response.text)
        response=client.post('/api/tasks/'+job+'/validations',json={'rules':['readback']})
        assert response.status_code==410,response.text


def test_worker_reports_policy_actually_loaded_in_process(tmp_path):
    s=manager(tmp_path)
    heartbeat(s,'idle')
    status=worker_status(s.settings)
    assert status['available']
    assert status['heartbeat']['cache_policy_version']==POLICY_VERSION
    assert status['heartbeat']['cache_policy_sha256']==LOADED_SHA256
