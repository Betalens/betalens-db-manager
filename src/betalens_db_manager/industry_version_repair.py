"""Conservative quality corrections over immutable, verified publications."""
from copy import deepcopy
import json
import uuid

from .reference_contracts import VERSION
from .reference_storage import ReferenceStore
from .util import digest, now

POLICY = 'sw2014-unversioned-endpoint-quality-correction-v1'


def correct_scope(scope, rows):
    """Withdraw an unsupported label without claiming a new source download."""
    if scope['selector'].get('scheme_code') != 'SW2014':
        raise ValueError('纠正只允许作用于 SW2014 范围')
    result = deepcopy(scope)
    for key in ['revision_id','row_count','key_hash','content_hash','selector_hash']:
        result.pop(key,None)
    result['rows'] = []
    for original in rows:
        if original['source_endpoint'] != 'index_member_all':
            raise ValueError('范围包含其他来源，禁止覆盖有独立证据的旧版归属')
        if digest(json.loads(original['raw_record'])) != original['source_record_hash']:
            raise ValueError('已发布记录的原文与源哈希不一致')
        row = dict(original)
        for key in ['revision_id','record_id','scope_id','import_task_id','attempt_id','ingestion_seq','ingested_at','module']:
            row.pop(key,None)
        row.update(classification_version=None,industry_code=None,industry_name=None,
                   membership_status='unknown',data_quality_status='unknown',mapping_version=VERSION,
                   schema_version=VERSION)
        result['rows'].append(row)
    result['evidence'].update(classification_version_basis='unproven_legacy_version',
                              quality_correction={'policy':POLICY,'source_revision_id':scope['revision_id'],
                                                  'source_content_hash':scope['content_hash']})
    return result


def repair(service, original_task, task_id=None):
    """Append, verify and publish only the affected SW2014 scope heads."""
    if task_id is None:
        item=service.state.create_job({'revision':'industry-quality-correction-v1','policy':POLICY,
            'source_task_id':original_task,'request':{'targets':['postgres','clickhouse'],'modules':['industry']}},
            service.settings.fingerprint)
        return {'task_id':item['id'],'status':'created'}
    item=service.state.job(task_id,metadata_only=True)
    if item['plan'].get('policy')!=POLICY or item['plan'].get('source_task_id')!=original_task:
        raise ValueError('纠正任务身份不一致')
    for target in ['postgres','clickhouse']:
        key=task_id+':'+target
        saved=service.state.ledger_get('industry_quality_correction',key)
        if saved and saved.get('status')=='completed':continue
        store=ReferenceStore(service,target)
        if saved is None:
            source=store.task_revisions(original_task)
            scope_ids={v for r in source if r['module']=='industry' for v in r['scope_ids']}
            current=store.snapshot(scope_ids=scope_ids)
            scopes=[s for s in current['scopes'] if s['selector'].get('scheme_code')=='SW2014']
            if not scopes:raise ValueError('未找到待纠正的可信 SW2014 范围')
            by_scope={s['scope_id']:[] for s in scopes}
            for row in current['rows']:
                if row['scope_id'] in by_scope:by_scope[row['scope_id']].append(row)
            corrected=[correct_scope(s,by_scope[s['scope_id']]) for s in scopes]
            revision={'revision_id':uuid.uuid4().hex,'import_task_id':task_id,'task_id':task_id,
                      'attempt_id':uuid.uuid4().hex,'target':target,'module':'industry','mapping_version':VERSION,
                      'source_id':'tushare','source_name':'tushare',
                      'downloaded_at':max(row['downloaded_at'] for rows in by_scope.values() for row in rows),
                      'quality_correction':{'policy':POLICY,'source_task_id':original_task}}
            saved={'task_id':task_id,'target':target,'status':'audited','revision':revision,
                   'scopes':corrected,'expected_heads':{s['scope_id']:current['heads'][s['scope_id']] for s in scopes},
                   'source_rows':sum(map(len,by_scope.values()))}
            service.state.ledger_put('industry_quality_correction',key,saved)
        store.stage(saved['revision'],saved['scopes'])
        verified=store.verify(saved['revision']['revision_id'])
        if not verified['verified']:raise ValueError('纠正版本读回核验失败')
        publication=store.publish(saved['revision']['revision_id'],saved['expected_heads'])
        saved.update(status='completed',verification=verified,publication=publication,updated_at=now())
        service.state.ledger_put('industry_quality_correction',key,saved)
        service.state.invalidate_validation_reports(['stock_industry_membership_versions'],[target])
    service.state.update_job(task_id,status='completed')
    service.state.log(task_id,'industry_quality_corrected',source_task_id=original_task,policy=POLICY)
    return {'task_id':task_id,'status':'completed'}
