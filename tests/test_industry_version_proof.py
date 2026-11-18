from copy import deepcopy
import gzip
import hashlib

import pytest

from betalens_db_manager.industry_codes import membership_scheme, archived_dictionaries
from betalens_db_manager.util import dumps
from test_manager import manager
from test_reference_domains import request, bundle, membership
from betalens_db_manager.reference_domains import build_scopes


def catalog():
    r = membership()
    return {'SW2021': {r[f'l{i}_code']: {'industry_code':r[f'l{i}_code'],
        'industry_name':r[f'l{i}_name'], 'level':f'L{i}',
        'parent_code':r[f'l{i-1}_code'] if i > 1 else None} for i in (1,2,3)}}


def test_tiered_membership_uses_full_versioned_dictionary_never_in_date():
    assert membership_scheme('index_member_all', membership(in_date='19900101'), catalog()) == 'SW2021'
    for field in ['l1_code','l2_name','l3_code']:
        with pytest.raises(ValueError):
            membership_scheme('index_member_all', membership(**{field:'different'}), catalog())
    for dictionaries in [{}, {'SW2014':catalog()['SW2021']}]:
        with pytest.raises(ValueError):membership_scheme('index_member_all', membership(), dictionaries)


def test_reused_code_and_same_name_do_not_prove_legacy_scheme():
    row = membership()
    dictionary = [{'index_code':row['l1_code'], 'industry_name':row['l1_name'], 'src':'SW2014'}]
    bundles = [bundle('index_member_all',[row],ts_code='000001.SZ',is_new='Y'),
               bundle('index_member_all',[],ts_code='000001.SZ',is_new='N'),
               bundle('index_classify',dictionary,src='SW2014',level='L1')]
    scope = build_scopes(request(['industry'],schemes=['SW2014']), bundles)[0]
    assert scope['complete']
    result = scope['rows'][0]
    assert result['classification_version'] is None
    assert result['industry_code'] is None and result['industry_name'] is None
    assert result['membership_status'] == result['data_quality_status'] == 'unknown'
    assert row['l1_code'] in result['raw_record']


def test_membership_only_plan_freezes_all_dictionary_dependencies(tmp_path):
    s = manager(tmp_path)
    plan = s.compile({'tables':['security_industry_membership'], 'sources':['tushare'],
                      'codes':['000001.SZ'],'start_date':'2005-01-01','end_date':'2026-11-17'})
    roots = [r for r in plan['requests'] if r['endpoint']=='index_classify']
    assert {r['parameters']['level'] for r in roots} == {'L1','L2','L3'}
    assert all(r['parameters']['src']=='SW2021' and r['dependency_only'] for r in roots)
    assert 'tushare:index_classify' in plan['source_contracts']


def test_dictionary_version_comes_from_archived_request_and_sha(tmp_path):
    rows = [dict(index_code='801010.SI',industry_name='行业',src='SW',level='L1',parent_code='0',industry_code='100000')]
    fields = list(rows[0]); raw = dumps({'data':{'fields':fields,'items':[[r[k] for k in fields] for r in rows]}}).encode()
    path = tmp_path/'source.json.gz'
    with gzip.open(path,'wb') as f:f.write(raw)
    part={'id':'dictionary','status':'downloaded','archive':str(path),'sha':hashlib.sha256(raw).hexdigest(),
          'request':{'source_id':'tushare','endpoint':'index_classify','parameters':{'src':'SW2014'}}}
    assert set(archived_dictionaries({'slices':[part]})) == {'SW2014'}
    changed=deepcopy(part);changed['sha']='bad'
    with pytest.raises(ValueError):archived_dictionaries({'slices':[changed]})


def test_missing_dictionary_prevents_all_generic_membership_projections(tmp_path):
    s=manager(tmp_path)
    job=s.state.create_job({'request':{},'mapping_snapshots':{}},s.settings.fingerprint)
    row={**membership(),'_raw':membership(),'_row':1,'_slice':'members'}
    with pytest.raises(ValueError,match='SW2021'):
        s._map_part('index_member_all',[row],job['id'],'2026-11-17T00:00:00Z',[])


def test_quality_correction_preserves_raw_source_and_rejects_hash_tampering():
    from betalens_db_manager.industry_version_repair import correct_scope
    from betalens_db_manager.util import digest
    raw=membership()
    scope={'selector':{'scheme_code':'SW2014'},'revision_id':'old','content_hash':'verified',
           'evidence':{},'complete':True}
    row={'source_endpoint':'index_member_all','raw_record':dumps(raw),'source_record_hash':digest(raw),
         'downloaded_at':'2026-11-09','available_at':None,'industry_code':raw['l1_code']}
    fixed=correct_scope(scope,[row])['rows'][0]
    assert fixed['industry_code'] is None and fixed['classification_version'] is None
    assert fixed['raw_record']==row['raw_record'] and fixed['source_record_hash']==row['source_record_hash']
    assert fixed['available_at'] is None and fixed['downloaded_at']==row['downloaded_at']
    with pytest.raises(ValueError):correct_scope(scope,[{**row,'source_record_hash':'wrong'}])
    with pytest.raises(ValueError):correct_scope({**scope,'selector':{'scheme_code':'SW2021'}},[row])
