from betalens_db_manager.canonical import canonical
from betalens_db_manager.comparison import compare


def member(**changes):
    row=dict(scheme_code='SW2021',security_code='000001.SZ',industry_code='801780.SI',
             valid_from='1991-04-03',valid_to=None,membership_status='current',event_date='1991-04-03',
             source_name='tushare',data_quality_status='accepted')
    row.update(changes);return row


def test_full_refresh_affirms_equal_keys_and_values_without_mixing_sources():
    result=compare(canonical('security_industry_membership'),[member(source_name='file',valid_to='2026-11-17',membership_status='historical')],[member()],'refresh_source')
    after=result['changes'][0]['after']
    assert after['source_name']=='file' and after['data_quality_status']=='accepted'


def test_partial_refresh_retaining_foreign_business_values_still_mixes_sources():
    result=compare(canonical('security_industry_membership'),[member(source_name='file',event_date=None,valid_to='2026-11-17')],[member()],'refresh_source')
    assert result['changes'][0]['after']['data_quality_status']=='mixed_sources'


def test_full_reassertion_can_correct_spurious_mixed_quality_without_changing_values():
    result=compare(canonical('security_industry_membership'),[member(source_name='file')],[member(source_name='file,tushare',data_quality_status='mixed_sources')],'refresh_source')
    assert result['counts']['updated']==1
    assert result['changes'][0]['after']['data_quality_status']=='accepted'
