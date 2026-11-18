from betalens_db_manager.industry_history import project, scheme_windows


def test_old_index_history_is_versioned_by_dictionary_and_research_window():
    dictionaries = {'SW2014': [dict(index_code='OLD.SI', industry_name='旧行业', parent_code='0',
                                    level='L1', industry_code='100000', src='SW2014')]}
    inputs = [{'scheme': 'SW2014', 'endpoint': 'index_member', 'archive': 'raw.json.gz',
               'archive_sha256': 'source-proof', 'row': dict(index_code='OLD.SI', con_code='000001.SZ',
                in_date='19910403', out_date=None, is_new='Y')},
              {'scheme': 'SW2014', 'endpoint': 'index_member', 'archive': 'later.json.gz',
               'archive_sha256': 'later-proof', 'row': dict(index_code='OLD.SI', con_code='000002.SZ',
                in_date='20220801', out_date=None, is_new='Y')}]
    result = project(dictionaries, inputs)
    assert result['membership_counts'] == {'SW2014': 1}
    row = result['security_industry_membership'][0]
    assert row['valid_from'] == '1991-04-03' and row['valid_to'] is None
    assert row['availability_basis'].endswith('historical_disclosure_unproven')
    assert next(iter(result['lineage'].values()))[0]['endpoint'] == 'index_member'
    assert not result['rejected']


def test_version_mismatch_and_missing_hierarchy_are_rejected():
    item = {'scheme': 'SW2014', 'endpoint': 'index_member_all', 'archive': 'wrong-version',
            'archive_sha256': 'wrong', 'row': dict(ts_code='000001.SZ', in_date='20000101', out_date=None, is_new='Y')}
    result = project({}, [item])
    assert not result['security_industry_membership']
    assert 'cannot establish' in result['rejected'][0]['reason']


def test_research_windows_do_not_rewrite_publication_or_event_dates():
    rows = [dict(scheme_code=scheme, event_date='2014-01-01', available_at=None) for scheme in ['SW2014', 'SW2021', 'CITICS']]
    result = scheme_windows(rows)
    assert all(row['event_date'] == '2014-01-01' and row['available_at'] is None for row in result)
    assert result[0]['research_valid_to'] == result[1]['research_valid_from'] == '2021-12-13'


def test_empty_source_interval_is_quarantined_and_reversed_is_blocking():
    dictionaries={'SW2014':[dict(index_code='OLD.SI',industry_name='旧',parent_code='0',level='L1',industry_code='1',src='SW2014')]}
    item={'scheme':'SW2014','endpoint':'index_member','archive':'raw','archive_sha256':'sha',
          'row':dict(index_code='OLD.SI',con_code='000001.SZ',in_date='20170406',out_date='20170406',is_new='N')}
    p=project(dictionaries,[item]);assert len(p['quarantined'])==1 and not p['rejected']
    item['row']['out_date']='20170405'
    p=project(dictionaries,[item]);assert len(p['rejected'])==1 and not p['quarantined']
