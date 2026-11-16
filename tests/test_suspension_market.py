from copy import deepcopy
import pytest
from betalens_db_manager.suspension_market import build_scopes,validate_scopes,FORMAT
from test_reference_storage import MemoryStore,revision


def source(events=None):
    def bundle(endpoint,rows,**params):
        return {'endpoint':endpoint,'parameters':params,'rows':rows,'complete':True,
                'downloaded_at':'2026-11-16T00:00:00+00:00','archive_sha':['verified']}
    result=[bundle('stock_basic',[{'ts_code':'000001.SZ','list_date':'19910403','list_status':'L'}] if s=='L' else [],list_status=s) for s in ('L','D','P','G','UN')]
    result += [bundle('trade_cal',[{'exchange':e,'cal_date':'20240102','is_open':1,'pretrade_date':'20231229'}],exchange=e) for e in ('SSE','SZSE')]
    result += [bundle('suspend_d',events or [],trade_date='20240102')]
    return result


def test_market_day_zero_packet_and_append_publication_revoke():
    scopes,pool=build_scopes({},source());validate_scopes(scopes,pool)
    assert len(scopes)==1 and scopes[0]['complete'] and not scopes[0]['rows']
    store=MemoryStore();rev={**revision('market','suspension'),'format':FORMAT,'market_evidence':pool}
    store.stage(rev,scopes);assert store.verify('market')['verified']
    assert not store.snapshot()['scopes']
    store.publish('market',{scopes[0]['scope_id']:None})
    assert len(store.snapshot()['scopes'])==1
    store.revoke('market','rollback');assert not store.snapshot()['scopes']


def test_intraday_source_is_preserved_without_dense_normal_rows():
    events=[{'ts_code':'000001.SZ','trade_date':'20240102','suspend_type':'S','suspend_timing':'09:30-10:00'},
            {'ts_code':'000001.SZ','trade_date':'20240102','suspend_type':'R','suspend_timing':None}]
    scopes,pool=build_scopes({},source(events));assert len(scopes[0]['rows'])==2
    store=MemoryStore();store.stage({**revision('market','suspension'),'format':FORMAT,'market_evidence':pool},scopes)
    assert store.verify('market')['verified']


@pytest.mark.parametrize('change',['duplicate','wrong_date','wrong_type','unknown_code'])
def test_invalid_daily_packets_are_blocked(change):
    row={'ts_code':'000001.SZ','trade_date':'20240102','suspend_type':'S','suspend_timing':None}
    events=[row]
    if change=='duplicate':events.append(deepcopy(row))
    if change=='wrong_date':row['trade_date']='20240103'
    if change=='wrong_type':row['suspend_type']='?'
    if change=='unknown_code':row['ts_code']='999999.SZ'
    with pytest.raises(ValueError):build_scopes({},source(events))


def test_missing_universe_partition_is_not_complete():
    bundles=source();bundles[0]['complete']=False
    scopes,pool=build_scopes({},bundles)
    assert not scopes[0]['complete']


def test_retired_compatibility_table_blocks_cached_writes_and_inverse(tmp_path):
    from types import SimpleNamespace
    from betalens_db_manager.pipeline_writer import _assert_not_retired
    from betalens_db_manager.commit_protocol import NeedsReconciliation
    import sqlite3
    path=tmp_path/'tasks.sqlite3'
    with sqlite3.connect(path) as c:
        c.execute('CREATE TABLE ledger(table_name TEXT,row_key TEXT,value TEXT)')
        c.execute("INSERT INTO ledger VALUES ('suspension_retirement','active','{}')")
    db=SimpleNamespace(settings=SimpleNamespace(state_dir=tmp_path))
    spec=SimpleNamespace(name='trade_status_daily')
    with pytest.raises(NeedsReconciliation,match='只读兼容表'):_assert_not_retired(db,spec)
    state=SimpleNamespace(ledger_get=lambda table,key:{'active':True})
    with pytest.raises(NeedsReconciliation):_assert_not_retired(db,spec,state)
    _assert_not_retired(db,SimpleNamespace(name='stock_daily'),state)


def test_retirement_keeps_unclassified_source_rows():
    from types import SimpleNamespace
    from betalens_db_manager.suspension_migration import retirement_sql
    statements=retirement_sql(SimpleNamespace(pg_schema='betalens',ch_database='betalens'))
    for target in ('postgres','clickhouse'):
        assert 'status_source IS NULL OR' in statements[target]['stage'][1]
