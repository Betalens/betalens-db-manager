from datetime import date
from types import SimpleNamespace

from test_retrieval import task
from betalens_db_manager.index_snapshot import annotate_validated_snapshots, retain_evidence, resolve_evidence
from betalens_db_manager.response_validation import validate_archives, manual_coverage_review


def source(endpoint, params):
    if params.get('offset', 0):
        return []
    return [{'index_code': '000906.SH', 'con_code': code, 'trade_date': '20240131', 'weight': '50'}
            for code in ['000001.SZ', '600000.SH']]


def prepared(tmp_path):
    service, adapter, item = task(tmp_path, 'index_weight',
                                {'index_code': '000906.SH', 'start_date': '20240131', 'end_date': '20240131'},
                                source, cap=5000, mode='offset_probe')
    from betalens_db_manager.downloader import TaskDownloader
    with TaskDownloader(service, item) as downloader:
        downloader.run()
    item = service.state.job(item['id'])
    item['plan']['mapping_snapshots'] = {'index_membership': {'bindings': [
        {'source_id': 'tushare', 'endpoint': 'index_weight', 'mapping_mode': 'builtin'}]}}
    return service, item


def rows():
    return [{'index_code': '000906.SH', 'security_code': code, 'trade_date': date(2024, 1, 31)}
            for code in ['000001.SZ', '600000.SH']]


def test_complete_chain_including_empty_terminal_proves_snapshot(tmp_path):
    service, item = prepared(tmp_path)
    assert not validate_archives(service, item)
    actual = rows()
    annotate_validated_snapshots(item, actual)
    assert [(r['snapshot_complete'], r['snapshot_member_count']) for r in actual] == [(1, 2), (1, 2)]
    item['slices'] = [p for p in item['slices'] if p['rows']]
    assert validate_archives(service, item)  # A short nonempty page is insufficient.


def test_dropped_constituent_cannot_acquire_proof(tmp_path):
    _, item = prepared(tmp_path)
    actual = rows()[:1]
    annotate_validated_snapshots(item, actual)
    assert 'snapshot_complete' not in actual[0]


def test_manual_coverage_exception_cannot_upgrade_completeness(tmp_path):
    _, item = prepared(tmp_path)
    actual = rows()
    with manual_coverage_review(item['id']):
        annotate_validated_snapshots(item, actual)
    assert all('snapshot_complete' not in r for r in actual)


def test_filtered_constituents_and_custom_mapping_remain_unknown(tmp_path):
    _, item = prepared(tmp_path)
    item['slices'][0]['request']['parameters']['con_code'] = '000001.SZ'
    actual = rows()
    annotate_validated_snapshots(item, actual)
    assert all('snapshot_complete' not in r for r in actual)


def test_snapshot_can_be_replayed_after_disposable_downloads_are_removed(tmp_path):
    service, item = prepared(tmp_path)
    actual = rows()
    annotate_validated_snapshots(item, actual)
    retain_evidence(service, item, actual)
    for part in item['slices']:
        from pathlib import Path
        path = Path(part['archive'])
        path.unlink()
        path.with_suffix('.meta.json').unlink()
    restored = resolve_evidence(service, item)
    assert not validate_archives(service, restored)
    replay = rows()
    annotate_validated_snapshots(restored, replay)
    assert all(r['snapshot_complete'] == 1 for r in replay)


def test_snapshot_proof_columns_have_registered_computed_lineage(tmp_path):
    from test_manager import manager
    from betalens_db_manager.field_lineage import producers_for, validate_producers
    service = manager(tmp_path)
    spec = service.registry.spec('index_membership')
    producers = producers_for(spec, service.mappings.get(spec.name))
    proof = [p for p in producers if p['column'] in {'snapshot_complete','snapshot_member_count'}]
    assert len(proof) == 2
    assert all(p['kind'] == 'computed' and p['processor'] == 'index_snapshot' for p in proof)
    validate_producers(producers, spec, service.sources)
