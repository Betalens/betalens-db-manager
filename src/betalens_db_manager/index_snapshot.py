"""Completeness evidence from validated, unfiltered index-weight archives."""
from collections import defaultdict
from pathlib import Path
import shutil

from .source import read_archive
from .util import parse_date


def resolve_evidence(service, item):
    """Replay from retained immutable evidence after disposable cache cleanup."""
    record = service.state.ledger_get('index_snapshot_evidence', item['id']) or {}
    refs = {r['id']: r for r in record.get('archives', [])}
    parts = []
    for part in item['slices']:
        ref = refs.get(part['id'])
        if ref and part.get('archive') and not Path(part['archive']).is_file():
            if ref['sha'] != part['sha']:
                raise ValueError('指数快照保留归档哈希与冻结分片不一致')
            part = {**part, 'archive': ref['path']}
        parts.append(part)
    return {**item, 'slices': parts}


def retain_evidence(service, item, rows):
    """Snapshot proofs depend on raw requests, not disposable download caches."""
    if not any(r.get('snapshot_complete') == 1 for r in rows):
        return
    root = service.settings.state_dir / 'index-snapshot-evidence' / item['id']
    root.mkdir(parents=True, exist_ok=True)
    refs = []
    for part in item['slices']:
        if part['request']['endpoint'] != 'index_weight' or not part.get('archive'):
            continue
        original = Path(part['archive'])
        # Validate bytes before retaining; keep the fetched-at sidecar unchanged.
        read_archive(original, part['sha'])
        destination = root / original.name
        if original != destination:
            shutil.copy2(original, destination)
            shutil.copy2(original.with_suffix('.meta.json'), destination.with_suffix('.meta.json'))
        refs.append({'id': part['id'], 'path': str(destination), 'sha': part['sha'],
                     'request': part['request'], 'status': part['status'], 'result': part.get('result')})
    service.state.ledger_put('index_snapshot_evidence', item['id'],
                            {'task_id': item['id'], 'archives': refs,
                             'snapshot_count': len({(r['index_code'], str(r['trade_date'])) for r in rows})})


def annotate_validated_snapshots(item, rows):
    """Called only after validate_archives succeeds and conversion has no rejects.

    Counts come from the entire frozen retrieval chain, including pages and
    partitions, rather than a conventional index size or target row count.
    Custom mappings and constituent-filtered requests cannot acquire this proof.
    """
    bindings = item['plan']['mapping_snapshots'].get('index_membership', {}).get('bindings', [])
    from .response_validation import _manual_coverage_job
    if _manual_coverage_job.get() is not None:
        return
    if not any(b.get('source_id') == 'tushare' and b.get('endpoint') == 'index_weight'
               and b.get('mapping_mode', 'builtin') == 'builtin' for b in bindings):
        return
    source = defaultdict(set)
    seen = {}
    for part in item['slices']:
        request = part['request']
        if request['endpoint'] != 'index_weight' or request['source_id'] != 'tushare':
            continue
        if request['parameters'].get('con_code'):
            return
        if part['status'] == 'unavailable':
            return
        if part['status'] == 'split':
            continue
        data = read_archive(Path(part['archive']), part['sha'])
        for values in data['items']:
            raw = dict(zip(data['fields'], values))
            identity = raw['index_code'], raw['trade_date'], raw['con_code']
            if identity in seen and seen[identity] != raw:
                return
            seen[identity] = raw
            source[raw['index_code'], parse_date(raw['trade_date'])].add(raw['con_code'])
    actual = defaultdict(set)
    for row in rows:
        actual[row['index_code'], row['trade_date']].add(row['security_code'])
    for row in rows:
        key = row['index_code'], row['trade_date']
        members = source.get(key)
        if members and actual[key] == members:
            row['snapshot_complete'] = 1
            row['snapshot_member_count'] = len(members)
