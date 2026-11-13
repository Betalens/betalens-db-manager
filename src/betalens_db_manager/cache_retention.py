"""Retire reproducible task caches, retaining compressed immutable audit evidence."""
from pathlib import Path
from contextlib import contextmanager
import hashlib
import json

from .download_cache import TERMINAL, cache_lock, cleanup_download_cache
from .pipeline_cache import compress_journal_manifest, read_journal_metadata
from .util import now

POLICY_VERSION = 'task-cache-v2-gzip-20261113'
LOADED_AT = now()
LOADED_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _safe_cache(path, directory):
    path = Path(path)
    if directory.absolute()!=directory.resolve():
        return False
    try:
        relative = path.relative_to(directory)
    except ValueError:
        return False
    if '..' in relative.parts or path.is_symlink():
        return False
    return path.resolve().is_relative_to(directory.resolve()) and not any(
        (directory / Path(*relative.parts[:i])).is_symlink() for i in range(1, len(relative.parts)))


@contextmanager
def retirement_guard(state, identity):
    """Fence one packet, allowing queue admission between maintenance packets."""
    with cache_lock(state.root):
        with state.connect() as conn:
            row=conn.execute('SELECT status FROM jobs WHERE id=?',(identity,)).fetchone()
            busy=conn.execute("""SELECT 1 FROM tasks t JOIN jobs j ON j.id=t.job
                WHERE t.status IN ('queued','running') AND
                (t.job=? OR json_extract(j.plan,'$.pipeline_parent')=?) LIMIT 1""",(identity,identity)).fetchone()
        yield bool(row and row[0] in TERMINAL and not busy)


def cleanup_derived_caches(service, *, job=None, dry_run=False, compression_budget_bytes=None, progress=None, exclude_jobs=()):
    """Delete acknowledged normalization copies, and compress terminal journals.

    A running/queued operation fences its entire task family. Rejected cohorts,
    journal Parquet images and reference audits are never removed. A normalized
    packet is disposable only when a durable expected image can replace it in
    subsequent validation for every still-selected target.
    """
    summary = {'files': 0, 'bytes': 0, 'disk_bytes': 0, 'manifests': 0,
               'manifest_input_bytes': 0, 'compression_pending': False, 'errors': [], 'results': []}
    state = service.state
    pipeline = service.settings.state_dir / 'pipeline'
    if not pipeline.is_dir():
        return summary
    with cache_lock(state.root):
        with state.connect() as conn:
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {'pipeline_units', 'pipeline_receipts'} <= tables:
                return summary
            jobs = {r['id']: dict(r) for r in conn.execute("""SELECT id,status,
                json_extract(plan,'$.pipeline_parent') parent,
                json_extract(plan,'$.request.targets') targets FROM jobs""")}

            def root_of(identity):
                seen = set()
                while identity in jobs and jobs[identity]['parent']:
                    if identity in seen:
                        return None
                    seen.add(identity)
                    identity = jobs[identity]['parent']
                return identity if identity in jobs else None

            roots = {identity: root_of(identity) for identity in jobs}
            blocked = {roots.get(r[0]) for r in conn.execute(
                "SELECT DISTINCT job FROM tasks WHERE status IN ('queued','running')")}
            blocked.update(r[0] for r in conn.execute("SELECT row_key FROM ledger WHERE table_name='cache_retirement'"))
            blocked.update(exclude_jobs)
            eligible = [identity for identity, row in jobs.items() if not row['parent']
                        and row['status'] in TERMINAL and identity not in blocked
                        and (job is None or identity == job)]

    for identity in eligible:
        with state.connect() as conn:
            units = [json.loads(r[0]) for r in conn.execute("""SELECT result FROM pipeline_units
                WHERE job=? AND kind='normalize' AND status='completed' AND result IS NOT NULL""", (identity,))]
            receipts = [dict(r) for r in conn.execute(
                'SELECT batch,target,status,manifest FROM pipeline_receipts WHERE job=?', (identity,))]
        if not units and not receipts:
            continue
        result = {'task_id': identity, 'files': 0, 'bytes': 0, 'disk_bytes': 0,
                  'manifests': 0, 'manifest_input_bytes': 0, 'errors': []}
        by_batch = {(r['batch'], r['target']): r for r in receipts}
        targets = json.loads(jobs[identity]['targets'] or '[]')
        seen_files = set()
        cleanup_recorded = False

        def remove(path):
            nonlocal cleanup_recorded
            if path in seen_files or not path.is_file():
                return
            seen_files.add(path)
            stat = path.stat()
            if not dry_run:
                if not cleanup_recorded:
                    previous=state.ledger_get('derived_cache_cleanup',identity) or {}
                    state.ledger_put('derived_cache_cleanup',identity,{**previous,'task_id':identity,
                        'policy_version':POLICY_VERSION,'status':'cleaning','checked_at':now()})
                    cleanup_recorded=True
                path.unlink()
            result['files'] += 1
            result['bytes'] += stat.st_size
            result['disk_bytes'] += getattr(stat, 'st_blocks', 0) * 512

        for unit in units:
            with retirement_guard(state, identity) as available:
                if not available:
                    break
                child = unit.get('child_id')
                if not child or roots.get(child) != identity or unit.get('rejected'):
                    continue
                metadata = {}
                directory = pipeline / child
                all_disposable = True
                for table, records in unit.get('tables', {}).items():
                    for index, record in enumerate(records):
                        path = Path(record['path'])
                        if not path.is_file():
                            continue
                        # Normalization output is a table subdirectory, never
                        # an intent/rollback directory referenced by receipts.
                        if not _safe_cache(path, directory / table) or path.suffix != '.parquet':
                            all_disposable = False
                            continue
                        batch = child + ':' + table + ':' + str(index)
                        try:
                            backed = bool(targets)
                            for target in targets:
                                receipt = by_batch.get((batch, target))
                                if not receipt or receipt['status'] != 'completed':
                                    backed = False; break
                                if (batch, target) not in metadata:
                                    metadata[batch, target] = read_journal_metadata(json.loads(receipt['manifest']))
                                expected = metadata[batch, target].get('expected')
                                if not expected or not Path(expected['path']).is_file():
                                    backed = False; break
                            if backed:
                                remove(path)
                            else:
                                all_disposable = False
                        except (OSError, ValueError, KeyError) as exc:
                            all_disposable = False
                            result['errors'].append({'path': str(path), 'error': str(exc)})
                index_path = directory / 'normalized.json'
                if all_disposable and _safe_cache(index_path, directory):
                    try:remove(index_path)
                    except OSError as exc:result['errors'].append({'path': str(index_path), 'error': str(exc)})
        seen_manifests = set()
        for receipt in receipts:
            record = json.loads(receipt['manifest']); path = Path(record['path'])
            if path in seen_manifests or path.suffix == '.gz' or not path.is_file():
                continue
            seen_manifests.add(path)
            if not _safe_cache(path, pipeline / identity):
                continue
            if compression_budget_bytes is not None and summary['manifest_input_bytes'] + result['manifest_input_bytes'] >= compression_budget_bytes:
                summary['compression_pending'] = True
                break
            with retirement_guard(state, identity) as available:
                if not available:
                    break
                try:
                    compressed = compress_journal_manifest(record, dry_run=dry_run)
                    if compressed:
                        result['manifests'] += compressed['files']
                        result['manifest_input_bytes'] += compressed['input_bytes']
                        result['bytes'] += compressed['bytes']
                        result['disk_bytes'] += compressed['disk_bytes']
                except (OSError, ValueError, EOFError) as exc:
                    result['errors'].append({'path': str(path), 'error': str(exc)})
            if progress:
                progress(identity, result)

        for key in ('files', 'bytes', 'disk_bytes', 'manifests', 'manifest_input_bytes'):
            summary[key] += result[key]
        summary['errors'].extend(result['errors'][:20])
        if any((result['files'], result['manifests'], result['errors'])):
            summary['results'].append(result)
            if not dry_run:
                previous = state.ledger_get('derived_cache_cleanup', identity) or {}
                state.ledger_put('derived_cache_cleanup', identity, {
                    **result, **{k: previous.get(k, 0) + result[k] for k in ('files','bytes','disk_bytes','manifests','manifest_input_bytes')},
                    'policy_version': POLICY_VERSION, 'checked_at': now(),
                    'status': 'failed' if result['errors'] else 'cleaned'})
                state.log(identity, 'derived_cache_cleanup', **{k: v for k, v in result.items() if k != 'task_id'})
    return summary


def cleanup_task_caches(service, *, job=None, dry_run=False, compression_budget_bytes=None, progress=None, retire_rolled_back=False):
    from .cache_retirement import retire_rolled_back_tasks
    retirement=retire_rolled_back_tasks(service,include_new=retire_rolled_back,job=job,dry_run=dry_run,progress=progress)
    retired_jobs={r['task_id'] for r in retirement['results']}
    raw = cleanup_download_cache(service, job=job, dry_run=dry_run,exclude_jobs=retired_jobs)
    derived = cleanup_derived_caches(service, job=job, dry_run=dry_run,
                                     compression_budget_bytes=compression_budget_bytes, progress=progress,exclude_jobs=retired_jobs)
    return {**raw, 'policy_version': POLICY_VERSION, 'raw_files': raw['files'], 'raw_bytes': raw['bytes'],
            **{k: raw[k] + derived[k] + retirement[k] for k in ('files', 'bytes', 'disk_bytes')},
            'errors': raw['errors'] + derived['errors'] + retirement['errors'], 'derived': derived,'retirement':retirement}
