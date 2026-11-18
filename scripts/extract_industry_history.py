"""Archive complete industry query chains with the db-manager source adapter."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path

from betalens_db_manager import DatabaseManagerService, Settings
from betalens_db_manager.source import read_archive
from betalens_db_manager.util import atomic_json, digest, now


def extract(args):
    service = DatabaseManagerService(Settings.from_env(args.env))
    root = args.output
    root.mkdir(parents=True, exist_ok=True)
    service.source.requests_per_minute = args.rpm
    service.source.retrieval_policies = {
        'tushare:' + endpoint: {'endpoint_rpm': args.rpm,
                               'basis': 'bounded_archived_history_extraction'}
        for endpoint in ['index_member', 'index_member_all', 'ci_index_member', 'index_classify']}
    completed = {}

    def fetch(endpoint, params, fields):
        path = root / 'source' / (endpoint + '-' + digest(params) + '.json.gz')
        meta_path = path.with_suffix('.meta.json')
        if path.exists() and meta_path.exists():
            meta = json.loads(meta_path.read_text())
            if meta['params'] != params or meta['fields'] != fields:
                raise ValueError('Archived request no longer matches its descriptor')
            data, sha = read_archive(path, meta['sha256']), meta['sha256']
        else:
            data, sha = service.source.fetch(endpoint, params, fields, path)
        if len(set(data['fields'])) != len(data['fields']) or set(data['fields']) != set(fields):
            raise ValueError('Unexpected source fields: ' + endpoint)
        if any(len(values) != len(data['fields']) for values in data['items']):
            raise ValueError('Source row width does not match fields')
        rows = [dict(zip(data['fields'], values)) for values in data['items']]
        return rows, {'archive': str(path), 'sha256': sha, 'parameters': params, 'rows': len(rows)}

    dictionaries = {}
    for scheme in ['SW2014', 'SW2021']:
        dictionaries[scheme] = []
        fields = [f['name'] for f in service.sources.endpoint('tushare', 'index_classify')['fields']]
        for level in [1, 2, 3]:
            params = {'src': scheme, 'level': f'L{level}'}
            rows, proof = fetch('index_classify', params, fields)
            if not rows or len(rows) >= 2000:
                raise ValueError('Classification catalog is empty or potentially truncated')
            if any(r.get('src') not in [None, '', 'SW', scheme] or r['level'] != f'L{level}' for r in rows):
                raise ValueError('Classification catalog changed version or level')
            dictionaries[scheme].extend(rows)
            completed['dictionary:' + scheme + ':' + str(level)] = proof
    atomic_json(root / 'dictionaries.json', dictionaries)

    # The existing CITICS catalog is a separately imported catalog, not a set
    # inferred from a saturated membership response. Preserve that basis.
    with service.pg.connect(readonly=True) as connection, connection.cursor() as cursor:
        cursor.execute(f'SELECT industry_code FROM "{service.settings.pg_schema}".industry_dim '
                       "WHERE scheme_code='CITICS' AND level='L1' ORDER BY industry_code")
        citics = [row[0] for row in cursor.fetchall()]
    if not citics:
        raise ValueError('Missing independent CITICS first-level catalog')
    descriptors = []
    for row in dictionaries['SW2014']:
        for flag in ['Y', 'N']:
            descriptors.append({'scheme': 'SW2014', 'endpoint': 'index_member',
                                'level': row['level'], 'industry_code': row['index_code'],
                                'params': {'index_code': row['index_code'], 'is_new': flag}})
    for scheme, codes, endpoint in [
        ('SW2021', [r['index_code'] for r in dictionaries['SW2021'] if r['level'] == 'L1'], 'index_member_all'),
        ('CITICS', citics, 'ci_index_member')]:
        for code in codes:
            for flag in ['Y', 'N']:
                descriptors.append({'scheme': scheme, 'endpoint': endpoint,
                                    'params': {'l1_code': code, 'is_new': flag}})
    atomic_json(root / 'extraction-plan.json', {
        'created_at': now(), 'start_date': args.start, 'end_date': args.end,
        'citics_catalog_basis': 'Existing db-manager imported first-level catalog; checked against fresh global source responses.',
        'citics_first_level_codes': citics, 'descriptors': descriptors})
    progress_path = root / 'extraction-progress.json'

    def chain(descriptor):
        endpoint = descriptor['endpoint']
        fields = (['index_code', 'con_code', 'in_date', 'out_date', 'is_new'] if endpoint == 'index_member'
                  else [f['name'] for f in service.sources.endpoint('tushare', endpoint)['fields']])
        pages, rows, seen = [], [], set()
        offset = 0
        # A successful terminal empty page is mandatory. Repeated or ignored
        # offset filters fail rather than declaring the short first page done.
        while True:
            params = {**descriptor['params'], 'offset': offset, 'limit': 2000}
            data, proof = fetch(endpoint, params, fields)
            pages.append(proof)
            if not data:
                break
            if any(r.get('is_new') != descriptor['params']['is_new'] for r in data):
                raise ValueError('Source ignored historical/current filter')
            if endpoint == 'index_member':
                if any(r.get('index_code') != descriptor['industry_code'] for r in data):
                    raise ValueError('Legacy source ignored industry filter')
            elif any(r.get('l1_code') != descriptor['params']['l1_code'] for r in data):
                raise ValueError('Source ignored first-level industry filter')
            hashes = {digest(r) for r in data}
            if len(hashes) != len(data) or hashes & seen:
                raise ValueError('Repeated or duplicate source page; completeness unproven')
            seen.update(hashes)
            rows.extend(data)
            offset += len(data)
            if len(pages) >= 100:
                raise ValueError('Unbounded page chain')
        value = {**descriptor, 'complete': True, 'pages': pages, 'rows': len(rows)}
        path = root / 'chains' / (digest(descriptor) + '.json')
        atomic_json(path, value)
        return digest(descriptor), {**value, 'chain_path': str(path)}

    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(chain, descriptor): descriptor for descriptor in descriptors}
        for future in as_completed(futures):
            descriptor = futures[future]
            try:
                key, result = future.result()
                completed[key] = result
            except Exception as exc:
                failures.append({**descriptor, 'error': service.safe_error(exc)})
            atomic_json(progress_path, {'updated_at': now(), 'completed': len(completed) - 6,
                                       'total': len(descriptors), 'failures': failures})
            if (len(completed) - 6) % 25 == 0 or failures:
                print('extraction', len(completed) - 6, '/', len(descriptors), 'failed', len(failures), flush=True)
    result = {'updated_at': now(), 'start_date': args.start, 'end_date': args.end,
              'status': 'failed' if failures else 'completed', 'failures': failures,
              'dictionaries': {scheme: len(rows) for scheme, rows in dictionaries.items()},
              'chains': [v for k, v in completed.items() if not k.startswith('dictionary:')]}
    atomic_json(root / 'extraction.json', result)
    if failures:
        raise RuntimeError('Historical source chains are incomplete; no database writes performed')
    print('completed', len(result['chains']), 'chains', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--start', default='2005-01-01')
    parser.add_argument('--end', default='2026-11-17')
    parser.add_argument('--rpm', type=int, default=200)
    parser.add_argument('--workers', type=int, default=4)
    extract(parser.parse_args())
