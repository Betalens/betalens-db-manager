"""Archive source evidence before choosing a historical industry import contract."""
import argparse
import json
from pathlib import Path

from betalens_db_manager import DatabaseManagerService, Settings
from betalens_db_manager.source import read_archive
from betalens_db_manager.util import atomic_json, digest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    service = DatabaseManagerService(Settings.from_env(args.env))
    dictionaries = {}
    report = {'requests': [], 'memberships': []}

    def fetch(endpoint, parameters):
        path = args.output / (endpoint + '-' + digest(parameters)[:16] + '.json.gz')
        metadata = path.with_suffix('.meta.json')
        if path.exists() and metadata.exists():
            sha = json.loads(metadata.read_text())['sha256']
            data = read_archive(path, sha)
        else:
            fields = [f['name'] for f in service.sources.endpoint('tushare', endpoint)['fields']]
            data, sha = service.source.fetch(endpoint, parameters, fields, path)
        rows = [dict(zip(data['fields'], values)) for values in data['items']]
        report['requests'].append({'endpoint': endpoint, 'parameters': parameters,
                                   'archive': str(path), 'sha256': sha, 'rows': len(rows)})
        return rows

    for scheme in ['SW2014', 'SW2021']:
        for level in [1, 2, 3]:
            dictionaries[scheme, level] = {
                row['index_code']: row['industry_name']
                for row in fetch('index_classify', {'src': scheme, 'level': f'L{level}'})}
    for endpoint in ['index_member_all', 'ci_index_member']:
        for code in ['000001.SZ', '600000.SH', '000629.SZ']:
            for current in ['Y', 'N']:
                for row in fetch(endpoint, {'ts_code': code, 'is_new': current}):
                    matches = {scheme: {
                        str(level): dictionaries[scheme, level].get(row.get(f'l{level}_code')) == row.get(f'l{level}_name')
                        and row.get(f'l{level}_code') in dictionaries[scheme, level]
                        for level in [1, 2, 3]} for scheme in ['SW2014', 'SW2021']}
                    report['memberships'].append({'endpoint': endpoint, 'requested_is_new': current,
                                                   'source': row, 'dictionary_matches': matches})
    report['dictionary_sizes'] = {scheme: {str(level): len(dictionaries[scheme, level])
                                          for level in [1, 2, 3]} for scheme in ['SW2014', 'SW2021']}
    atomic_json(args.output / 'probe.json', report)
    print(json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
