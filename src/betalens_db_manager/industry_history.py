"""Auditable projections of archived industry history; no database I/O."""
from collections import Counter, defaultdict
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from .industry_codes import normalize_parents
from .util import digest, parse_date

SWITCH = parse_date('2021-12-13')
RESEARCH_VERSION_POLICY = 'source_version_intervals;retrospective_research_only'
RESEARCH_FIELDS = [
    {'name': 'research_valid_from', 'type': 'date', 'zh_name': '研究口径适用起日', 'nullable': True,
     'description': '追溯研究的版本选择起点，不是历史公开时间'},
    {'name': 'research_valid_to', 'type': 'date', 'zh_name': '研究口径适用止日', 'nullable': True,
     'description': '追溯研究的版本选择止日，左闭右开'},
    {'name': 'research_selection_basis', 'type': 'text', 'zh_name': '研究版本选择依据', 'nullable': True,
     'description': '显式研究口径，不用于严格PIT可得时间证明'},
]


def project(dictionaries, inputs, start='2005-01-01', end='2026-11-17'):
    """Keep source intervals; the managed scheme window controls applicability."""
    first, last = parse_date(start), parse_date(end)
    dimensions, lineage, memberships, rejected, quarantined, conflicts = {}, {}, {}, [], [], []
    catalog_rows = []
    for scheme, rows in dictionaries.items():
        for raw in rows:
            if raw.get('src') not in [None, '', 'SW', scheme]:
                raise ValueError('Dictionary version differs from its archived request')
            catalog_rows.append({'scheme_code': scheme, 'industry_code': raw['index_code'],
                                 'industry_name': raw['industry_name'], 'parent_code': raw.get('parent_code'),
                                 'level': raw['level'], 'classification_code': raw.get('industry_code'),
                                 'src': raw.get('src'), 'is_pub': raw.get('is_pub'),
                                 'source_id': 'tushare', 'source_endpoint': 'index_classify'})
    normalize_parents(catalog_rows)
    for row in catalog_rows:
        row = {k: v for k, v in row.items() if k not in {'source_id', 'source_endpoint'}}
        dimensions[row['scheme_code'], row['industry_code']] = row

    for item in inputs:
        scheme, endpoint, raw = item['scheme'], item['endpoint'], item['row']
        code = raw.get('con_code') if endpoint == 'index_member' else raw.get('ts_code')
        try:
            begin = parse_date(raw['in_date'])
            stop = parse_date(raw['out_date']) if raw.get('out_date') else None
            if begin > last or (stop is not None and stop <= first):
                continue
            if scheme == 'SW2014' and begin >= SWITCH:
                continue
            if stop is not None and stop == begin:
                quarantined.append({**item, 'reason':'Source interval has no duration under the database half-open contract; inclusivity unproven'})
                continue
            if stop is not None and stop < begin:
                raise ValueError('Source membership interval is reversed')
            if not code or not str(code).endswith(('.SH', '.SZ', '.BJ')):
                raise ValueError('Source security code is not a domestic stock')
            if endpoint == 'index_member':
                industry = raw.get('index_code')
                if scheme != 'SW2014' or (scheme, industry) not in dimensions:
                    raise ValueError('Legacy membership has no versioned dictionary evidence')
                levels = [(industry, dimensions[scheme, industry])]
            else:
                expected = 'SW2021' if endpoint == 'index_member_all' else 'CITICS'
                if scheme != expected:
                    raise ValueError('Source endpoint cannot establish the requested scheme')
                levels = []
                for level in [1, 2, 3]:
                    industry, name = raw.get(f'l{level}_code'), raw.get(f'l{level}_name')
                    if not industry or not name:
                        raise ValueError('Source is missing a classification level')
                    dim = {'scheme_code': scheme, 'industry_code': industry, 'industry_name': name,
                           'parent_code': raw.get(f'l{level-1}_code') if level > 1 else None,
                           'level': f'L{level}'}
                    previous = dimensions.get((scheme, industry))
                    if previous is not None and any(previous.get(k) != dim[k]
                                                    for k in ['industry_name', 'parent_code', 'level']):
                        raise ValueError('Membership classification differs from the versioned dictionary')
                    dimensions.setdefault((scheme, industry), dim)
                    levels.append((industry, dimensions[scheme, industry]))
            available = datetime.combine(begin + timedelta(days=1), time.min, ZoneInfo('Asia/Shanghai')).isoformat()
            for industry, dim in levels:
                row = {'scheme_code': scheme, 'security_code': code, 'industry_code': industry,
                       'valid_from': begin.isoformat(), 'valid_to': stop.isoformat() if stop else None,
                       'membership_status': 'current' if raw.get('is_new') == 'Y' else 'historical',
                       'event_date': begin.isoformat(), 'available_at': available,
                       'availability_basis': 'membership_effective_date_assumed;historical_disclosure_unproven'}
                key = (scheme, code, industry, row['valid_from'])
                if key in memberships and memberships[key] != row:
                    conflicts.append({'key': key, 'previous': memberships[key], 'incoming': row})
                    continue
                memberships[key] = row
                lineage.setdefault(digest(key), []).append({
                    'endpoint': endpoint, 'source_record_hash': digest(raw),
                    'archive': item['archive'], 'archive_sha256': item['archive_sha256'],
                    'classification_version': scheme, 'level': dim['level']})
        except (KeyError, TypeError, ValueError) as exc:
            rejected.append({'endpoint': endpoint, 'scheme': scheme, 'source': raw,
                             'reason': str(exc), 'archive': item['archive']})
    if conflicts:
        raise ValueError('Conflicting source membership keys: ' + str(conflicts[:3]))
    overlaps = []
    groups = defaultdict(list)
    for row in memberships.values():
        groups[(row['scheme_code'], dimensions[row['scheme_code'],row['industry_code']]['level'],row['security_code'])].append(row)
    for key, rows in groups.items():
        rows.sort(key=lambda r:(r['valid_from'],r['industry_code']))
        for n, left in enumerate(rows):
            for right in rows[n+1:]:
                begin = max(left['valid_from'],right['valid_from'],start)
                stop = min(left['valid_to'] or '9999-12-31',right['valid_to'] or '9999-12-31',
                           SWITCH.isoformat() if key[0]=='SW2014' else (last+timedelta(days=1)).isoformat())
                if begin < stop:
                    overlaps.append({'scheme':key[0],'level':key[1],'security_code':key[2],
                                     'overlap_from':begin,'overlap_to':stop,'left':left,'right':right,
                                     'handling':'Preserve both source intervals; ambiguous downstream queries fail closed'})
    counts = Counter(row['scheme_code'] for row in memberships.values())
    return {'industry_dim': list(dimensions.values()), 'security_industry_membership': list(memberships.values()),
            'lineage': lineage, 'rejected': rejected, 'quarantined': quarantined, 'overlaps':overlaps,
            'membership_counts': dict(counts)}


def scheme_windows(rows, start='2005-01-01'):
    """Preserve event/publication fields and record a separate research window."""
    result = []
    for original in rows:
        row = dict(original)
        scheme = row['scheme_code']
        if scheme not in {'SW2014', 'SW2021', 'CITICS'}:
            continue
        row.update(research_valid_from='2021-12-13' if scheme == 'SW2021' else start,
                   research_valid_to='2021-12-13' if scheme == 'SW2014' else None,
                   research_selection_basis=RESEARCH_VERSION_POLICY)
        result.append(row)
    if {row['scheme_code'] for row in result} != {'SW2014', 'SW2021', 'CITICS'}:
        raise ValueError('Missing explicit scheme metadata; publication dates must not be invented')
    return result
