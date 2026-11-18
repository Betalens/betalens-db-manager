"""Resolve industry parents only through the archived, scheme-local catalog."""
from .util import digest

POLICY = 'industry-parent-index-namespace-v1'


def membership_scheme(endpoint, raw, dictionaries):
    """The unversioned tiered endpoint cannot establish SW2014 membership."""
    if endpoint == 'ci_index_member':
        return 'CITICS'
    if endpoint != 'index_member_all':
        raise ValueError('不支持的行业成员接口')
    catalog = dictionaries.get('SW2021', {})
    for level in (1, 2, 3):
        dim = catalog.get(raw.get(f'l{level}_code'))
        if (not dim or dim.get('level') != f'L{level}' or
                dim.get('industry_name') != raw.get(f'l{level}_name') or
                (level > 1 and dim.get('parent_code') != raw.get(f'l{level-1}_code'))):
            raise ValueError('index_member_all 缺少完整 SW2021 字典证据，禁止按接口名或纳入日期标注版本')
    return 'SW2021'


def archived_dictionaries(item):
    """Read only SHA-verified, explicitly versioned dictionary requests."""
    from pathlib import Path
    from .source import read_archive
    rows = []
    for part in item.get('slices', []):
        request = part.get('request', {})
        if request.get('endpoint') != 'index_classify' or request.get('source_id') != 'tushare':
            continue
        if not part.get('archive') or part['status'] not in {'downloaded', 'paged'}:
            continue
        scheme = request.get('parameters', {}).get('src')
        if scheme not in {'SW2014', 'SW2021'}:
            raise ValueError('申万字典请求必须明确 src=SW2014/SW2021')
        data = read_archive(Path(part['archive']), part['sha'])
        for values in data['items']:
            raw = dict(zip(data['fields'], values))
            if raw.get('src') not in {None, '', 'SW', scheme}:
                raise ValueError('申万字典响应与请求版本不一致')
            rows.append(dict(scheme_code=scheme, industry_code=raw['index_code'],
                             industry_name=raw['industry_name'], parent_code=raw.get('parent_code'),
                             classification_code=raw.get('industry_code'), level=raw['level'],
                             source_id='tushare', source_endpoint='index_classify'))
    normalize_parents(rows)
    result = {}
    for row in rows:
        catalog = result.setdefault(row['scheme_code'], {})
        previous = catalog.get(row['industry_code'])
        if previous is not None and previous != row:
            raise ValueError('归档行业字典存在冲突')
        catalog[row['industry_code']] = row
    return result


def normalize_parents(rows):
    aliases = {}
    levels = {}
    for row in rows:
        if row.get('source_id') != 'tushare' or row.get('_binding_identity'):continue
        scheme, code = row.get('scheme_code'), row.get('industry_code')
        if not scheme or not code:continue
        levels.setdefault((scheme, code), set()).add(row.get('level'))
        classification = row.get('classification_code')
        if classification:
            aliases.setdefault((scheme, classification), set()).add(code)
    for row in rows:
        if row.get('source_id') != 'tushare' or row.get('_binding_identity') or row.get('source_endpoint') != 'index_classify':continue
        scheme, parent, level = row.get('scheme_code'), row.get('parent_code'), row.get('level')
        if parent in (None, '', '0'):
            if parent == '0' and level != 'L1':
                raise ValueError('行业父级为0但当前行业不是L1')
            normalized = None
        else:
            candidates = set(aliases.get((scheme, parent), ()))
            if (scheme, parent) in levels:candidates.add(parent)
            if len(candidates) != 1:
                raise ValueError(f'行业父代码缺少同体系唯一映射：{scheme}/{parent}')
            normalized = next(iter(candidates))
            expected = {'L2':'L1', 'L3':'L2'}.get(level)
            if normalized == row.get('industry_code') or expected is None or levels.get((scheme, normalized)) != {expected}:
                raise ValueError(f'行业父子层级不一致：{scheme}/{row.get("industry_code")} → {normalized}')
        if parent == normalized:continue
        row['parent_code'] = normalized
        row['mapping_version'] = digest([row.get('mapping_version'), POLICY])
        if '_field_origins' in row:
            row['_field_origins']['parent_code'] = {
                'selected':normalized is not None, 'source_null':parent in (None, ''),
                'value_hash':digest(normalized),
                'origins':['tushare:index_classify:'+name for name in ('parent_code','industry_code','index_code')]}
