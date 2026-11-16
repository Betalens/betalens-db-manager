"""Whole-market daily suspension acquisition and immutable publication.

One packet proves a market day, including an empty list. Only actual events
are stored; listing/calendar evidence is shared by each monthly revision.
"""
from collections import defaultdict
from copy import deepcopy
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import uuid
import re

from .util import digest, dumps, now, parse_date, atomic_json

FORMAT = 'suspension-market-day-v3'


def selector(day):
    return {'market': 'CN_A', 'trade_date': str(day)[:10]}


def day_id(day):
    return digest({'module': 'suspension', 'selector': selector(day)})


def build_scopes(request, bundles):
    from .reference_domains import _scope, _suspension_rows, _proof, _date
    stocks = [b for b in bundles if b['endpoint'] == 'stock_basic']
    statuses = {b['parameters'].get('list_status') for b in stocks if b.get('complete')}
    universe_complete = {'L', 'D', 'P', 'G', 'UN'} <= statuses
    securities = {}
    for bundle in stocks:
        if not bundle.get('complete'):
            continue
        for row in bundle['rows']:
            code = row.get('ts_code')
            if not code or code.rsplit('.', 1)[-1] not in {'SH', 'SZ', 'BJ'}:
                continue
            life = {'list_date': _date(row.get('list_date')), 'delist_date': _date(row.get('delist_date')),
                    'list_status': row.get('list_status'), 'market': row.get('market')}
            life = {k: v.isoformat() if isinstance(v, date) else v for k, v in life.items()}
            if code in securities and securities[code] != life:
                raise ValueError('上市范围冲突: ' + code)
            securities[code] = life
    mappings=[b for b in bundles if b['endpoint']=='bse_mapping']
    aliases={}
    for bundle in mappings:
        if not bundle.get('complete'):continue
        for row in bundle['rows']:
            old,new=row.get('o_code'),row.get('n_code')
            if not old or not new or not old.endswith('.BJ') or not new.endswith('.BJ'):
                raise ValueError('北交所映射代码无效')
            if old in aliases and aliases[old]!=new:raise ValueError('北交所旧代码对应多个证券')
            if new not in securities:raise ValueError('北交所新代码缺少上市范围: '+new)
            aliases[old]=new
    if len(set(aliases.values()))!=len(aliases):raise ValueError('北交所映射不是唯一对应')
    unknown={r['ts_code'] for b in bundles if b['endpoint']=='suspend_d' for r in b['rows']
             if r.get('ts_code') not in securities and r.get('ts_code') not in aliases}
    unresolved={c:{'complete':False,'reason':'unresolved_cross_endpoint_identifier'} for c in unknown if re.fullmatch(r'T\d{5,6}\.SH',c or '')}
    if unknown-set(unresolved):raise ValueError('停牌事件证券不在完整股票清单或源端代码映射: '+str(sorted(unknown-set(unresolved))[:5]))
    universe = {'complete': universe_complete, 'securities': securities, 'queries': _proof(stocks),
                'aliases':aliases,'alias_queries':_proof(mappings),
                'alias_basis':'source_bse_mapping_without_code_switch_date',
                'unresolved_source_codes':unresolved,
                'calendar_basis':{'BSE':'reference_SSE'}}
    pool = {digest(universe): universe}
    universe_ref = digest(universe)
    calendars = defaultdict(dict)
    for bundle in bundles:
        if bundle['endpoint'] != 'trade_cal' or not bundle.get('complete'):
            continue
        for row in bundle['rows']:
            exchange = row.get('exchange') or bundle['parameters'].get('exchange')
            day = _date(row.get('cal_date'))
            if day is None or row.get('is_open') not in (0, 1):
                raise ValueError('日历日期或开市标志无效')
            value = {'is_open': int(row['is_open']), 'previous_trade_date': str(row.get('pretrade_date') or '')}
            if day.isoformat() in calendars[exchange] and calendars[exchange][day.isoformat()] != value:
                raise ValueError('日历冲突')
            calendars[exchange][day.isoformat()] = value
    calendar_refs = defaultdict(dict)
    for exchange, values in calendars.items():
        months=defaultdict(dict)
        for day,value in values.items():months[day[:7]][day]=value
        for month,days in months.items():
            value = {'exchange': exchange, 'days': days}
            ref = digest(value); pool[ref] = value; calendar_refs[month][exchange] = ref
    scopes = []
    for bundle in bundles:
        if bundle['endpoint'] != 'suspend_d':
            continue
        p = bundle['parameters']; day = _date(p.get('trade_date'))
        if day is None or p.get('ts_code') or p.get('suspend_type'):
            raise ValueError('全市场停牌凭证必须为无证券/类型过滤的单日查询')
        rows = []; seen = set()
        for number, row in enumerate(bundle['rows'], 1):
            if _date(row.get('trade_date')) != day or row.get('suspend_type') not in {'S', 'R'}:
                raise ValueError('停牌响应日期或事件类型无效')
            identity = digest(row)
            if identity in seen:
                raise ValueError('停牌响应存在重复事件')
            seen.add(identity)
            rows.extend(_suspension_rows(row['ts_code'], day, [(bundle, number, row)]))
        complete = bool(bundle.get('complete')) and universe_complete and all(
            day.isoformat() in calendars.get(e, {}) for e in ('SSE', 'SZSE'))
        evidence = {'event_query_complete': bool(bundle.get('complete')), 'universe_ref': universe_ref,
                    'calendar_refs': dict(calendar_refs[day.isoformat()[:7]]), 'queries': _proof([bundle]),
                    'timezone': 'Asia/Shanghai', 'interval_convention': 'left_closed_right_open',
                    'source_intraday_endpoint_inclusivity': 'unknown',
                    'dependency_basis': 'self_contained_archived_source_queries'}
        scope = _scope('suspension', selector(day), rows, complete,
                       [] if complete else ['query_or_dependency_incomplete'], evidence)
        scope['format'] = FORMAT; scopes.append(scope)
    if len({s['scope_id'] for s in scopes}) != len(scopes):
        raise ValueError('全市场日期凭证重复')
    return sorted(scopes, key=lambda s: s['selector']['trade_date']), pool


def validate_scopes(scopes, pool):
    for ref, value in pool.items():
        if digest(value) != ref:
            raise ValueError('全市场停牌共享证据哈希不匹配')
    for scope in scopes:
        day = scope['selector'].get('trade_date')
        if scope.get('format') != FORMAT or scope['selector'] != selector(day) or scope['scope_id'] != day_id(day):
            raise ValueError('全市场停牌范围标识无效')
        evidence = scope['evidence']
        universe = pool[evidence['universe_ref']]
        known=set(universe['securities']) | set(universe.get('aliases',{})) | set(universe.get('unresolved_source_codes',{}))
        for ref in evidence['calendar_refs'].values():
            if ref not in pool: raise ValueError('日历共享证据缺失')
        for row in scope.get('rows', []):
            if str(row['trade_date'])[:10] != day or row['security_code'] not in known:
                raise ValueError('停牌事件超出全市场覆盖范围')


class MarketWorkflow:
    def __init__(self, service):
        self.s = service

    def plan(self, value):
        from .pipeline_options import options, DEFAULTS
        from .retrieval import tushare_policy, request_rate
        from .source_registry import source_identity
        from .reference_contracts import VERSION, CONTRACT_HASH
        from .pipeline_plan import params_for
        request = deepcopy(value)
        allowed={'modules','targets','security_codes','start_date','end_date','source_id',
                 'suspension_update_mode','update_mode','requests_per_minute','parent_task_id',
                 'exchanges','schemes','levels','check_times'} | set(DEFAULTS)
        if set(request)-allowed:raise ValueError('全市场停牌请求包含未登记字段')
        if request.get('source_id','tushare')!='tushare':raise ValueError('全市场停牌模式当前仅支持 Tushare')
        if request.get('modules', ['suspension']) != ['suspension'] or request.get('security_codes'):
            raise ValueError('market_day 任务仅接受全市场 suspension 模块')
        start = parse_date(request.get('start_date', '2000-01-03'))
        yesterday = datetime.now(ZoneInfo('Asia/Shanghai')).date() - timedelta(days=1)
        end = parse_date(request.get('end_date', yesterday.isoformat()))
        if start < date(2000, 1, 3) or start > end or end > yesterday:
            raise ValueError('停牌全历史日期须从 2000-01-03 起，且不超过最近完整自然日')
        if request.get('update_mode', 'full') not in {'full', 'incremental'}:
            raise ValueError('更新模式须为 full 或 incremental')
        request.update(options(request))
        request.update(modules=['suspension'], security_codes=[], source_id='tushare',
                       start_date=start.isoformat(), end_date=end.isoformat(),
                       targets=request.get('targets', ['postgres', 'clickhouse']),
                       requests_per_minute=min(500, request_rate(request.get('requests_per_minute', 500))),
                       download_workers=min(4, request.get('download_workers', 4)),
                       check_times=[], suspension_update_mode='market_day', update_mode=request.get('update_mode', 'full'))
        if not request['targets'] or set(request['targets']) - {'postgres','clickhouse'}:
            raise ValueError('目标库无效')
        roots = [{'source_id':'tushare','endpoint':'stock_basic','parameters':{'list_status':status},
                  'strategy':'reference_scope','dependency_only':True} for status in ('L','D','P','G','UN')]
        for exchange in ('SSE','SZSE'):
            for params in params_for('trade_cal', request, {'exchange':exchange}):
                roots.append({'source_id':'tushare','endpoint':'trade_cal','parameters':params,'strategy':'reference_scope','dependency_only':True})
        roots.append({'source_id':'tushare','endpoint':'bse_mapping','parameters':{},'strategy':'reference_scope','dependency_only':True})
        contracts = {'tushare:'+name:deepcopy(self.s.sources.endpoint('tushare',name)) for name in ('stock_basic','trade_cal','suspend_d','bse_mapping')}
        plan = {'revision':'reference-v1','pipeline_version':2,'suspension_storage':FORMAT,
                'request':request,'requests':roots,'source_contracts':contracts,
                'retrieval_policies':{k:tushare_policy(k.split(':')[1],5000) for k in contracts},
                'mapping_version':VERSION,'contract_hash':CONTRACT_HASH,
                'source_identity':source_identity(self.s.sources.values['tushare'])}
        item=self.s.state.create_job(plan,self.s.settings.fingerprint)
        self.s.state.log(item['id'],'suspension_market_task_created',start_date=str(start),end_date=str(end),update_mode=request['update_mode'])
        return item

    def acquire(self, item):
        from .downloader import TaskDownloader
        from .reference_service import ReferenceWorkflow
        from .reference_domains import _date
        job=item['id']; plan=item['plan']; req=plan['request']
        self.s.state.update_job(job,status='downloading',error=None)
        self.s.source.active_job=job; self.s.source.requests_per_minute=req['requests_per_minute']
        self.s.source.retrieval_policies=plan['retrieval_policies']
        dependencies=[r for r in plan['requests'] if r.get('dependency_only')]
        with TaskDownloader(self.s,item) as downloader:
            downloader.run(dependencies,workers=req['download_workers'])
        dependency_item=self.s.state.job(job)
        dependency_item['plan']={**dependency_item['plan'],'requests':dependencies}
        bundles=ReferenceWorkflow(self.s).bundles(dependency_item)
        if any(not b['complete'] for b in bundles):raise ValueError('日历或股票清单依赖未完整获取')
        dates=set()
        for bundle in bundles:
            if bundle['endpoint']=='trade_cal':
                dates.update(_date(r['cal_date']).isoformat() for r in bundle['rows'] if r['is_open']==1)
        dates=sorted(d for d in dates if req['start_date']<=d<=req['end_date'])
        if req['update_mode']=='incremental' and len(dates)>20:
            from .reference_storage import ReferenceStore
            common=set(dates)
            for target in req['targets']:
                heads=ReferenceStore(self.s,target).heads([day_id(d) for d in dates])
                common &= {d for d in dates if heads.get(day_id(d))}
            dates=sorted(set(dates)-common | set(dates[-20:]))
        roots=dependencies+[{'source_id':'tushare','endpoint':'suspend_d','parameters':{'trade_date':d.replace('-','')},'strategy':'reference_scope'} for d in dates]
        plan={**plan,'requests':roots,'market_trade_dates':dates}
        self.s.state.replace_plan(job,plan)
        self.s.state.log(job,'suspension_market_dates_frozen',trading_days=len(dates),first=dates[0] if dates else None,last=dates[-1] if dates else None)
        with TaskDownloader(self.s,self.s.state.job(job)) as downloader:
            downloader.run([r for r in roots if not r.get('dependency_only')],workers=req['download_workers'])
        self.s.state.update_job(job,status='downloaded')
        return []

    def audit(self, job):
        from .reference_service import ReferenceWorkflow
        from .reference_storage import ReferenceStore
        item=self.s.state.job(job);bundles=ReferenceWorkflow(self.s).bundles(item)
        scopes,pool=build_scopes(item['plan']['request'],bundles);validate_scopes(scopes,pool)
        expected=item['plan'].get('market_trade_dates',[])
        if sorted(s['selector']['trade_date'] for s in scopes)!=expected:
            raise ValueError('全市场停牌日期清单不完整')
        cache={'format':FORMAT,'scopes':scopes,'market_evidence':pool,
               'archives':[[p['id'],p['sha'],p['status']] for p in item['slices']]}
        path=self.s.settings.state_dir/'reference-cache'/job/'audit.json';atomic_json(path,cache)
        heads={t:ReferenceStore(self.s,t).heads([s['scope_id'] for s in scopes]) for t in item['plan']['request']['targets']}
        rejected=sum(not s['complete'] for s in scopes)
        preview={'job':job,'cache_path':str(path),'cache_hash':digest(cache),'format':FORMAT,
                 'scope_count':len(scopes),'row_count':sum(len(s['rows']) for s in scopes),
                 'target_heads':heads,'targets':item['plan']['request']['targets'],
                 'blocking':bool(rejected or not scopes),'rejected':rejected,
                 'archives':cache['archives'],
                 'changes':[{'target':t,'module':'suspension','operation':'replace_market_days',
                             'selector':{'market':'CN_A','start_date':item['plan']['request']['start_date'],'end_date':item['plan']['request']['end_date']},
                             'rows':sum(len(s['rows']) for s in scopes),'scope_count':len(scopes),
                             'before':[],'after':[r for scope in scopes[:5] for r in scope['rows'][:5]]} for t in heads],
                 'checks':[{'target':t,'status':'ready','scope_count':len(scopes)} for t in heads]}
        preview['token']=digest(preview);self.s.state.update_job(job,preview=preview,status='ready_to_commit')
        self.s.state.log(job,'suspension_market_audited',scope_count=len(scopes),row_count=preview['row_count'],rejected=rejected)
        return preview

    def publish(self, job, token, *, stage_only=False):
        from .reference_service import ReferenceWorkflow
        from .reference_storage import ReferenceStore
        from .reference_contracts import VERSION
        item=self.s.state.job(job);preview=item.get('preview') or {}
        if token!=preview.get('token') or preview.get('blocking'):raise ValueError('全市场停牌审计令牌无效')
        cache=json.loads(Path(preview['cache_path']).read_text())
        if digest(cache)!=preview['cache_hash']:raise ValueError('停牌审计缓存已变化')
        bundles=ReferenceWorkflow(self.s).bundles(item)
        regenerated,pool=build_scopes(item['plan']['request'],bundles)
        if digest(regenerated)!=digest(cache['scopes']) or digest(pool)!=digest(cache['market_evidence']):
            raise ValueError('停牌来源归档已变化，必须重新审计')
        months=defaultdict(list)
        for scope in cache['scopes']:months[scope['selector']['trade_date'][:7]].append(scope)
        staged=[];results=[]
        self.s.state.update_job(job,status='committing',error=None)
        for month,scopes in sorted(months.items()):
            for target in item['plan']['request']['targets']:
                key=job+':'+target+':suspension:'+month
                revision=self.s.state.ledger_get('reference_revision',key)
                if revision is None:
                    revision={'revision_id':uuid.uuid4().hex,'import_task_id':job,'task_id':job,'attempt_id':job,
                              'target':target,'module':'suspension','format':FORMAT,'mapping_version':VERSION,
                              'source_name':'tushare','source_id':'tushare','downloaded_at':max(b['downloaded_at'] for b in bundles),
                              'request':{**item['plan']['request'],'start_date':scopes[0]['selector']['trade_date'],'end_date':scopes[-1]['selector']['trade_date']},
                              'market_evidence':{ref:cache['market_evidence'][ref] for ref in
                                  {s['evidence']['universe_ref'] for s in scopes} |
                                  {ref for s in scopes for ref in s['evidence']['calendar_refs'].values()}}}
                    self.s.state.ledger_put('reference_revision',key,revision)
                self.s.state.check_cancel(job)
                store=ReferenceStore(self.s,target);store.stage(revision,scopes)
                verified=store.verify(revision['revision_id'])
                if not verified['verified']:raise ValueError('停牌目标库读回核验失败')
                expected={s['scope_id']:preview['target_heads'][target].get(s['scope_id']) for s in scopes}
                staged.append((key,target,store,revision,expected,verified))
                self.s.state.log(job,'suspension_market_month_verified',target=target,month=month,rows=verified['row_count'])
        # Every month on both targets is verified before the first publication.
        if stage_only:
            result={'job':job,'status':'staged','verified_month_targets':len(staged),
                    'row_count':sum(v['row_count'] for _,_,_,_,_,v in staged)}
            self.s.state.ledger_put('suspension_market_stage',job,result)
            self.s.state.update_job(job,status='ready_to_commit')
            return result
        for key,target,store,revision,expected,verified in staged:
            self.s.state.check_cancel(job)
            event=store.publish(revision['revision_id'],expected)
            row={'task_id':job,'target':target,'module':'suspension','status':'published',
                 'revision_id':revision['revision_id'],'verification':verified,'publication':event}
            self.s.state.ledger_put('reference_result',key,row);results.append(row)
        self.s.state.ledger_put('reference_task_result',job,{'task_id':job,'status':'completed','results':results,'finished_at':now()})
        self.s.state.update_job(job,status='completed',error=None)
        return {'job':job,'status':'completed','results':results}
