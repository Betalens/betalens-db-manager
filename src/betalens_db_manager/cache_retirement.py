"""Explicit retirement of completely rolled-back task families."""
from pathlib import Path
import json
import os

from .download_cache import cache_lock
from .util import digest,now


def retire_rolled_back_tasks(service, *, include_new=False, job=None, dry_run=False, progress=None):
    state=service.state
    summary={'tasks':0,'files':0,'bytes':0,'disk_bytes':0,'retained_files':0,'errors':[],'results':[]}
    with cache_lock(state.root):
        with state.connect() as conn:
            retired={r[0] for r in conn.execute("SELECT row_key FROM ledger WHERE table_name='cache_retirement' AND json_extract(value,'$.status')!='retired'")}
            if not include_new and not retired:return summary
            jobs={r['id']:dict(r) for r in conn.execute("""SELECT id,status,
                json_extract(plan,'$.pipeline_parent') parent FROM jobs""")}
            def root_of(identity):
                seen=set()
                while identity in jobs and jobs[identity]['parent']:
                    if identity in seen:return None
                    seen.add(identity);identity=jobs[identity]['parent']
                return identity if identity in jobs else None
            roots={identity:root_of(identity) for identity in jobs}
            busy={roots.get(r[0]) for r in conn.execute("SELECT DISTINCT job FROM tasks WHERE status IN ('queued','running')")}
            candidates={identity for identity,row in jobs.items() if not row['parent'] and row['status']=='rolled_back'
                and identity not in busy and (include_new or identity in retired) and (job is None or job==identity)}
            tables={r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'pipeline_receipts' in tables:
                # A status label alone cannot retire an incompletely undone write.
                candidates-={r[0] for r in conn.execute("""SELECT DISTINCT o.job FROM pipeline_receipts o
                    WHERE o.batch NOT LIKE 'rollback:%' AND NOT EXISTS(
                        SELECT 1 FROM pipeline_receipts r WHERE r.job=o.job AND r.target=o.target
                        AND r.batch='rollback:'||o.batch AND r.status='completed')""")}
            if not candidates:return summary
            owners={identity:root for identity,root in roots.items() if root in candidates}
            bases=[Path(service.settings.archive_dir or service.settings.state_dir/'archive'),
                   service.settings.state_dir/'pipeline',service.settings.state_dir/'reference-cache']
            resolved_bases=[base.resolve() for base in bases]
            existing_dirs=[str(base/identity) for identity in owners for base in bases if (base/identity).is_dir()]
            signature=digest([sorted(candidates),sorted(busy,key=str),sorted(existing_dirs),
                sorted((identity,row['status']) for identity,row in jobs.items() if not row['parent'])])
            if not dry_run and not include_new and getattr(service,'_cache_retirement_signature',None)==signature:return summary
            protected=set();protected_dirs=set()
            def protect(value,directory=False):
                if not value:return
                path=Path(value)
                if not path.is_absolute():path=state.root/path
                path=Path(os.path.abspath(path))
                for base in resolved_bases:
                    try:relative=path.relative_to(base)
                    except ValueError:continue
                    if relative.parts and relative.parts[0] in owners:
                        path=path.resolve()
                        if directory:protected_dirs.add(path.parent)
                        else:
                            protected.update((path,path.with_suffix('.meta.json'),path.with_suffix('.tmp')))
                            if path.name.endswith('.json.gz'):
                                protected.add(path.with_name(path.name[:-8]+'.response.json.gz'))
            conn.execute('CREATE TEMP TABLE retiring_jobs(id TEXT PRIMARY KEY)')
            conn.executemany('INSERT INTO retiring_jobs VALUES(?)',((identity,) for identity in owners))
            for r in conn.execute("""SELECT DISTINCT archive FROM slices s LEFT JOIN retiring_jobs x ON x.id=s.job
                WHERE x.id IS NULL AND archive IS NOT NULL"""):protect(r[0])
            for r in conn.execute("""SELECT value FROM ledger l LEFT JOIN retiring_jobs x
                ON x.id=json_extract(l.value,'$.task_id') WHERE l.table_name='file_ingestion' AND x.id IS NULL"""):
                protect(json.loads(r[0]).get('source_path'))
            if 'pipeline_receipts' in tables:
                for r in conn.execute("""SELECT manifest FROM pipeline_receipts p LEFT JOIN retiring_jobs x ON x.id=p.job
                    WHERE x.id IS NULL"""):
                    protect(json.loads(r[0])['path'],directory=True)
            if 'pipeline_units' in tables:
                for r in conn.execute("""SELECT json_extract(result,'$.child_id') child FROM pipeline_units p
                    LEFT JOIN retiring_jobs x ON x.id=p.job WHERE x.id IS NULL AND result IS NOT NULL"""):
                    if r[0] in owners:protected_dirs.add((service.settings.state_dir/'pipeline'/r[0]).resolve())

        results={root:{'task_id':root,'files':0,'bytes':0,'disk_bytes':0,'retained_files':0,'errors':[]} for root in candidates}
        if not dry_run:
            for root in candidates:
                previous=state.ledger_get('cache_retirement',root) or {}
                state.ledger_put('cache_retirement',root,{**previous,'task_id':root,'status':'retiring',
                    'reason':'用户明确选择彻底退役已完整回退的旧任务','checked_at':now()})
        for identity,root in owners.items():
            if Path(identity).name!=identity or identity in {'.','..'}:continue
            result=results[root]
            for base in bases:
                directory=base/identity
                if not directory.is_dir() or directory.is_symlink():continue
                def walk_error(exc):
                    if len(result['errors'])<20:result['errors'].append({'path':str(exc.filename),'error':str(exc)})
                for current,dirs,files in os.walk(directory,followlinks=False,onerror=walk_error):
                    result['retained_files']+=sum((Path(current)/d).is_symlink() for d in dirs)
                    dirs[:]=[d for d in dirs if not (Path(current)/d).is_symlink()]
                    for name in files:
                        path=Path(current)/name
                        if path.is_symlink() or path in protected or any(path.is_relative_to(p) for p in protected_dirs):
                            result['retained_files']+=1;continue
                        try:
                            stat=path.stat()
                            if not dry_run:path.unlink()
                            result['files']+=1;result['bytes']+=stat.st_size
                            result['disk_bytes']+=getattr(stat,'st_blocks',0)*512
                        except FileNotFoundError:pass
                        except OSError as exc:
                            if len(result['errors'])<20:result['errors'].append({'path':str(path),'error':str(exc)})
                    if progress:progress(root,result)
                if not dry_run:
                    for current,_,_ in os.walk(directory,topdown=False,followlinks=False):
                        try:Path(current).rmdir()
                        except OSError:pass
        for root,result in results.items():
            for key in ('files','bytes','disk_bytes','retained_files'):summary[key]+=result[key]
            summary['errors'].extend(result['errors'])
            if result['files'] or result['retained_files'] or result['errors']:
                summary['tasks']+=1;summary['results'].append(result)
            if not dry_run:
                previous=state.ledger_get('cache_retirement',root) or {}
                status='failed' if result['errors'] else 'retired_shared' if result['retained_files'] else 'retired'
                state.ledger_put('cache_retirement',root,{**previous,**result,'status':status,'checked_at':now(),
                    **{key:previous.get(key,0)+result[key] for key in ('files','bytes','disk_bytes')}})
                if result['files'] or result['errors']:state.log(root,'cache_retirement',**{k:v for k,v in result.items() if k!='task_id'})
        if not dry_run and not summary['errors']:service._cache_retirement_signature=signature
    return summary
