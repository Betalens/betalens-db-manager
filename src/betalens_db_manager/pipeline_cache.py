"""Typed immutable Parquet snapshots and exact rollback images."""
import gzip,hashlib,json,os,tempfile
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from .util import digest,atomic_json,dumps

JOURNAL_FORMAT='gzip-json-v1'


def read_journal_metadata(record):
    """Read new compressed manifests and legacy pointers without changing hashes."""
    path=Path(record['path'])
    try:
        payload=path.read_bytes()
    except FileNotFoundError:
        # Migration keeps the original logical path and SHA in local/remote
        # receipts. Its compressed sidecar contains exactly the original bytes.
        payload=gzip.decompress(path.with_suffix(path.suffix+'.gz').read_bytes())
    if hashlib.sha256(payload).hexdigest()!=record['sha256']:raise ValueError('回退清单哈希不符')
    if path.suffix=='.gz':payload=gzip.decompress(payload)
    return json.loads(payload)


def _atomic_gzip(path,payload):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    descriptor,name=tempfile.mkstemp(prefix=path.name+'.',suffix='.tmp',dir=path.parent)
    try:
        with os.fdopen(descriptor,'wb') as stream:
            with gzip.GzipFile(filename='',fileobj=stream,mode='wb',compresslevel=1,mtime=0) as compressed:
                compressed.write(payload)
            stream.flush();os.fsync(stream.fileno())
        os.replace(name,path)
        if os.name=='posix':
            directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
            try:os.fsync(directory)
            finally:os.close(directory)
    finally:
        Path(name).unlink(missing_ok=True)


def compress_journal_manifest(record,dry_run=False):
    """Retain immutable audit bytes while retiring their uncompressed copy."""
    path=Path(record['path'])
    if path.suffix=='.gz' or not path.is_file() or path.is_symlink():return None
    before=path.stat();destination=path.with_suffix(path.suffix+'.gz')
    if dry_run:return {'files':1,'input_bytes':before.st_size,'bytes':0,'disk_bytes':0}
    payload=path.read_bytes()
    if hashlib.sha256(payload).hexdigest()!=record['sha256']:raise ValueError('旧审计清单哈希不符，保留原文件')
    _atomic_gzip(destination,payload)
    # Verify the entire compressed stream before dropping the original copy.
    if gzip.decompress(destination.read_bytes())!=payload:raise ValueError('审计清单压缩核验失败，保留原文件')
    current=path.stat()
    if (current.st_ino,current.st_size,current.st_mtime_ns)!=(before.st_ino,before.st_size,before.st_mtime_ns):
        raise ValueError('审计清单在压缩期间变化，保留原文件')
    after=destination.stat()
    if after.st_size>=before.st_size:
        destination.unlink();return None
    path.unlink()
    return {'files':1,'input_bytes':before.st_size,'bytes':before.st_size-after.st_size,
            'disk_bytes':max(0,getattr(before,'st_blocks',0)-getattr(after,'st_blocks',0))*512}


def schema(spec):
    types={'date':pa.date32(),'timestamp':pa.timestamp('us',tz='UTC'),'numeric':pa.decimal128(38,12),'integer':pa.int64(),'text':pa.string()}
    return pa.schema([pa.field(f['name'],pa.decimal128(f.get('precision',38),f.get('scale',12)) if f['type']=='numeric' else types[f['type']],nullable=True) for f in spec.fields])


def file_sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024**2),b''):h.update(block)
    return h.hexdigest()


def share_identical_snapshot(path,existing,*,expected_sha=None):
    """Atomically share immutable bytes, preserving both audit paths and hashes."""
    path,existing=Path(path),Path(existing)
    if path.is_symlink() or existing.is_symlink():return None
    before,source=path.stat(),existing.stat()
    if before.st_dev!=source.st_dev or before.st_size!=source.st_size:return None
    if before.st_ino==source.st_ino:return None
    actual=file_sha(existing)
    if expected_sha is not None and actual!=expected_sha:raise ValueError('镜像哈希不符，保留原文件')
    if file_sha(path)!=actual:
        if expected_sha is not None:raise ValueError('镜像哈希不符，保留原文件')
        return None
    def stamp(value):return value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns
    if stamp(path.stat())!=stamp(before) or stamp(existing.stat())!=stamp(source):
        raise ValueError('镜像在核验期间变化，保留原文件')
    descriptor,temporary=tempfile.mkstemp(prefix=path.name+'.share-',suffix='.tmp',dir=path.parent)
    os.close(descriptor);temporary=Path(temporary);temporary.unlink()
    try:
        os.link(existing,temporary,follow_symlinks=False)
        if stamp(temporary.stat())!=stamp(source) or stamp(path.stat())!=stamp(before):
            raise ValueError('镜像在共享期间变化，保留原文件')
        os.replace(temporary,path)
        if os.name=='posix':
            directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
            try:os.fsync(directory)
            finally:os.close(directory)
    finally:temporary.unlink(missing_ok=True)
    return {'files':1,'bytes':before.st_size if before.st_nlink==1 else 0,
            'disk_bytes':getattr(before,'st_blocks',0)*512 if before.st_nlink==1 else 0}


def write_rows(path,spec,rows):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    table=rows if isinstance(rows,pa.Table) else pa.Table.from_pylist(rows,schema=schema(spec))
    tmp=path.with_suffix('.tmp')
    pq.write_table(table,tmp,compression='zstd',row_group_size=50000)
    with tmp.open('rb') as h:os.fsync(h.fileno())
    tmp.replace(path)
    ranges={}
    import pyarrow.compute as pc
    for field in table.schema:
        if pa.types.is_date(field.type):
            limits=pc.min_max(table[field.name]).as_py()
            ranges[field.name]={**{k:str(v) if v is not None else None for k,v in limits.items()},'null_count':table[field.name].null_count}
    return {'path':str(path),'sha256':file_sha(path),'rows':len(table),'bytes':path.stat().st_size,'schema_hash':spec.hash,'ranges':ranges}


def read_rows(record,spec,columns=None,filters=None):
    path=Path(record['path'])
    if spec.hash!=record['schema_hash'] or file_sha(path)!=record['sha256']:raise ValueError('标准化缓存或镜像校验失败')
    return pq.read_table(path,schema=schema(spec),columns=columns,filters=filters).to_pylist()


def save_journal(root,spec,comparison,expected=None):
    root=Path(root);root.mkdir(parents=True,exist_ok=True)
    changes=comparison['changes']
    before=write_rows(root/'before.parquet',spec,[c['before'] or {} for c in changes])
    after=write_rows(root/'after.parquet',spec,[c['after'] or {} for c in changes])
    meta={**{k:v for k,v in comparison.items() if k not in {'changes','conflicts'}},'table':spec.name,'before':before,'after':after,
          'changes':[{k:v for k,v in c.items() if k not in {'before','after'}}|{'has_before':c['before'] is not None,'has_after':c['after'] is not None} for c in changes]}
    if expected is not None:
        meta['expected']=write_rows(root/'expected.parquet',spec,expected)
        if meta['expected']['sha256']==after['sha256']:
            try:share_identical_snapshot(meta['expected']['path'],after['path'],expected_sha=after['sha256'])
            except OSError:pass  # Filesystems without hard links retain both verified copies.
    path=root/'manifest.json.gz'
    _atomic_gzip(path,dumps(meta).encode('utf-8'))
    return {'path':str(path),'sha256':file_sha(path),'table':spec.name}


def load_journal(record,spec):
    meta=read_journal_metadata(record);before=read_rows(meta['before'],spec);after=read_rows(meta['after'],spec)
    return {**{k:v for k,v in meta.items() if k not in {'before','after','table'}},
            'changes':[{**{k:v for k,v in c.items() if k not in {'has_before','has_after'}},
                        'before':before[i] if c['has_before'] else None,'after':after[i] if c['has_after'] else None} for i,c in enumerate(meta['changes'])]}
