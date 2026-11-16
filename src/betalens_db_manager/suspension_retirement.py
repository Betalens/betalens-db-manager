"""Streaming evidence for retiring dense status rows without changing facts."""
from hashlib import sha256
from pathlib import Path
import json

import requests
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from .reference_storage import _typed
from .util import dumps

PREDICATE="status_source IS NULL OR status_source NOT IN ('derived_from_daily_quote','security_lifecycle')"


def table_signature(service,target,table,*,retained_only=False,db=None):
    """Sorted canonical business rows; target-local ingestion time is omitted."""
    spec=service.registry.spec('trade_status_daily')
    fields=[f for f in spec.fields if f['name']!='ingested_at']
    keys=list(spec.keys);h=sha256();count=0
    def consume(rows):
        nonlocal count
        for row in rows:
            record={f['name']:_typed(f,row.get(f['name'])) for f in fields}
            h.update(dumps(record).encode());h.update(b'\n');count+=1
    if target=='postgres':
        adapter=db or service.pg
        with adapter.connect(readonly=True) as connection:
            with connection.cursor(name='suspension_retirement_scan',cursor_factory=RealDictCursor) as cur:
                query=sql.SQL('SELECT {} FROM {}').format(sql.SQL(',').join(sql.Identifier(f['name']) for f in fields),sql.Identifier(adapter.settings.pg_schema,table))
                if retained_only:query+=sql.SQL(' WHERE '+PREDICATE)
                query+=sql.SQL(' ORDER BY {}').format(sql.SQL(',').join(map(sql.Identifier,keys)))
                cur.itersize=5000;cur.execute(query)
                while True:
                    rows=cur.fetchmany(5000)
                    if not rows:break
                    consume(rows)
    else:
        adapter=db or service.ch
        names=','.join('`'+f['name']+'`' for f in fields)
        query='SELECT '+names+' FROM '+adapter.table(table)+' FINAL'
        if retained_only:query+=' WHERE '+PREDICATE
        query+=' ORDER BY '+','.join('`'+k+'`' for k in keys)+' FORMAT JSONEachRow'
        response=requests.post(adapter.settings.ch_url,data=query.encode(),auth=(adapter.settings.ch_user,adapter.settings.ch_password),
                               params={'max_threads':3,'max_memory_usage':34359738368,'date_time_output_format':'iso'},stream=True,timeout=(10,1800))
        response.raise_for_status()
        for line in response.iter_lines():
            if line:consume([json.loads(line)])
    return {'rows':count,'sha256':h.hexdigest()}


def verify_archive_files(report):
    """Require the completed full-restore proof and SHA256 of every archive."""
    pg=report['postgres'];ch=report['clickhouse']
    if not pg.get('restored_rows') or ch.get('restored_rows')!=ch.get('original_rows') or not ch.get('content_checksum'):
        raise ValueError('旧表恢复演练证据不完整')
    files=[(Path(pg['path']),pg['sha256'])]
    files.extend((Path(ch['archive_directory'])/(part+'.native.gz'),value['sha256']) for part,value in ch['partitions'].items())
    for path,expected in files:
        h=sha256()
        with path.open('rb') as stream:
            for block in iter(lambda:stream.read(4*1024**2),b''):h.update(block)
        if h.hexdigest()!=expected:raise ValueError('旧表恢复归档哈希变化: '+str(path))
    return {'verified_files':len(files),'bytes':sum(path.stat().st_size for path,_ in files)}
