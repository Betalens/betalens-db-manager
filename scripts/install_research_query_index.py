"""Preview and concurrently install the non-destructive execution-query index."""
import argparse
import time
from pathlib import Path

import psycopg2
from psycopg2 import sql

from betalens_db_manager import Settings
from betalens_db_manager.util import atomic_json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    settings = Settings.from_env(args.env)
    connection = psycopg2.connect(**settings.pg())
    connection.autocommit = True
    name = 'market_daily_post_adjusted_security_date_idx'
    statement = sql.SQL('CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {} (security_code,trade_date)').format(
        sql.Identifier(name), sql.Identifier(settings.pg_schema, 'market_daily_post_adjusted'))
    report = {'ddl': statement.as_string(connection), 'status': 'previewed', 'changes_business_rows': False}
    atomic_json(args.output / 'index-preview.json', report)
    if not args.execute:
        connection.close()
        return
    started = time.monotonic()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET lock_timeout='10min'")
            cursor.execute("SET statement_timeout='1800000'")
            cursor.execute("SET maintenance_work_mem='512MB'")
            cursor.execute('SET max_parallel_maintenance_workers=2')
            cursor.execute('SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s',
                           (settings.pg_schema, name))
            previous = cursor.fetchone()
            if previous and not previous[0]:
                # Only this migration's exact, unusable index is replaced.
                cursor.execute(sql.SQL('DROP INDEX CONCURRENTLY {}').format(sql.Identifier(settings.pg_schema, name)))
                report['replaced_invalid_index'] = True
            cursor.execute(statement)
            cursor.execute('SELECT i.indisvalid,i.indisready,pg_relation_size(c.oid) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname=%s',
                           (settings.pg_schema, name))
            valid, ready, size = cursor.fetchone()
            report.update(status='completed' if valid and ready else 'failed', valid=valid, ready=ready, bytes=size)
    except Exception as exc:
        report.update(status='failed', error=str(exc))
        raise
    finally:
        report['elapsed_seconds'] = time.monotonic() - started
        atomic_json(args.output / 'index-result.json', report)
        connection.close()


if __name__ == '__main__':
    main()
