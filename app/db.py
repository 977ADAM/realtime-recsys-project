import os
from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

try:
    from psycopg_pool import ConnectionPool
except ImportError:  # optional dependency
    ConnectionPool = None

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/recsys")
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"

_pool = None


def _get_pool():
    global _pool
    if ConnectionPool is None:
        return None
    if _pool is None:
        min_size = int(os.getenv("DB_POOL_MIN_SIZE", "1"))
        max_size = int(os.getenv("DB_POOL_MAX_SIZE", "10"))
        _pool = ConnectionPool(
            conninfo=DATABASE_URL,
            min_size=min_size,
            max_size=max_size,
            kwargs={"row_factory": dict_row},
            open=True,
        )
    return _pool


@contextmanager
def get_conn():
    pool = _get_pool()
    if pool is not None:
        with pool.connection() as conn:
            yield conn
        return

    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction():
    with get_conn() as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def init_db(schema_path: Path = SCHEMA_PATH):
    ddl = schema_path.read_text(encoding="utf-8")
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)


def close_pool():
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
