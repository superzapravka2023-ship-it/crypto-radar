import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import UTC, datetime


class Store:
    def __init__(self, url):
        self.postgres = url.startswith(("postgres://", "postgresql://"))
        if self.postgres:
            import psycopg

            self.db = psycopg.connect(url, autocommit=True, connect_timeout=10)
        elif url.startswith("sqlite:"):
            self.db = sqlite3.connect(url.removeprefix("sqlite:"), isolation_level=None)
        else:
            raise ValueError("Expected a PostgreSQL URL or sqlite:path")
        self.execute("""CREATE TABLE IF NOT EXISTS radar_records (
            kind TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL, ts BIGINT NOT NULL,
            PRIMARY KEY (kind, id))""")
        self.execute("CREATE INDEX IF NOT EXISTS radar_records_ts ON radar_records(kind, ts)")
        self.execute("""CREATE TABLE IF NOT EXISTS radar_budget (
            day TEXT PRIMARY KEY, micros BIGINT NOT NULL DEFAULT 0)""")
        self.execute("""CREATE TABLE IF NOT EXISTS radar_usage (
            id TEXT PRIMARY KEY, day TEXT NOT NULL, model TEXT NOT NULL,
            micros BIGINT NOT NULL, status TEXT NOT NULL, tokens_in BIGINT NOT NULL DEFAULT 0,
            tokens_out BIGINT NOT NULL DEFAULT 0)""")
        self.execute("CREATE INDEX IF NOT EXISTS radar_usage_day ON radar_usage(day)")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.db.close()

    def execute(self, sql, params=()):
        return self.db.execute(sql.replace("?", "%s") if self.postgres else sql, params)

    @contextmanager
    def transaction(self):
        if self.postgres:
            with self.db.transaction():
                yield
        else:
            self.execute("BEGIN IMMEDIATE")
            try:
                yield
                self.execute("COMMIT")
            except BaseException:
                self.execute("ROLLBACK")
                raise

    def put(self, kind, key, value, *, replace=True):
        conflict = "DO UPDATE SET payload=excluded.payload, ts=excluded.ts" if replace else "DO NOTHING"
        return self.execute(
            f"INSERT INTO radar_records VALUES (?, ?, ?, ?) ON CONFLICT (kind, id) {conflict}",
            (kind, str(key), json.dumps(value, ensure_ascii=False, allow_nan=False), int(time.time())),
        ).rowcount == 1

    def get(self, kind, key):
        row = self.execute("SELECT payload FROM radar_records WHERE kind=? AND id=?", (kind, str(key))).fetchone()
        return json.loads(row[0]) if row else None

    def recent(self, kind, since=0, limit=100):
        rows = self.execute(
            """SELECT id, payload FROM radar_records WHERE kind=? AND ts>=?
               ORDER BY ts DESC, id DESC LIMIT ?""", (kind, int(since), limit)
        ).fetchall()
        return [(key, json.loads(value)) for key, value in rows]

    def delete_before(self, kind, cutoff):
        self.execute("DELETE FROM radar_records WHERE kind=? AND ts<?", (kind, int(cutoff)))

    @staticmethod
    def today():
        return datetime.now(UTC).date().isoformat()

    def reserve(self, request_id, model, amount, cap):
        day = self.today()
        with self.transaction():
            self.execute("INSERT INTO radar_budget(day) VALUES (?) ON CONFLICT DO NOTHING", (day,))
            updated = self.execute(
                "UPDATE radar_budget SET micros=micros+? WHERE day=? AND micros+?<=?",
                (amount, day, amount, cap),
            ).rowcount
            if not updated:
                return False
            self.execute(
                "INSERT INTO radar_usage(id, day, model, micros, status) VALUES (?, ?, ?, ?, 'reserved')",
                (request_id, day, model, amount),
            )
        return True

    def settle(self, request_id, actual, tokens_in=0, tokens_out=0):
        with self.transaction():
            row = self.execute(
                "SELECT day, micros FROM radar_usage WHERE id=? AND status='reserved'", (request_id,)
            ).fetchone()
            if not row:
                return
            day, reserved = row
            cursor = self.execute(
                """UPDATE radar_usage SET micros=?, status='settled', tokens_in=?, tokens_out=?
                   WHERE id=? AND status='reserved'""",
                (actual, tokens_in, tokens_out, request_id),
            )
            if cursor.rowcount:
                self.execute("UPDATE radar_budget SET micros=micros+? WHERE day=?", (actual - reserved, day))

    def usage(self):
        return self.execute(
            """SELECT model, status, CAST(SUM(micros) AS BIGINT), COUNT(*),
               CAST(SUM(tokens_in) AS BIGINT), CAST(SUM(tokens_out) AS BIGINT)
               FROM radar_usage WHERE day=? GROUP BY model, status ORDER BY model, status""", (self.today(),)
        ).fetchall()
