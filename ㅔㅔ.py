import asyncio, json, os, signal, time, yaml
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Any, Tuple

import aio_pika
import oracledb

# ---------------- Load Config ----------------
with open("config.yaml", "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

RMQ_URL = cfg["rabbitmq"]["url"]
RMQ_QUEUE = cfg["rabbitmq"]["queue"]
PREFETCH = cfg["rabbitmq"]["prefetch"]

ORA_DSN = cfg["oracle"]["dsn"]
ORA_USER = cfg["oracle"]["user"]
ORA_PASS = cfg["oracle"]["password"]

FLUSH_SIZE = cfg["consumer"]["flush_size"]
FLUSH_INTERVAL_MS = cfg["consumer"]["flush_interval_ms"]

# ---------------- Structures ----------------
@dataclass
class TableBuffer:
    rows: List[Dict[str, Any]] = field(default_factory=list)
    tags: List[aio_pika.IncomingMessage] = field(default_factory=list)
    last_flush_ms: int = 0
    cols_cache: List[str] = field(default_factory=list)
    insert_sql: str = ""

def norm_table(table: str) -> Tuple[str, str, str]:
    t = (table or "").strip()
    if "." in t:
        owner, name = t.split(".", 1)
    else:
        owner, name = "APP", t
    return f"{owner.upper()}.{name.upper()}", owner.upper(), name.upper()

def fetch_table_columns(conn, owner: str, name: str) -> List[str]:
    sql = """
        SELECT column_name
        FROM   all_tab_columns
        WHERE  owner = :own AND table_name = :t
           AND NVL(identity_column,'NO') = 'NO'
           AND NVL(virtual_column,'NO') = 'NO'
        ORDER  BY column_id
    """
    with conn.cursor() as cur:
        cur.execute(sql, own=owner, t=name)
        return [r[0].upper() for r in cur.fetchall()]

def build_insert_sql(full_table: str, cols: List[str]) -> str:
    bind_list = ", ".join([f":{c}" for c in cols])
    col_list = ", ".join(cols)
    return f"INSERT /*+ APPEND_VALUES */ INTO {full_table} ({col_list}) VALUES ({bind_list})"

def filter_row(cols: List[str], row: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for c in cols:
        v = None
        if c in row: v = row[c]
        else:
            for k, val in row.items():
                if k.upper() == c: v = val; break
        out[c] = v
    return out

# ---------------- Consumer ----------------
class OracleBatchConsumer:
    def __init__(self, loop):
        self.loop = loop
        self.conn = None
        self.buffers: Dict[str, TableBuffer] = defaultdict(TableBuffer)
        self.seen_keys: Dict[str, float] = {}
        self.stop_event = asyncio.Event()
        self.flush_task = None

    async def start(self):
        self.conn = oracledb.connect(user=ORA_USER, password=ORA_PASS, dsn=ORA_DSN)
        self.conn.autocommit = False
        self.flush_task = self.loop.create_task(self._periodic_flush())

        self.rmq_conn: aio_pika.RobustConnection = await aio_pika.connect_robust(RMQ_URL, loop=self.loop)
        self.channel: aio_pika.abc.AbstractChannel = await self.rmq_conn.channel()
        await self.channel.set_qos(prefetch_count=PREFETCH)
        queue = await self.channel.declare_queue(RMQ_QUEUE, durable=True)
        await queue.consume(self.on_message, no_ack=False)
        print(f"[*] Consuming {RMQ_QUEUE} (prefetch={PREFETCH}, flush={FLUSH_SIZE}/{FLUSH_INTERVAL_MS}ms)")

    async def stop(self):
        self.stop_event.set()
        if self.flush_task:
            await self.flush_task
        await self._flush_all("shutdown")
        if self.channel: await self.channel.close()
        if self.rmq_conn: await self.rmq_conn.close()
        if self.conn:
            try: self.conn.commit()
            except: pass
            self.conn.close()

    async def _periodic_flush(self):
        while not self.stop_event.is_set():
            await asyncio.sleep(FLUSH_INTERVAL_MS/1000.0)
            await self._flush_due("timer")

    async def on_message(self, msg: aio_pika.IncomingMessage):
        async with msg.process(ignore_processed=True, requeue=False):
            try:
                payload = json.loads(msg.body.decode("utf-8"))
                table = payload.get("Table")
                key = payload.get("Key")
                data = payload.get("Payload") or {}

                if not table or not isinstance(data, dict):
                    return

                if key and key in self.seen_keys:
                    return
                if key:
                    self.seen_keys[key] = time.time()

                full_table, owner, name = norm_table(table)
                tb = self.buffers[full_table]

                if not tb.cols_cache:
                    tb.cols_cache = fetch_table_columns(self.conn, owner, name)
                    if not tb.cols_cache:
                        print(f"[WARN] No columns for {full_table}; dropping")
                        return
                    tb.insert_sql = build_insert_sql(full_table, tb.cols_cache)

                row = filter_row(tb.cols_cache, {k.upper(): v for k, v in data.items()})
                tb.rows.append(row)
                tb.tags.append(msg)
                now_ms = int(time.time()*1000)
                if tb.last_flush_ms == 0: tb.last_flush_ms = now_ms

                if len(tb.rows) >= FLUSH_SIZE:
                    await self._flush_table(full_table, tb, "size")
                    tb.last_flush_ms = now_ms
            except Exception as e:
                print(f"[ERR] on_message: {e}")
                raise

    async def _flush_due(self, reason: str):
        now_ms = int(time.time()*1000)
        for full_table, tb in list(self.buffers.items()):
            if not tb.rows: continue
            if now_ms - tb.last_flush_ms >= FLUSH_INTERVAL_MS:
                await self._flush_table(full_table, tb, reason)
                tb.last_flush_ms = now_ms

    async def _flush_all(self, reason: str):
        for full_table, tb in list(self.buffers.items()):
            if tb.rows:
                await self._flush_table(full_table, tb, reason)

    async def _flush_table(self, full_table: str, tb: TableBuffer, reason: str):
        rows, tags = tb.rows, tb.tags
        if not rows: return
        tb.rows, tb.tags = [], []
        sql = tb.insert_sql
        try:
            with self.conn.cursor() as cur:
                cur.executemany(sql, rows, batcherrors=True)
                errs = cur.getbatcherrors()
                if errs:
                    print(f"[WARN] {full_table} batch errors: {len(errs)}")
                self.conn.commit()
            for m in tags:
                await m.ack()
            print(f"[FLUSH] {full_table}: {len(rows)} rows ({reason})")
        except Exception as e:
            print(f"[ERR] flush {full_table}: {e}")
            for m in tags:
                try: await m.reject(requeue=False)
                except: pass

async def main():
    loop = asyncio.get_running_loop()
    c = OracleBatchConsumer(loop)

    stop = asyncio.Event()
    def _grace(*_): stop.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: loop.add_signal_handler(sig, _grace)
        except NotImplementedError: pass

    await c.start()
    await stop.wait()
    await c.stop()

if __name__ == "__main__":
    asyncio.run(main())
