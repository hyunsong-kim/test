#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RabbitMQ -> Oracle DB Consumer (batch loader)
- Separate background process
- Uses pika (AMQP) and python-oracledb (thin mode, no Oracle Client required)
- Manual ack AFTER successful DB commit (idempotent-ish)
- Batch flush on size or time interval
- Config via config.yaml

Requirements (requirements.txt):
    pika>=1.3
    python-oracledb>=1.5
    PyYAML>=6.0

Run:
    python consumer.py
"""
import json
import time
import signal
import sys
import threading
from datetime import datetime

import pika
import oracledb
import yaml


class GracefulExit(Exception):
    pass


def install_signal_handlers():
    def handler(signum, frame):
        raise GracefulExit()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


class BatchBuffer:
    def __init__(self, size: int):
        self.size = size
        self.rows = []
        self.tags = []  # RabbitMQ delivery_tag list (ack after commit)
        self.lock = threading.Lock()
        self.last_flush_ts = time.monotonic()

    def add(self, row, tag):
        with self.lock:
            self.rows.append(row)
            self.tags.append(tag)

    def should_flush(self, flush_seconds: int) -> bool:
        with self.lock:
            if len(self.rows) >= self.size:
                return True
            if (time.monotonic() - self.last_flush_ts) >= flush_seconds and len(self.rows) > 0:
                return True
            return False

    def pop_all(self):
        with self.lock:
            rows, tags = self.rows, self.tags
            self.rows, self.tags = [], []
            self.last_flush_ts = time.monotonic()
            return rows, tags


def connect_rabbit(cfg):
    credentials = pika.PlainCredentials(cfg["user"], cfg["password"])
    params = pika.ConnectionParameters(
        host=cfg.get("host", "127.0.0.1"),
        port=int(cfg.get("port", 5672)),
        virtual_host=cfg.get("vhost", "/"),
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=300,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=cfg["queue"], durable=True, auto_delete=False)
    ch.basic_qos(prefetch_count=int(cfg.get("prefetch", 1000)))
    return conn, ch


def connect_oracle(cfg):
    # Thin mode (no Oracle Client install required)
    return oracledb.connect(user=cfg["user"], password=cfg["password"], dsn=cfg["dsn"])


def bulk_insert(con, table, rows):
    """
    rows: list[dict] with keys: Id, CustNo, Amount, OrderTs (ISO-8601 or datetime)
    """
    if not rows:
        return

    ids = []
    cust_nos = []
    amts = []
    tss = []

    for r in rows:
        ids.append(r["Id"])
        cust_nos.append(r["CustNo"])
        amts.append(r.get("Amount"))
        ts = r.get("OrderTs")
        if isinstance(ts, datetime):
            tss.append(ts)
        elif isinstance(ts, str):
            # Accept "YYYY-MM-DDTHH:MM:SS" (with or without timezone)
            # datetime.fromisoformat supports many ISO-8601 variants in Python 3.11+
            tss.append(datetime.fromisoformat(ts))
        else:
            tss.append(None)

    sql = f"INSERT INTO {table}(ID, CUST_NO, AMOUNT, ORDER_TS) VALUES (:1,:2,:3,:4)"
    with con.cursor() as cur:
        cur.executemany(sql, list(zip(ids, cust_nos, amts, tss)))
    con.commit()


def run_consumer(cfg):
    rcfg = cfg["rabbitmq"]
    ocfg = cfg["oracle"]
    lcfg = cfg["loader"]

    conn, ch = connect_rabbit(rcfg)
    con = connect_oracle(ocfg)

    buffer = BatchBuffer(size=int(lcfg.get("batch_size", 500)))
    queue = rcfg["queue"]
    table = lcfg["table"]
    flush_seconds = int(lcfg.get("flush_seconds", 2))

    stop_flag = {"stop": False}

    def periodic_flush():
        if stop_flag["stop"]:
            return
        try:
            if buffer.should_flush(flush_seconds):
                rows, tags = buffer.pop_all()
                if rows:
                    bulk_insert(con, table, rows)
                    # Ack after successful commit
                    for tag in tags:
                        ch.basic_ack(tag)
        except Exception as e:
            # On failure, do NOT ack -> messages remain in queue for retry
            print("[FLUSH ERROR]", repr(e), file=sys.stderr)
        finally:
            # re-arm timer
            threading.Timer(1.0, periodic_flush).start()

    periodic_flush()

    def on_message(channel, method, properties, body):
        try:
            data = json.loads(body)
            buffer.add(data, method.delivery_tag)
            # ack deferred to flush
        except Exception as e:
            print("[MSG ERROR] nack (drop or route to DLQ):", repr(e), file=sys.stderr)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    ch.basic_consume(queue=queue, on_message_callback=on_message, auto_ack=False)
    print("Consumer started. Ctrl+C to stop.")
    try:
        ch.start_consuming()
    except GracefulExit:
        pass
    except Exception as e:
        print("[CONSUME ERROR]", repr(e), file=sys.stderr)
    finally:
        # final flush on exit
        try:
            rows, tags = buffer.pop_all()
            if rows:
                bulk_insert(con, table, rows)
                for tag in tags:
                    ch.basic_ack(tag)
        except Exception as e:
            print("[FINAL FLUSH ERROR]", repr(e), file=sys.stderr)
        stop_flag["stop"] = True
        try:
            ch.stop_consuming()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    install_signal_handlers()
    # Load config
    cfg_path = "config.yaml"
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"[ERROR] {cfg_path} not found. Create it next to consumer.py.", file=sys.stderr)
        sys.exit(1)
    run_consumer(cfg)
