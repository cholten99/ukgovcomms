#!/usr/bin/env python3
"""
Export the `Source` table to a UTF-8 CSV (friendly for Google Docs).

Usage:
  python3 tools/export_sources_csv.py --out sources.csv --log-level INFO
  # Optional filters:
  python3 tools/export_sources_csv.py --out sources.csv --kind Blog --enabled 1
"""

import os
import sys
import csv
import argparse
import logging
from pathlib import Path

import pymysql

COLUMNS = [
    "id", "name", "url", "kind", "is_enabled",
    "last_checked", "last_success", "status_code", "notes",
    "created_at", "updated_at",
]

def load_env(env_path=".env"):
    if not os.path.exists(env_path):
        logging.info(".env not found at %s; relying on environment.", env_path)
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
    except Exception as e:
        logging.warning("Failed to read .env: %s", e)

def get_conn():
    host = os.environ.get("DB_HOST", "localhost")
    name = os.environ.get("DB_NAME", "UKGovComms")
    user = os.environ.get("DB_USER")
    pwd  = os.environ.get("DB_PASSWORD")
    if not (user and pwd):
        raise RuntimeError("DB_USER/DB_PASSWORD not set in env/.env")
    return pymysql.connect(host=host, user=user, password=pwd, database=name, charset="utf8mb4")

def export_sources(out_path, kind=None, enabled=None):
    conn = get_conn()
    try:
        where = []
        params = []
        if kind:
            where.append("kind = %s")
            params.append(kind)
        if enabled is not None:
            where.append("is_enabled = %s")
            params.append(int(enabled))
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        sql = f"""
            SELECT {", ".join(COLUMNS)}
            FROM Source
            {where_sql}
            ORDER BY kind, name
        """
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(COLUMNS)
            for row in rows:
                w.writerow(row)

        logging.info("Exported %d rows to %s", len(rows), out_path)
    finally:
        conn.close()

def main():
    ap = argparse.ArgumentParser(description="Export Source table to CSV")
    ap.add_argument("--out", default="sources.csv", help="Output CSV path")
    ap.add_argument("--kind", help="Filter by kind (e.g., Blog, YouTube, RSS, Other)")
    ap.add_argument("--enabled", type=int, choices=[0,1], help="Filter by is_enabled (0/1)")
    ap.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(levelname)s: %(message)s")

    load_env(".env")
    try:
        export_sources(args.out, kind=args.kind, enabled=args.enabled)
    except Exception as e:
        logging.error("Export failed: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)

