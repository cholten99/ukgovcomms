#!/usr/bin/env python3
"""
Build a single ZIP export containing:
  - sources.csv
  - blog_posts.csv
  - youtube_videos.csv (only if table exists)

Outputs (default outdir=exports/):
  - ukgovcomms-data-latest.zip
  - ukgovcomms-data-YYYY-MM-DD_HHMM.zip
  - plus the raw CSVs alongside

Loads DB creds from .env (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD).
"""

import os
import sys
import csv
import zipfile
import logging
import argparse
from datetime import datetime
from contextlib import closing

# Load .env (non-fatal if missing)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import mysql.connector


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M")


def open_db_from_env():
    host = os.getenv("DB_HOST", "localhost")
    name = os.getenv("DB_NAME", "UKGovComms")
    user = os.getenv("DB_USER")
    pwd  = os.getenv("DB_PASSWORD")
    if not (user and pwd and name):
        raise RuntimeError("Missing DB env vars. Need DB_HOST, DB_NAME, DB_USER, DB_PASSWORD in .env")
    return mysql.connector.connect(
        host=host, database=name, user=user, password=pwd, autocommit=True
    )


def table_exists(conn, table: str) -> bool:
    try:
        with closing(conn.cursor()) as cur:
            cur.execute("SHOW TABLES LIKE %s", (table,))
            return cur.fetchone() is not None
    except Exception as e:
        logging.warning("Error checking table %s: %s", table, e)
        return False


def export_table_star(conn, table: str, out_csv: str) -> int:
    """
    Export SELECT * FROM `table` to CSV using server-reported column order.
    Returns number of rows written.
    """
    with closing(conn.cursor(dictionary=True)) as cur:
        cur.execute(f"SELECT * FROM `{table}`")
        rows = cur.fetchall()
        # Prefer cursor.column_names if available; fallback to keys from first row
        try:
            fieldnames = list(cur.column_names)  # type: ignore[attr-defined]
        except Exception:
            fieldnames = list(rows[0].keys()) if rows else []

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})
    logging.info("Wrote %s (%d rows)", out_csv, len(rows))
    return len(rows)


def zip_files(zip_path: str, members: list) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for m in members:
            z.write(m, arcname=os.path.basename(m))
    logging.info("Wrote ZIP %s", zip_path)


def clone_to_latest_and_timestamp(src_zip: str, latest_name: str, outdir: str) -> str:
    latest_path = os.path.join(outdir, latest_name)
    with open(src_zip, "rb") as r, open(latest_path, "wb") as w:
        w.write(r.read())
    os.utime(latest_path, None)  # bump mtime so it sorts first

    ts_name = latest_name.replace("-latest", f"-{ts()}")
    ts_path = os.path.join(outdir, ts_name)
    with open(src_zip, "rb") as r, open(ts_path, "wb") as w:
        w.write(r.read())
    return ts_path


def prune_old_archives(outdir: str, prefix: str, keep: int) -> None:
    if keep <= 0:
        return
    try:
        files = [
            f for f in os.listdir(outdir)
            if f.startswith(prefix) and f.endswith(".zip") and not f.endswith("-latest.zip")
        ]
        files.sort(key=lambda fn: os.path.getmtime(os.path.join(outdir, fn)), reverse=True)
        for f in files[keep:]:
            p = os.path.join(outdir, f)
            try:
                os.remove(p)
                logging.info("Pruned old archive %s", p)
            except Exception as e:
                logging.warning("Could not remove %s: %s", p, e)
    except FileNotFoundError:
        pass


def build_export(outdir: str, keep: int) -> None:
    ensure_dir(outdir)
    conn = open_db_from_env()

    members = []

    # Source (required)
    src_csv = os.path.join(outdir, "sources.csv")
    export_table_star(conn, "Source", src_csv)
    members.append(src_csv)

    # BlogPost (required)
    bp_csv = os.path.join(outdir, "blog_posts.csv")
    export_table_star(conn, "BlogPost", bp_csv)
    members.append(bp_csv)

    # YouTubeVideo (optional)
    yt_csv = os.path.join(outdir, "youtube_videos.csv")
    if table_exists(conn, "YouTubeVideo"):
        try:
            export_table_star(conn, "YouTubeVideo", yt_csv)
            members.append(yt_csv)
        except Exception as e:
            logging.warning("YouTube export failed, continuing without it: %s", e)
    else:
        logging.info("YouTubeVideo table not found; skipping youtube_videos.csv")

    # Build the single ZIP
    tmp_zip = os.path.join(outdir, "_ukgovcomms-data-tmp.zip")
    zip_files(tmp_zip, members)

    latest_name = "ukgovcomms-data-latest.zip"
    ts_path = clone_to_latest_and_timestamp(tmp_zip, latest_name, outdir)
    os.remove(tmp_zip)
    logging.info("Wrote %s and %s", latest_name, os.path.basename(ts_path))

    prune_old_archives(outdir, prefix="ukgovcomms-data-", keep=keep)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Build single UKGovComms data export ZIP")
    p.add_argument("--outdir", default="exports", help="Output directory (default: exports)")
    p.add_argument("--keep", type=int, default=5, help="How many timestamped archives to keep (default: 5)")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging level (default: INFO)")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level),
                        format="%(asctime)s %(levelname)s %(message)s")
    try:
        build_export(outdir=args.outdir, keep=args.keep)
    except Exception as e:
        logging.error("Build failed: %s", e)
        sys.exit(2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)

