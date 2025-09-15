#!/usr/bin/env python3
"""
Build a single ZIP export containing:
- sources.csv          (all columns from Source)
- blog_posts.csv       (essentials from BlogPost)
- youtube_videos.csv   (essentials from YouTubeVideo, if table exists)

Options:
  --outdir  : where to write the ZIPs (default: exports/)
  --keep    : keep only the N most recent timestamped ZIPs (default: 5)
  --latest  : also write/overwrite a stable ukgc_export_latest.zip

Usage:
  python3 tools/build_source_exports.py --outdir exports --keep 5 --latest
"""

import os
import io
import sys
import glob
import argparse
import zipfile
import datetime as dt

import pymysql
import pandas as pd


def get_conn_from_env():
    host = os.getenv("DB_HOST", "localhost")
    user = os.getenv("DB_USER")
    pwd  = os.getenv("DB_PASSWORD")
    db   = os.getenv("DB_NAME")
    port = int(os.getenv("DB_PORT", "3306"))
    if not (user and pwd and db):
        raise RuntimeError("Missing DB env vars. Need DB_HOST, DB_USER, DB_PASSWORD, DB_NAME.")
    return pymysql.connect(
        host=host, user=user, password=pwd, database=db, port=port,
        charset="utf8mb4", autocommit=True
    )


def table_exists(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES LIKE %s", (name,))
        return cur.fetchone() is not None


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def add_csv(zf: zipfile.ZipFile, name: str, df: pd.DataFrame):
    zf.writestr(name, df_to_csv_bytes(df))


def prune_old(outdir: str, keep: int):
    if keep is None or keep <= 0:
        return
    files = sorted(glob.glob(os.path.join(outdir, "ukgc_export_*.zip")))
    extra = len(files) - keep
    for f in files[:max(0, extra)]:
        try:
            os.remove(f)
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser(description="Build ZIP export for Source, BlogPost, and YouTubeVideo.")
    ap.add_argument("--outdir", default="exports", help="Output directory (default: exports)")
    ap.add_argument("--keep", type=int, default=5, help="Keep only the N most recent timestamped zips (default: 5)")
    ap.add_argument("--latest", action="store_true", help="Also write ukgc_export_latest.zip")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_path = os.path.join(args.outdir, f"ukgc_export_{stamp}.zip")
    latest_path = os.path.join(args.outdir, "ukgc_export_latest.zip")

    conn = get_conn_from_env()

    # 1) Source (all columns)
    df_sources = pd.read_sql("SELECT * FROM Source ORDER BY id", conn)

    # 2) BlogPost essentials (if table exists)
    if table_exists(conn, "BlogPost"):
        blog_sql = """
            SELECT id, blog_name, url, title, published_at
            FROM BlogPost
            ORDER BY published_at
        """
        df_blogs = pd.read_sql(blog_sql, conn)
    else:
        df_blogs = pd.DataFrame(columns=["id","blog_name","url","title","published_at"])

    # 3) YouTubeVideo essentials (if table exists)
    if table_exists(conn, "YouTubeVideo"):
        yt_sql = """
            SELECT id, source_id, video_id, title, published_at, privacy_status, discovered_via, playlist_id
            FROM YouTubeVideo
            ORDER BY published_at
        """
        df_yts = pd.read_sql(yt_sql, conn)
    else:
        df_yts = pd.DataFrame(columns=["id","source_id","video_id","title","published_at","privacy_status","discovered_via","playlist_id"])

    # Write ZIP
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        add_csv(zf, "sources.csv", df_sources)
        add_csv(zf, "blog_posts.csv", df_blogs)
        add_csv(zf, "youtube_videos.csv", df_yts)

        readme = f"""UKGovComms export

Generated: {dt.datetime.now().isoformat(timespec='seconds')}
Files included:
- sources.csv          : all rows/columns from Source
- blog_posts.csv       : id, blog_name, url, title, published_at
- youtube_videos.csv   : id, source_id, video_id, title, published_at, privacy_status, discovered_via, playlist_id

Notes:
- Timestamps are typically UTC.
- Additional kinds will be added over time as tables are introduced.
"""
        zf.writestr("README.txt", readme)

    print(f"Wrote: {zip_path}")

    if args.latest:
        with open(zip_path, "rb") as r, open(latest_path, "wb") as w:
            w.write(r.read())
        # bump mtime so your /downloads list sorts it to the top
        os.utime(latest_path, None)
        print(f"Also wrote: {latest_path}")

    prune_old(args.outdir, args.keep)


if __name__ == "__main__":
    main()

