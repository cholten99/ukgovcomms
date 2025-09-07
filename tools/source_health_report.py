#!/usr/bin/env python3
"""
Report data health for a Source (Blog):
- Shows the Source row
- Counts posts by host and by blog_name
- Lists blog_name variants present under the host
- Samples the latest 10 posts (date/title/url)

Usage examples:
  python3 tools/source_health_report.py --host educationhub.blog.gov.uk
  python3 tools/source_health_report.py --host prisonjobs.blog.gov.uk
  python3 tools/source_health_report.py --id 125
  python3 tools/source_health_report.py --url https://educationhub.blog.gov.uk/

Requires .env with DB_HOST, DB_NAME, DB_USER, DB_PASSWORD
"""

from __future__ import annotations
import os
import argparse
import pymysql
from typing import Optional

def load_env(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

def get_conn():
    host = os.environ.get("DB_HOST", "localhost")
    name = os.environ.get("DB_NAME", "UKGovComms")
    user = os.environ.get("DB_USER")
    pwd  = os.environ.get("DB_PASSWORD")
    if not (user and pwd):
        raise RuntimeError("DB_USER/DB_PASSWORD missing in environment (.env)")
    return pymysql.connect(host=host, user=user, password=pwd, database=name, charset="utf8mb4")

def host_from_url(url: str) -> str:
    return url.split("//", 1)[-1].split("/", 1)[0].lower()

def select_source(cur, sid: Optional[int], host: Optional[str], url: Optional[str]):
    q = "SELECT id,name,url,kind,is_enabled,last_success,total_posts FROM Source WHERE 1=1"
    params = []
    if sid is not None:
        q += " AND id=%s"
        params.append(sid)
    elif host:
        q += " AND SUBSTRING_INDEX(SUBSTRING_INDEX(url,'/',3),'/',-1)=%s"
        params.append(host.lower())
    elif url:
        q += " AND SUBSTRING_INDEX(SUBSTRING_INDEX(url,'/',3),'/',-1)=%s"
        params.append(host_from_url(url))
    else:
        raise ValueError("Provide one of: --id, --host, or --url")
    q += " LIMIT 1"
    cur.execute(q, params)
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Source not found with given selector.")
    return row  # (id,name,url,kind,is_enabled,last_success,total_posts)

def main():
    ap = argparse.ArgumentParser(description="Report data health for a Source (Blog).")
    ap.add_argument("--id", type=int)
    ap.add_argument("--host")
    ap.add_argument("--url")
    args = ap.parse_args()

    load_env(".env")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            sid, name, url, kind, enabled, last_success, total_posts = select_source(cur, args.id, args.host, args.url)
            host = host_from_url(url)

            print("== Source ==")
            print(f"id={sid}  name={name}  url={url}  kind={kind}  enabled={enabled}  last_success={last_success}  total_posts(summary)={total_posts}")
            print()

            # Counts by HOST (robust even if blog_name mismatched)
            cur.execute(
                """
                SELECT
                  COUNT(*)                                          AS total_by_host,
                  SUM(title IS NOT NULL AND TRIM(title) <> '')      AS titles_non_empty,
                  SUM(published_at IS NULL)                         AS missing_dates,
                  DATE(MIN(published_at))                           AS first_dt,
                  DATE(MAX(published_at))                           AS last_dt
                FROM BlogPost
                WHERE url LIKE CONCAT('https://', %s, '/%%')
                """,
                (host,)
            )
            th = cur.fetchone()
            print("== Counts by HOST ==")
            print(f"total_by_host={th[0]}  titles_non_empty={th[1]}  missing_dates={th[2]}  first_dt={th[3]}  last_dt={th[4]}")
            print()

            # Counts by blog_name (exact match to Source.name)
            cur.execute(
                """
                SELECT
                  COUNT(*)                                          AS total_by_name,
                  SUM(title IS NOT NULL AND TRIM(title) <> '')      AS titles_non_empty
                FROM BlogPost
                WHERE blog_name=%s
                """,
                (name,)
            )
            tn = cur.fetchone()
            print("== Counts by blog_name (exact match to Source.name) ==")
            print(f"total_by_name={tn[0]}  titles_non_empty={tn[1]}")
            print()

            # blog_name variants present under this host
            cur.execute(
                """
                SELECT COALESCE(blog_name,'(NULL)') AS blog_name, COUNT(*) AS c
                FROM BlogPost
                WHERE url LIKE CONCAT('https://', %s, '/%%')
                GROUP BY blog_name
                ORDER BY c DESC
                """,
                (host,)
            )
            rows = cur.fetchall()
            print("== blog_name variants under this host ==")
            for bn, c in rows:
                print(f"{bn:40}  {c}")
            print()

            # Sample latest 10 posts by host
            cur.execute(
                """
                SELECT DATE(published_at) AS dt, LEFT(COALESCE(title,''), 120) AS title, url
                FROM BlogPost
                WHERE url LIKE CONCAT('https://', %s, '/%%')
                ORDER BY published_at DESC
                LIMIT 10
                """,
                (host,)
            )
            latest = cur.fetchall()
            print("== Latest 10 by host ==")
            for dt, title, purl in latest:
                print(f"{dt}  {title}  {purl}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()

