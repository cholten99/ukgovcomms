#!/usr/bin/env python3
"""
Scrape https://www.blog.gov.uk/ for the directory of GOV.UK blogs,
write a CSV of Name,URL, and (optionally) insert/upsert into Source.

Usage:
  python3 tools/export_govuk_blogs.py --out sources_blogs.csv
  python3 tools/export_govuk_blogs.py --out sources_blogs.csv --write-db
"""

import os
import csv
import sys
import logging
import argparse
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# Optional DB
try:
    import pymysql  # noqa: F401
    HAVE_DB = True
except Exception:
    HAVE_DB = False

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UKGovCommsScraper/1.0)"}
INDEX_URL = "https://www.blog.gov.uk/"


def load_env(env_path=".env"):
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def fetch_html(url):
    r = requests.get(url, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return r.text


def looks_like_blog_home(url: str) -> bool:
    """
    Keep only *.blog.gov.uk roots (no post paths).
    """
    try:
        p = urlparse(url)
        if not p.scheme.startswith("http"):
            return False
        if not p.hostname or not p.hostname.endswith(".blog.gov.uk"):
            return False
        # accept only bare homes like https://gds.blog.gov.uk/ (no /YYYY/MM/DD/)
        return p.path in ("", "/")
    except Exception:
        return False


def extract_blogs(html):
    """
    Parse the index page and return a list of (name, url) unique pairs.
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen = set()

    # The directory section lists each blog as a heading link
    # We'll pick anchors whose host endswith ".blog.gov.uk" and path is "/" or ""
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = (a.get_text() or "").strip()
        if not text or not href:
            continue
        if looks_like_blog_home(href):
            key = (text, href.rstrip("/") + "/")
            if key not in seen:
                seen.add(key)
                items.append((text, key[1]))

    # Sort by name for stability
    items.sort(key=lambda t: t[0].lower())
    return items


def write_csv(rows, out_path):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["name", "url", "kind", "is_enabled"])
        for name, url in rows:
            w.writerow([name, url, "Blog", 1])


def upsert_into_db(rows):
    host = os.environ.get("DB_HOST", "localhost")
    name = os.environ.get("DB_NAME", "UKGovComms")
    user = os.environ.get("DB_USER")
    pwd = os.environ.get("DB_PASSWORD")
    if not (user and pwd):
        raise RuntimeError("DB_USER/DB_PASSWORD not set in env/.env")

    import pymysql
    conn = pymysql.connect(host=host, user=user, password=pwd, database=name, charset="utf8mb4")
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO Source (name, url, kind, is_enabled)
                VALUES (%s, %s, 'Blog', 1)
                ON DUPLICATE KEY UPDATE
                  name = VALUES(name),
                  kind = VALUES(kind),
                  is_enabled = VALUES(is_enabled),
                  updated_at = CURRENT_TIMESTAMP
            """
            cur.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()


def main():
    ap = argparse.ArgumentParser(description="Export the GOV.UK blogs directory to CSV (and optionally DB).")
    ap.add_argument("--out", default="sources_blogs.csv", help="Output CSV path")
    ap.add_argument("--write-db", action="store_true", help="Also upsert rows into Source")
    ap.add_argument("--log-level", default="INFO", help="DEBUG, INFO, WARNING, ERROR")
    args = ap.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(levelname)s: %(message)s")

    if args.write_db and not HAVE_DB:
        logging.error("PyMySQL not installed; run without --write-db or install it.")
        sys.exit(2)

    load_env(".env")

    try:
        html = fetch_html(INDEX_URL)
        rows = extract_blogs(html)
        if not rows:
            logging.error("No blogs found on the directory page.")
            sys.exit(1)
        logging.info("Found %d blogs.", len(rows))
        write_csv(rows, args.out)
        logging.info("Wrote %s", args.out)

        if args.write_db:
            upsert_into_db(rows)
            logging.info("Upserted %d rows into Source.", len(rows))
    except Exception as e:
        logging.error("Failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)

