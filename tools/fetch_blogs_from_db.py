#!/usr/bin/env python3
"""
Fetch GOV.UK blog posts for all sources listed in the Source table.

Behavior:
- Selects Source rows where kind='Blog' AND is_enabled=1 AND (last_checked is NULL OR DATE(last_checked)<CURDATE()).
- Unless --force is provided (then it ignores last_checked).
- For each blog: start at latest post, walk "previous" links until a known URL is found (incremental),
  or until no more previous posts (first run).
- Upserts into BlogPost (UNIQUE url).
- Updates Source.last_checked / last_success and summary (first_post_date, last_post_date, total_posts).
- Logs to a daily rotating file (keeps 5 days). No console output.

Usage:
  python3 tools/fetch_blogs_from_db.py --log-level INFO
  python3 tools/fetch_blogs_from_db.py --force --only-host gds.blog.gov.uk --max-posts-per-blog 500

Requirements:
  requests, beautifulsoup4, PyMySQL
"""

import os
import sys
import time
import re
import logging
import argparse
from logging.handlers import TimedRotatingFileHandler
from urllib.parse import urlparse, urljoin
from datetime import datetime
from typing import Optional, List, Tuple

import requests
from bs4 import BeautifulSoup
import pymysql

# ---------- Config ----------

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UKGovCommsFetcher/1.0)"}
POST_URL_REGEX = re.compile(r"^https://[a-z0-9\-]+\.blog\.gov\.uk/\d{4}/\d{2}/\d{2}/", re.IGNORECASE)

# ---------- Logging ----------

def setup_logging(log_dir: str, level: str) -> None:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # File-only logging, rotate nightly, keep 5 days
    fh = TimedRotatingFileHandler(
        filename=os.path.join(log_dir, "fetch_blogs.log"),
        when="midnight",
        backupCount=5,
        encoding="utf-8"
    )
    fh.setLevel(getattr(logging, level.upper(), logging.INFO))
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(fh)

# ---------- Env / DB ----------

def load_env(env_path: str = ".env") -> None:
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

def db_connect():
    host = os.environ.get("DB_HOST", "localhost")
    name = os.environ.get("DB_NAME", "UKGovComms")
    user = os.environ.get("DB_USER")
    pwd  = os.environ.get("DB_PASSWORD")
    if not (user and pwd):
        raise RuntimeError("DB_USER/DB_PASSWORD not set in .env")
    return pymysql.connect(host=host, user=user, password=pwd, database=name, charset="utf8mb4", autocommit=False)

# ---------- HTTP fetch with backoff ----------

def fetch(url: str, session: requests.Session, max_retries: int = 5, backoff_base: float = 1.7, timeout: int = 25) -> Tuple[str, Optional[int]]:
    """
    GET a URL with exponential backoff on 429 and 5xx responses.
    Returns (text, status_code).
    """
    last_status: Optional[int] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=timeout)
            last_status = resp.status_code
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                wait = backoff_base ** attempt
                logging.warning("Got %s from %s. Backing off %.1fs (attempt %d/%d).",
                                resp.status_code, url, wait, attempt, max_retries)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.text, last_status
        except requests.RequestException as e:
            if attempt == max_retries:
                logging.error("Failed to fetch %s after %d attempts: %s", url, attempt, e)
                raise
            wait = backoff_base ** attempt
            logging.warning("Error fetching %s: %s. Retrying in %.1fs (attempt %d/%d).",
                            url, e, wait, attempt, max_retries)
            time.sleep(wait)
    raise RuntimeError("Unreachable")

# ---------- Scraping helpers ----------

def find_latest_post_url(home_html: str) -> str:
    soup = BeautifulSoup(home_html, "html.parser")
    # Prefer obvious permalinks by regex
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if POST_URL_REGEX.match(href):
            return href
    # Fallback: first article h2 a
    a = soup.select_one("article h2 a[href]")
    if a and POST_URL_REGEX.match(a["href"]):
        return a["href"]
    raise ValueError("Could not locate latest post URL on homepage.")

def extract_title_and_date(post_html: str) -> Tuple[str, Optional[str]]:
    soup = BeautifulSoup(post_html, "html.parser")
    title: Optional[str] = None
    for sel in ["article h1", ".entry-title", "h1.entry-title", "h1"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            title = el.get_text(strip=True)
            break

    pub_date: Optional[str] = None
    t = soup.select_one("time[datetime]")
    if t and t.get("datetime"):
        pub_date = t["datetime"].strip()[:10]
    if not pub_date:
        meta = soup.find("meta", attrs={"property": "article:published_time"})
        if meta and meta.get("content"):
            pub_date = meta["content"].strip()[:10]
    if not pub_date and t and t.get_text(strip=True):
        m = re.search(r"\d{4}-\d{2}-\d{2}", t.get_text())
        if m:
            pub_date = m.group(0)

    return (title or ""), pub_date

def find_prev_next_urls(post_html: str, current_url: str) -> Tuple[Optional[str], Optional[str]]:
    soup = BeautifulSoup(post_html, "html.parser")
    prev_url: Optional[str] = None
    next_url: Optional[str] = None

    rel_prev = soup.find("a", rel=lambda v: v and ("prev" in v or "previous" in v))
    if rel_prev and rel_prev.get("href"):
        prev_url = urljoin(current_url, rel_prev["href"].strip())
    rel_next = soup.find("a", rel=lambda v: v and "next" in v)
    if rel_next and rel_next.get("href"):
        next_url = urljoin(current_url, rel_next["href"].strip())

    if not prev_url:
        for a in soup.find_all("a", href=True):
            txt = (a.get_text() or "").strip()
            if txt.startswith("←") or txt.startswith("\u2190"):
                prev_url = urljoin(current_url, a["href"].strip()); break
    if not next_url:
        for a in soup.find_all("a", href=True):
            txt = (a.get_text() or "").strip()
            if txt.endswith("→") or txt.endswith("\u2192"):
                next_url = urljoin(current_url, a["href"].strip()); break

    if not prev_url:
        cand = soup.select_one(".nav-previous a, .previous a, a.previous, a.nav-previous")
        if cand and cand.get("href"):
            prev_url = urljoin(current_url, cand["href"].strip())
    if not next_url:
        cand = soup.select_one(".nav-next a, .next a, a.next, a.nav-next")
        if cand and cand.get("href"):
            next_url = urljoin(current_url, cand["href"].strip())

    # Sanity: accept only canonical post permalinks
    if prev_url and not POST_URL_REGEX.match(prev_url): prev_url = None
    if next_url and not POST_URL_REGEX.match(next_url): next_url = None
    return prev_url, next_url

# ---------- DB helpers ----------

def host_from_url(url: str) -> str:
    return (urlparse(url).hostname or "").lower()

def blogpost_exists(conn, url: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM BlogPost WHERE url=%s LIMIT 1", (url,))
        return cur.fetchone() is not None

def upsert_blogpost(conn, blog_name: str, url: str, title: str,
                    published_at: Optional[str], previous_url: Optional[str], next_url: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO BlogPost (blog_name, url, title, published_at, previous_url, next_url)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              blog_name=VALUES(blog_name),
              title=VALUES(title),
              published_at=VALUES(published_at),
              previous_url=VALUES(previous_url),
              next_url=VALUES(next_url),
              updated_at=CURRENT_TIMESTAMP
        """, (blog_name, url, title, published_at, previous_url, next_url))
    conn.commit()

def update_source_check(conn, source_id: int, ok: bool, status_code: Optional[int]) -> None:
    with conn.cursor() as cur:
        if ok:
            cur.execute("""
                UPDATE Source SET last_checked=NOW(), last_success=NOW(), status_code=%s
                WHERE id=%s
            """, (status_code or 200, source_id))
        else:
            cur.execute("""
                UPDATE Source SET last_checked=NOW(), status_code=%s
                WHERE id=%s
            """, (status_code or 0, source_id))
    conn.commit()

def update_source_summary_for_host(conn, source_id: int, host: str) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT MIN(published_at), MAX(published_at), COUNT(*)
            FROM BlogPost
            WHERE url LIKE CONCAT('https://', %s, '/%%')
        """, (host,))
        first_date, last_date, total = cur.fetchone()
        cur.execute("""
            UPDATE Source
            SET first_post_date=%s, last_post_date=%s, total_posts=%s, updated_at=NOW()
            WHERE id=%s
        """, (first_date, last_date, total or 0, source_id))
    conn.commit()

def select_sources(conn, force: bool, only_hosts: Optional[List[str]]):
    where = ["kind='Blog'", "is_enabled=1"]
    params: List[str] = []
    if not force:
        where.append("(last_checked IS NULL OR DATE(last_checked) < CURDATE())")
    if only_hosts:
        where.append("SUBSTRING_INDEX(SUBSTRING_INDEX(url,'/',3),'/',-1) IN (" + ",".join(["%s"]*len(only_hosts)) + ")")
        params.extend([h.lower() for h in only_hosts])
    sql = f"""
        SELECT id, name, url
        FROM Source
        WHERE {' AND '.join(where)}
        ORDER BY name
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()

# ---------- Core crawl for one blog ----------

def crawl_blog_incremental(conn, session: requests.Session, blog_name: str, base_url: str,
                           max_posts_per_blog: int, sleep_s: float) -> Tuple[int, int]:
    """
    Returns (new_posts, http_status_of_homepage)
    """
    if not base_url.endswith("/"):
        base_url += "/"

    last_status: Optional[int] = None
    home_html, last_status = fetch(base_url, session)
    latest_url = find_latest_post_url(home_html)

    new_posts = 0
    current = latest_url
    seen_loop_guard = set()

    while current and current not in seen_loop_guard and new_posts < max_posts_per_blog:
        seen_loop_guard.add(current)

        # Stop if we've already got this post (incremental finish)
        if blogpost_exists(conn, current):
            logging.info("Encountered existing post; stopping at %s", current)
            break

        post_html, status = fetch(current, session)
        last_status = status or last_status

        title, published_at = extract_title_and_date(post_html)
        prev_url, next_url = find_prev_next_urls(post_html, current)

        upsert_blogpost(conn, blog_name, current, title, published_at, prev_url, next_url)
        new_posts += 1
        logging.info("Added: %s | %s", published_at or "Unknown date", title or current)

        if not prev_url:
            logging.info("No previous link; reached start of archive.")
            break

        current = prev_url
        time.sleep(sleep_s)

    return new_posts, (last_status or 200)

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser(description="Fetch GOV.UK blog posts for all un-checked sources.")
    ap.add_argument("--force", action="store_true",
                    help="Ignore last_checked and process all enabled blogs.")
    ap.add_argument("--only-host", action="append",
                    help="Limit to one or more hosts (e.g. --only-host gds.blog.gov.uk). Can repeat.")
    ap.add_argument("--max-posts-per-blog", type=int, default=100000,
                    help="Safety limit per blog (default 100000).")
    ap.add_argument("--sleep", type=float, default=0.8,
                    help="Seconds to sleep between requests (default 0.8).")
    ap.add_argument("--log-dir", default="logs",
                    help="Directory for rotating logs (default logs).")
    ap.add_argument("--log-level", default="INFO",
                    help="File log level (DEBUG, INFO, WARNING, ERROR).")
    args = ap.parse_args()

    setup_logging(args.log_dir, args.log_level)
    load_env(".env")

    try:
        conn = db_connect()
    except Exception as e:
        logging.error("DB connection failed: %s", e)
        sys.exit(2)

    session = requests.Session()

    try:
        sources = select_sources(conn, force=args.force, only_hosts=args.only_host)
        if not sources:
            logging.info("No sources to process (maybe all checked today). Use --force to override.")
            conn.close()
            return

        logging.info("Processing %d sources ...", len(sources))
        for source_id, name, url in sources:
            host = host_from_url(url)
            logging.info("Source #%s | %s (%s)", source_id, name, host)
            ok = False
            status_code: Optional[int] = None
            try:
                new_posts, status_code = crawl_blog_incremental(
                    conn, session, name, url, args.max_posts_per_blog, args.sleep
                )
                ok = True
                logging.info("Result: %d new posts", new_posts)
                # Refresh summary fields for this source
                update_source_summary_for_host(conn, source_id, host)
            except Exception as e:
                logging.error("Failed for %s: %s", host, e)
                ok = False
            finally:
                update_source_check(conn, source_id, ok=ok, status_code=status_code)

        logging.info("All done.")
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # No console output; ensure file log has a record
        logging.error("Interrupted by user (KeyboardInterrupt).")
        sys.exit(130)
    except Exception as e:
        logging.error("Fatal error: %s", e)
        sys.exit(1)

