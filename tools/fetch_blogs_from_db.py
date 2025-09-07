#!/usr/bin/env python3
"""
Crawl enabled Blog sources from the DB and upsert posts into BlogPost.

Features
- Skips sources already checked today unless --force or --only-host is used
- Starts from latest post auto-discovered from homepage
  * If homepage parsing fails, falls back to feed (/feed/ or <link rel="alternate">)
  * Then falls back to WordPress JSON (/wp-json/wp/v2/posts?per_page=1)
- NEW: --start-url lets you override the starting post URL for a target host
- Follows "previous" links back in time until it reaches already-stored posts
- Upserts each post (by unique URL) and updates Source summaries

Usage
  python3 tools/fetch_blogs_from_db.py --log-level INFO
  python3 tools/fetch_blogs_from_db.py --only-host gds.blog.gov.uk --force --log-level DEBUG
  python3 tools/fetch_blogs_from_db.py --only-host nda.blog.gov.uk --start-url https://nda.blog.gov.uk/2025/07/25/some-post/

Requires
  - .env with DB_HOST, DB_NAME, DB_USER, DB_PASSWORD
  - Tables: Source(kind='Blog'), BlogPost with at least:
      url (UNIQUE), title, blog_name, published_at DATETIME NULL,
      previous_url, next_url, created_at, updated_at
"""

from __future__ import annotations

import os
import re
import sys
import time
import argparse
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, date, timedelta
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import pymysql
import xml.etree.ElementTree as ET


# ------------- Config -------------

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "fetch_blogs.log")
USER_AGENT = "ukgovcomms-bot/1.0 (+contact: admin@localhost)"

REQUEST_TIMEOUT = 20
BACKOFF_SLEEP = 5           # seconds for 429/5xx
MAX_RETRIES = 3

DEFAULT_SLEEP_BETWEEN = 0.5  # polite pause between HTTP requests


# ------------- Utils -------------

def load_env(path: str = ".env"):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if "=" in line and not line.strip().startswith("#"):
                k, v = line.strip().split("=", 1)
                os.environ.setdefault(k, v)


def setup_logging(level: str):
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # File-only logging, rotate nightly, keep 5 days
    fh = TimedRotatingFileHandler(LOG_FILE, when="midnight", backupCount=5, encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)


def get_db():
    host = os.environ.get("DB_HOST", "localhost")
    name = os.environ.get("DB_NAME", "UKGovComms")
    user = os.environ.get("DB_USER")
    pwd  = os.environ.get("DB_PASSWORD")
    if not (user and pwd):
        raise RuntimeError("DB_USER/DB_PASSWORD missing in environment (.env)")
    return pymysql.connect(host=host, user=user, password=pwd, database=name, charset="utf8mb4", autocommit=False)


def http_get(session: requests.Session, url: str) -> requests.Response:
    """GET with basic retry/backoff on 429/5xx."""
    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code in (429, 500, 502, 503, 504):
            logging.warning("HTTP %s on %s (attempt %d/%d); backing off %ss",
                            resp.status_code, url, attempt, MAX_RETRIES, BACKOFF_SLEEP)
            time.sleep(BACKOFF_SLEEP)
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()  # will raise the last status error


def normalize_url(base_url: str, href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("#"):
        return None
    if href.startswith(("http://", "https://")):
        return href
    return urljoin(base_url, href)


def host_from_url(url: str) -> str:
    return url.split("//", 1)[-1].split("/", 1)[0].lower()


# ------------- Feed fallback helpers -------------

def discover_feed_url(home_html: str, home_url: str) -> str:
    """Find an RSS/Atom link in <head>; else guess /feed/ (WordPress default)."""
    try:
        soup = BeautifulSoup(home_html, "html.parser")
        for link in soup.find_all("link", rel=lambda v: v and "alternate" in v):
            t = (link.get("type") or "").lower()
            if any(x in t for x in ("rss", "atom", "xml")):
                href = link.get("href")
                if href:
                    u = normalize_url(home_url, href)
                    if u:
                        return u
    except Exception:
        pass
    return urljoin(home_url, "/feed/")


def feed_latest_entry_link(feed_xml: str) -> Optional[str]:
    """Return newest item link from an RSS or Atom feed (very tolerant)."""
    try:
        root = ET.fromstring(feed_xml)

        # RSS 2.0
        channel = root.find("./channel")
        if channel is not None:
            item = channel.find("./item")
            if item is not None:
                link = item.findtext("link")
                if link and link.strip():
                    return link.strip()

        # Atom
        for entry in root.iter():
            if entry.tag.endswith("entry"):
                for child in entry:
                    if child.tag.endswith("link"):
                        href = child.attrib.get("href")
                        rel = child.attrib.get("rel", "alternate")
                        if href and (rel == "alternate" or not rel):
                            return href
                break
    except Exception:
        pass
    return None


# ------------- Post parsing -------------

TITLE_SELECTORS = [
    "h1.entry-title",
    "article h1",
    "header h1",
    "h1",
]

DATE_META_SELECTORS = [
    # OpenGraph / schema
    ("meta[property='article:published_time']", "content"),
    ("meta[name='pubdate']", "content"),
    ("time[datetime]", "datetime"),
]

PREV_LINK_SELECTORS = [
    "a[rel='prev']",
    "nav.post-navigation a[rel='prev']",
    ".post-navigation .nav-previous a",
    "a.previous-post",
    "a.prev-post",
]

NEXT_LINK_SELECTORS = [
    "a[rel='next']",
    "nav.post-navigation a[rel='next']",
    ".post-navigation .nav-next a",
    "a.next-post",
]


def extract_title(soup: BeautifulSoup) -> Optional[str]:
    for sel in TITLE_SELECTORS:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            return el.get_text(strip=True)
    # Fallback to <title>
    t = soup.title.string if soup.title else None
    return t.strip() if t else None


def parse_date_str(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    # Try common ISO-like formats first
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S%zZ", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    # Loose parse: 2025-08-04T12:34:56Z -> TZ naive
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d")
        except Exception:
            pass
    return None


def extract_published_at(soup: BeautifulSoup) -> Optional[datetime]:
    # Prefer meta tags
    for sel, attr in DATE_META_SELECTORS:
        el = soup.select_one(sel)
        if el and el.get(attr):
            dt = parse_date_str(el.get(attr))
            if dt:
                return dt
    # Try <time datetime="">
    for t in soup.find_all("time"):
        dt = parse_date_str(t.get("datetime") or t.get_text())
        if dt:
            return dt
    return None


def extract_prev_next(soup: BeautifulSoup, page_html: str) -> Tuple[Optional[str], Optional[str]]:
    # First try conventional selectors
    prev = next_ = None
    for sel in PREV_LINK_SELECTORS:
        a = soup.select_one(sel)
        if a and a.get("href"):
            prev = a.get("href"); break
    for sel in NEXT_LINK_SELECTORS:
        a = soup.select_one(sel)
        if a and a.get("href"):
            next_ = a.get("href"); break

    # Heuristic: look for links labelled "Previous"/"Next" near "share this page"
    if not prev or not next_:
        try:
            # Find "share this page" text block and scan nearby anchors
            share_idx = page_html.lower().find("share this page")
            if share_idx != -1:
                window = page_html[max(0, share_idx - 2000): share_idx + 1000]
                s2 = BeautifulSoup(window, "html.parser")
                anchors = s2.find_all("a", href=True)
                # try to infer left/right by arrow glyphs or text
                for a in anchors:
                    txt = (a.get_text() or "").strip().lower()
                    if not prev and ("previous" in txt or "older" in txt or "←" in txt):
                        prev = a.get("href")
                    if not next_ and ("next" in txt or "newer" in txt or "→" in txt):
                        next_ = a.get("href")
                    if prev and next_:
                        break
        except Exception:
            pass

    return prev, next_


# ------------- Latest post discovery -------------

def latest_post_from_home(session: requests.Session, home_url: str) -> Optional[str]:
    resp = http_get(session, home_url)
    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    # Common listing anchors (prioritised)
    candidates = []
    candidates += [a.get("href") for a in soup.select("h2.entry-title a[href]")]
    candidates += [a.get("href") for a in soup.select("article header h2 a[href]")]
    candidates += [a.get("href") for a in soup.select("main a[href]")]
    for href in candidates:
        u = normalize_url(home_url, href)
        if u:
            return u

    # Fallbacks: feed then WP JSON
    feed_url = discover_feed_url(html, home_url)
    try:
        f = http_get(session, feed_url)
        if f.ok and ("xml" in f.headers.get("Content-Type", "").lower() or f.text.lstrip().startswith("<")):
            u = feed_latest_entry_link(f.text)
            if u:
                return u
    except Exception as e:
        logging.debug("Feed fallback failed for %s: %s", home_url, e)

    try:
        api = urljoin(home_url, "/wp-json/wp/v2/posts?per_page=1&_fields=link,date")
        j = http_get(session, api)
        data = j.json()
        if isinstance(data, list) and data and data[0].get("link"):
            return data[0]["link"]
    except Exception as e:
        logging.debug("WP JSON fallback failed for %s: %s", home_url, e)

    return None


# ------------- DB ops -------------

def list_sources(conn, only_host: Optional[str], force: bool):
    q = """
      SELECT id, name, url, kind, is_enabled, last_checked, last_success
      FROM Source
      WHERE kind='Blog' AND is_enabled=1
    """
    params = []
    if only_host:
        q += " AND SUBSTRING_INDEX(SUBSTRING_INDEX(url,'/',3),'/',-1)=%s"
        params.append(only_host.lower())
    if not force and not only_host:
        q += " AND (last_checked IS NULL OR DATE(last_checked) < CURDATE())"
    q += " ORDER BY id"
    with conn.cursor() as cur:
        cur.execute(q, params)
        return cur.fetchall()


def upsert_post(conn, blog_name: str, url: str, title: Optional[str],
                published_at: Optional[datetime], prev_url: Optional[str], next_url: Optional[str]):
    sql = """
      INSERT INTO BlogPost (blog_name, url, title, published_at, previous_url, next_url, created_at, updated_at)
      VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
      ON DUPLICATE KEY UPDATE
        title=VALUES(title),
        published_at=VALUES(published_at),
        previous_url=VALUES(previous_url),
        next_url=VALUES(next_url),
        updated_at=NOW()
    """
    with conn.cursor() as cur:
        cur.execute(sql, (blog_name, url, title, published_at, prev_url, next_url))


def refresh_source_summary(conn, source_id: int, home_host: str):
    sql = """
      UPDATE Source s
      JOIN (
        SELECT
          DATE(MIN(published_at)) AS first_date,
          DATE(MAX(published_at)) AS last_date,
          COUNT(*) AS total
        FROM BlogPost
        WHERE url LIKE CONCAT('https://', %s, '/%%')
      ) b ON 1=1
      SET s.first_post_date = b.first_date,
          s.last_post_date  = b.last_date,
          s.total_posts     = b.total,
          s.updated_at      = NOW()
      WHERE s.id=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (home_host, source_id))


def mark_source_checked(conn, source_id: int, success: bool, status_code: int = 0):
    q = """
      UPDATE Source
      SET last_checked = NOW(),
          {success_field} = NOW(),
          status_code = %s
      WHERE id = %s
    """.format(success_field="last_success" if success else "last_checked")  # last_success only on success
    with conn.cursor() as cur:
        cur.execute(q, (status_code, source_id))


# ------------- Crawl logic -------------

def crawl_blog(session: requests.Session, conn, source_row, args) -> Tuple[int, Optional[str]]:
    """
    Returns (new_posts_count, error_message).
    """
    source_id, name, url, kind, is_enabled, last_checked, last_success = source_row
    home_url = url if url.endswith("/") else url + "/"
    host = host_from_url(home_url)
    logging.info("Source #%s | %s (%s)", source_id, name, host)

    try:
        # Decide starting post
        if args.only_host and args.start_url and host == args.only_host:
            start_url = args.start_url
        elif args.start_url and not args.only_host:
            # If user provided start-url without only-host, use it for any single source run
            start_url = args.start_url
        else:
            start_url = latest_post_from_home(session, home_url)

        if not start_url:
            raise RuntimeError("Could not locate latest post URL on homepage.")

        # Walk back via previous links
        seen = 0
        this_url = start_url
        visited = set()
        while this_url:
            if this_url in visited:
                break
            visited.add(this_url)

            resp = http_get(session, this_url)
            html = resp.text
            soup = BeautifulSoup(html, "html.parser")

            title = extract_title(soup)
            published_at = extract_published_at(soup)
            prev_href, next_href = extract_prev_next(soup, html)

            prev_abs = normalize_url(this_url, prev_href) if prev_href else None
            next_abs = normalize_url(this_url, next_href) if next_href else None

            # Upsert this post
            try:
                upsert_post(conn, name, this_url, title, published_at, prev_abs, next_abs)
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise

            seen += 1
            if args.max_posts_per_blog and seen >= args.max_posts_per_blog:
                logging.info("Reached max_posts_per_blog=%d for %s", args.max_posts_per_blog, host)
                break

            # Stop early if we've already seen prev_abs in DB (incremental)
            if prev_abs:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM BlogPost WHERE url=%s LIMIT 1", (prev_abs,))
                    exists = cur.fetchone() is not None
                if exists:
                    logging.info("Hit already-known post; stopping at %s", prev_abs)
                    break

            # Move to previous
            this_url = prev_abs

            if args.sleep > 0:
                time.sleep(args.sleep)

        # Refresh summary and mark success
        refresh_source_summary(conn, source_id, host)
        conn.commit()
        mark_source_checked(conn, source_id, success=True, status_code=0)
        conn.commit()
        logging.info("Success for %s; new/updated posts this run: %d", host, seen)
        return seen, None

    except Exception as e:
        logging.error("Failed for %s: %s", host, e)
        try:
            mark_source_checked(conn, source_id, success=False, status_code=getattr(e, "status_code", 1))
            conn.commit()
        except Exception:
            conn.rollback()
        return 0, str(e)


# ------------- Main -------------

def main():
    ap = argparse.ArgumentParser(description="Crawl enabled Blog sources and upsert posts.")
    ap.add_argument("--force", action="store_true", help="Ignore 'checked today' and crawl anyway")
    ap.add_argument("--only-host", help="Limit to a specific host (e.g., gds.blog.gov.uk)")
    ap.add_argument("--start-url", help="Override: start crawl from this post URL (use with --only-host)")
    ap.add_argument("--max-posts-per-blog", type=int, default=0, help="Ceiling for posts fetched per blog (0=unlimited)")
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_BETWEEN, help="Seconds to sleep between requests")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    load_env(".env")
    setup_logging(args.log_level)

    # HTTP session
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    conn = get_db()
    try:
        rows = list_sources(conn, args.only_host, args.force)
        logging.info("Processing %d sources ...", len(rows))
        total_new = 0
        for row in rows:
            new_count, err = crawl_blog(session, conn, row, args)
            total_new += new_count
        logging.info("All done. New/updated posts total: %d", total_new)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

