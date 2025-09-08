#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crawl enabled GOV.UK blogs from the DB and store posts in BlogPost.

Usage examples:
  python3 tools/fetch_blogs_from_db.py --log-level INFO
  python3 tools/fetch_blogs_from_db.py --only-host systemsthinking.blog.gov.uk --log-level DEBUG
  python3 tools/fetch_blogs_from_db.py --only-host nda.blog.gov.uk --start-url "https://nda.blog.gov.uk/...." --force

What it does:
  - Picks Sources where kind='Blog' AND is_enabled=1 (optionally only a specific host)
  - Determines a start URL (from --start-url or the Atom feed’s latest entry)
  - Visits the post page, parses title + published date, saves to BlogPost
  - Finds the OLDER post by comparing link dates (handles “older on the right”)
  - If nav is missing/ambiguous, falls back to the Atom/RSS /feed/ (and /feed/?paged=N)
  - Stops when no older post is found or a visited loop is detected
  - Updates Source summary fields at the end

DB requirements:
  - Table Source(name, url, kind, is_enabled, last_success, first_post_date, last_post_date, total_posts, ...)
  - Table BlogPost(blog_name, url, title, published_at, ...)
"""

import os
import re
import sys
import time
import argparse
import logging
import datetime as dt
from urllib.parse import urljoin, urlparse

import requests
import pymysql
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import xml.etree.ElementTree as ET

# ---------- Config / CLI ----------

DATE_URL_RE = re.compile(r'/(\d{4})/(\d{2})/(\d{2})/')
UA = "ukgovcomms-crawler/1.0 (+https://github.com/cholten99/ukgovcomms)"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})
TIMEOUT = 25

log = logging.getLogger("crawl")


def parse_args():
    p = argparse.ArgumentParser(description="Fetch blog posts into DB.")
    p.add_argument("--only-host", help="Only crawl the Source whose host matches this (e.g. systemsthinking.blog.gov.uk)")
    p.add_argument("--start-url", help="Seed URL (single blog post) to start walking backwards from")
    p.add_argument("--sleep", type=float, default=0.8, help="Seconds to sleep between HTTP requests")
    p.add_argument("--force", action="store_true", help="Ignore last_success and crawl anyway")
    p.add_argument("--max-posts", type=int, default=None, help="Stop after this many posts per source")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


# ---------- Env / DB helpers ----------

def env(name, default=None):
    v = os.getenv(name, default)
    if v is None:
        log.warning("Missing env %s", name)
    return v

def get_db():
    return pymysql.connect(
        host=env("DB_HOST", "localhost"),
        user=env("DB_USER"),
        password=env("DB_PASSWORD"),
        database=env("DB_NAME"),
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

def host_of(url: str) -> str:
    p = urlparse(url)
    return (p.netloc or "").lower()

def base_of(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


# ---------- Date parsing ----------

def date_from_url(u: str):
    m = DATE_URL_RE.search(u or "")
    if not m:
        return None
    y, mth, d = map(int, m.groups())
    try:
        return dt.date(y, mth, d)
    except ValueError:
        return None

def date_from_page(url: str):
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        s = BeautifulSoup(r.text, "html.parser")

        # <time datetime="...">
        t = s.select_one("time[datetime]")
        if t and t.get("datetime"):
            try:
                return dt.datetime.fromisoformat(t["datetime"].replace("Z", "+00:00")).date()
            except Exception:
                pass

        # OpenGraph / schema-ish
        for sel, attr in [
            ('meta[property="article:published_time"]', 'content'),
            ('meta[name="pubdate"]', 'content'),
            ('meta[itemprop="datePublished"]', 'content'),
        ]:
            m = s.select_one(sel)
            if m and m.get(attr):
                try:
                    return dt.datetime.fromisoformat(m[attr].replace("Z", "+00:00")).date()
                except Exception:
                    pass
    except Exception:
        return None
    return None

def safe_post_date(url: str):
    return date_from_url(url) or date_from_page(url)


def current_page_date(soup: BeautifulSoup, url: str):
    # Prefer page content
    t = soup.select_one("time[datetime]")
    if t and t.get("datetime"):
        try:
            return dt.datetime.fromisoformat(t["datetime"].replace("Z", "+00:00")).date()
        except Exception:
            pass
    # fallbacks
    for sel, attr in [
        ('meta[property="article:published_time"]', 'content'),
        ('meta[name="pubdate"]', 'content'),
        ('meta[itemprop="datePublished"]', 'content'),
    ]:
        m = soup.select_one(sel)
        if m and m.get(attr):
            try:
                return dt.datetime.fromisoformat(m[attr].replace("Z", "+00:00")).date()
            except Exception:
                pass
    return date_from_url(url)


# ---------- Navigation / feed helpers ----------

def nav_candidates(soup: BeautifulSoup, page_url: str):
    seen = set()
    out = []

    sels = [
        ".post-navigation .nav-previous a",
        ".post-navigation .nav-next a",
        ".nav-links .nav-previous a",
        ".nav-links .nav-next a",
        'a[rel="prev"]',
        'a[rel="next"]',
        "a",  # final catch-all: filtered by text/arrows
    ]
    for sel in sels:
        for a in soup.select(sel):
            href = a.get("href")
            if not href:
                continue
            href = urljoin(page_url, href)
            if href in seen:
                continue
            if sel == "a":
                txt = (a.get_text() or "").strip().lower()
                if not any(k in txt for k in ["previous", "older", "next", "newer", "«", "»", "←", "→"]):
                    continue
            seen.add(href)
            out.append(href)
    return out

def find_older_link(soup: BeautifulSoup, page_url: str):
    """Pick the strictly older post link regardless of visual left/right."""
    curr = current_page_date(soup, page_url)
    cands = nav_candidates(soup, page_url)
    if not cands:
        return None

    dated = [(u, safe_post_date(u)) for u in cands]
    dated_with = [(u, d) for (u, d) in dated if d]

    if curr and dated_with:
        older = [(u, d) for (u, d) in dated_with if d < curr]
        if older:
            # closest earlier first
            older.sort(key=lambda x: x[1], reverse=True)
            return older[0][0]
        return None

    if dated_with:
        # no current date; guess by earliest
        dated_with.sort(key=lambda x: x[1])
        return dated_with[0][0]

    if len(cands) == 1:
        return cands[0]

    return None


def feed_latest_post(base_url: str):
    """Return latest post URL from Atom/RSS feed."""
    feed_url = base_url.rstrip("/") + "/feed/"
    try:
        r = SESSION.get(feed_url, timeout=TIMEOUT)
        r.raise_for_status()
        root = ET.fromstring(r.content)
    except Exception as e:
        log.debug("Feed latest error for %s: %s", base_url, e)
        return None

    ns = {'a': 'http://www.w3.org/2005/Atom'}
    e = root.find('a:entry', ns)
    if e is not None:
        link = e.find("a:link[@rel='alternate']", ns) or e.find('a:link', ns)
        if link is not None and link.get('href'):
            return link.get('href')

    channel = root.find('channel')
    if channel is not None:
        it = channel.find('item')
        if it is not None:
            l = it.find('link')
            if l is not None and l.text:
                return l.text.strip()

    return None


def feed_prev_post(base_url: str, current_date: dt.date, max_pages=20):
    """
    Find the entry immediately OLDER than current_date from /feed/ and /feed/?paged=N.
    """
    def pages():
        yield base_url.rstrip("/") + "/feed/"
        for p in range(2, max_pages + 1):
            yield base_url.rstrip("/") + f"/feed/?paged={p}"

    ns = {'a': 'http://www.w3.org/2005/Atom'}
    for feed_url in pages():
        try:
            r = SESSION.get(feed_url, timeout=TIMEOUT)
            if r.status_code >= 400:
                if r.status_code in (404, 410):
                    break
                continue
            root = ET.fromstring(r.content)
        except Exception:
            continue

        entries = root.findall('a:entry', ns)
        items = []
        if entries:
            for e in entries:
                link_el = e.find("a:link[@rel='alternate']", ns) or e.find('a:link', ns)
                href = link_el.get('href') if link_el is not None else None
                upd = e.find('a:updated', ns) or e.find('a:published', ns)
                d = None
                if upd is not None and upd.text:
                    try:
                        d = dt.datetime.fromisoformat(upd.text.replace("Z", "+00:00")).date()
                    except Exception:
                        d = None
                if href:
                    items.append((href, d))
        else:
            channel = root.find('channel')
            if channel is not None:
                for it in channel.findall('item'):
                    l = it.find('link')
                    pd = it.find('pubDate')
                    href = l.text.strip() if l is not None and l.text else None
                    d = None
                    if pd is not None and pd.text:
                        try:
                            d = dt.datetime.strptime(pd.text.strip(), "%a, %d %b %Y %H:%M:%S %z").date()
                        except Exception:
                            d = None
                    if href:
                        items.append((href, d))

        older = [(u, d) for (u, d) in items if d and current_date and d < current_date]
        if older:
            older.sort(key=lambda x: x[1], reverse=True)  # closest earlier
            return older[0][0]
    return None


# ---------- Parsing current post ----------

def parse_post(url: str):
    """Return (title, published_date) for a post URL, best-effort."""
    r = SESSION.get(url, timeout=TIMEOUT, allow_redirects=True)
    r.raise_for_status()
    s = BeautifulSoup(r.text, "html.parser")

    title = None
    # Typical WP/GOV.UK blog title
    h1 = s.select_one("h1.entry-title, h1.post-title, h1")
    if h1 and h1.get_text(strip=True):
        title = h1.get_text(strip=True)

    if not title:
        m = s.select_one('meta[property="og:title"]') or s.select_one('meta[name="title"]')
        if m and m.get("content"):
            title = m["content"].strip()

    if not title and s.title and s.title.string:
        title = s.title.string.strip()

    pub_date = current_page_date(s, url) or date_from_url(url)

    return title, pub_date, s


# ---------- DB upserts & summaries ----------

def get_sources(conn, only_host=None):
    with conn.cursor() as cur:
        if only_host:
            cur.execute("""
                SELECT id, name, url, is_enabled
                FROM Source
                WHERE kind='Blog' AND is_enabled=1 AND
                      LOWER(SUBSTRING_INDEX(SUBSTRING_INDEX(url,'/',3),'/',-1))=%s
            """, (only_host.lower(),))
        else:
            cur.execute("""
                SELECT id, name, url, is_enabled
                FROM Source
                WHERE kind='Blog' AND is_enabled=1
            """)
        return cur.fetchall()

def blogpost_exists(conn, url: str):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM BlogPost WHERE url=%s", (url,))
        row = cur.fetchone()
        return row["id"] if row else None

def upsert_blogpost(conn, blog_name: str, url: str, title: str, published_at):
    """Insert new or update existing BlogPost row for this URL."""
    with conn.cursor() as cur:
        cur.execute("SELECT id, title, published_at FROM BlogPost WHERE url=%s", (url,))
        row = cur.fetchone()
        if not row:
            cur.execute("""
                INSERT INTO BlogPost (blog_name, url, title, published_at)
                VALUES (%s, %s, %s, %s)
            """, (blog_name, url, title or "", published_at))
            return True
        # Update if title or date improved
        need = False
        new_title = row["title"] or ""
        new_date = row["published_at"]
        if (title or "") and (title or "") != (row["title"] or ""):
            new_title = title
            need = True
        if published_at and (row["published_at"] is None or published_at != row["published_at"]):
            new_date = published_at
            need = True
        if need:
            cur.execute("""
                UPDATE BlogPost
                SET title=%s, published_at=%s
                WHERE id=%s
            """, (new_title, new_date, row["id"]))
            return True
    return False

def update_source_summary(conn, src_id: int, host: str):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE Source s
            LEFT JOIN (
              SELECT COUNT(*) AS total,
                     DATE(MIN(published_at)) AS first_dt,
                     DATE(MAX(published_at)) AS last_dt
              FROM BlogPost
              WHERE url LIKE CONCAT('https://', %s, '/%%')
            ) b ON 1=1
            SET s.total_posts = b.total,
                s.first_post_date = b.first_dt,
                s.last_post_date  = b.last_dt,
                s.last_success    = NOW()
            WHERE s.id=%s
        """, (host, src_id))


# ---------- Crawl loop per source ----------

def crawl_source(conn, src, args):
    src_id, name, url, enabled = src["id"], src["name"], src["url"], src["is_enabled"]
    host = host_of(url)
    base = base_of(url)
    log.info("Source #%s | %s (%s)", src_id, name, host)

    # Determine start URL
    start_url = args.start_url
    if not start_url:
        start_url = feed_latest_post(base)
        if not start_url:
            log.error("Could not determine latest post via feed for %s", host)
            return 0

    # Walk backwards
    visited = set()
    count_new_or_updated = 0
    url_next = start_url

    while url_next:
        if url_next in visited:
            log.debug("Cycle detected at %s; stopping", url_next)
            break
        visited.add(url_next)

        try:
            title, pub_date, soup = parse_post(url_next)
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", "?")
            log.warning("HTTP %s for %s", status, url_next)
            # try to skip to older via feed if we at least know current date
            d = date_from_url(url_next)
            if not d:
                d = None
            older = feed_prev_post(base, d) if d else None
            url_next = older
            if args.sleep: time.sleep(args.sleep)
            continue
        except Exception as e:
            log.warning("Error parsing %s: %s", url_next, e)
            break

        # Store into DB
        changed = upsert_blogpost(conn, name, url_next, title, pub_date)
        if changed:
            count_new_or_updated += 1
            conn.commit()

        # Decide next older link
        older = find_older_link(soup, url_next)
        if not older:
            # feed fallback: use page's date if possible
            curr = pub_date or current_page_date(soup, url_next)
            older = feed_prev_post(base, curr)

        # Stop if limit reached
        if args.max_posts and len(visited) >= args.max_posts:
            log.debug("Reached max-posts=%s for %s", args.max_posts, host)
            older = None

        url_next = older
        if args.sleep:
            time.sleep(args.sleep)

    # Update source summary
    update_source_summary(conn, src_id, host)
    conn.commit()
    log.info("Done %s | new/updated rows: %s", host, count_new_or_updated)
    return count_new_or_updated


# ---------- Main ----------

def main():
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    load_dotenv()
    conn = get_db()

    only_host = (args.only_host or "").strip().lower()

    with conn:
        sources = get_sources(conn, only_host=only_host if only_host else None)
        if not sources:
            if only_host:
                log.info("No enabled Blog sources for host=%s", only_host)
            else:
                log.info("No enabled Blog sources found")
            return

        log.info("Processing %d sources ...", len(sources))
        total = 0
        for src in sources:
            try:
                total += crawl_source(conn, src, args)
            except Exception as e:
                conn.rollback()
                log.error("Failed for %s: %s", host_of(src['url']), e)
        log.info("All done. New/updated posts total: %s", total)


if __name__ == "__main__":
    main()

