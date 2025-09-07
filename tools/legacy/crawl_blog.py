#!/usr/bin/env python3
import sys
import os
import time
import re
import logging
import argparse
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Optional import only when --write-db is used
try:
    import pymysql  # noqa: F401
    HAVE_PYMYSQL = True
except Exception:
    HAVE_PYMYSQL = False

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UKGovCommsBlogCrawler/1.0)"
}

# Matches canonical post URLs like: https://gds.blog.gov.uk/2025/08/18/some-title/
POST_URL_REGEX = re.compile(r"^https://[a-z0-9\-]+\.blog\.gov\.uk/\d{4}/\d{2}/\d{2}/", re.IGNORECASE)


def fetch(url, session, max_retries=5, backoff_base=1.7, timeout=25):
    """HTTP GET with exponential backoff on 429/5xx and network errors."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=timeout)
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                wait = backoff_base ** attempt
                logging.warning("Got %s from %s. Backing off %.1fs (attempt %d/%d).",
                                resp.status_code, url, wait, attempt, max_retries)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            if attempt == max_retries:
                logging.error("Failed to fetch %s after %d attempts: %s", url, attempt, e)
                raise
            wait = backoff_base ** attempt
            logging.warning("Error fetching %s: %s. Retrying in %.1fs (attempt %d/%d).",
                            url, e, wait, attempt, max_retries)
            time.sleep(wait)
    raise RuntimeError(f"Unreachable state fetching {url}")


def find_latest_post_url(home_html):
    """Find the newest post URL on the blog homepage."""
    soup = BeautifulSoup(home_html, "html.parser")

    # Prefer obvious permalinks by regex
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if POST_URL_REGEX.match(href):
            logging.debug("Latest matched by regex: %s", href)
            return href

    # Fallback: first article h2 a
    a = soup.select_one("article h2 a[href]")
    if a and POST_URL_REGEX.match(a["href"]):
        return a["href"]

    raise ValueError("Could not locate the latest post URL on the homepage.")


def extract_title_and_date(post_html):
    """Best-effort extraction of title & published date."""
    soup = BeautifulSoup(post_html, "html.parser")
    title = None

    # Common title locations
    for sel in ["article h1", ".entry-title", "h1.entry-title", "h1"]:
        el = soup.select_one(sel)
        if el and el.get_text(strip=True):
            title = el.get_text(strip=True)
            break

    # Published date candidates
    date = None

    # <time datetime="...">
    t = soup.select_one("time[datetime]")
    if t and t.get("datetime"):
        date = t["datetime"].strip()[:10]  # YYYY-MM-DD

    # <meta property="article:published_time" content="...">
    if not date:
        meta = soup.find("meta", attrs={"property": "article:published_time"})
        if meta and meta.get("content"):
            date = meta["content"].strip()[:10]

    # GOV.UK WP often shows date text inside time element as well
    if not date and t and t.get_text(strip=True):
        # Try parse YYYY or similar fragments; keep YYYY-MM-DD if present
        m = re.search(r"\d{4}-\d{2}-\d{2}", t.get_text())
        if m:
            date = m.group(0)

    return title or "", date


def find_prev_next_urls(post_html, current_url):
    """Return (previous_url, next_url) using multiple strategies."""
    soup = BeautifulSoup(post_html, "html.parser")

    prev_url = None
    next_url = None

    # Strategy 1: rel attrs
    rel_prev = soup.find("a", rel=lambda v: v and ("prev" in v or "previous" in v))
    if rel_prev and rel_prev.get("href"):
        prev_url = urljoin(current_url, rel_prev["href"].strip())

    rel_next = soup.find("a", rel=lambda v: v and "next" in v)
    if rel_next and rel_next.get("href"):
        next_url = urljoin(current_url, rel_next["href"].strip())

    # Strategy 2: arrow glyphs (← →)
    if not prev_url:
        for a in soup.find_all("a", href=True):
            txt = (a.get_text() or "").strip()
            if txt.startswith("←") or txt.startswith("\u2190"):
                prev_url = urljoin(current_url, a["href"].strip())
                break
    if not next_url:
        for a in soup.find_all("a", href=True):
            txt = (a.get_text() or "").strip()
            if txt.endswith("→") or txt.endswith("\u2192"):
                next_url = urljoin(current_url, a["href"].strip())
                break

    # Strategy 3: common WP classes
    if not prev_url:
        cand = soup.select_one(".nav-previous a, .previous a, a.previous, a.nav-previous")
        if cand and cand.get("href"):
            prev_url = urljoin(current_url, cand["href"].strip())
    if not next_url:
        cand = soup.select_one(".nav-next a, .next a, a.next, a.nav-next")
        if cand and cand.get("href"):
            next_url = urljoin(current_url, cand["href"].strip())

    # Strategy 4: nav blocks with 2 links (left=prev, right=next)
    if not prev_url or not next_url:
        for block in soup.select("nav, .post-nav, .entry-nav, .blog-nav, .gds-blog__post-nav, .pagination"):
            links = block.find_all("a", href=True)
            if len(links) >= 1 and not prev_url:
                prev_url = urljoin(current_url, links[0]["href"].strip())
            if len(links) >= 2 and not next_url:
                next_url = urljoin(current_url, links[-1]["href"].strip())
            if prev_url and next_url:
                break

    # Sanity: only accept links that look like post permalinks
    if prev_url and not POST_URL_REGEX.match(prev_url):
        logging.debug("Discarding prev that doesn't look like a post: %s", prev_url)
        prev_url = None
    if next_url and not POST_URL_REGEX.match(next_url):
        logging.debug("Discarding next that doesn't look like a post: %s", next_url)
        next_url = None

    return prev_url, next_url


def infer_blog_name(base_url):
    """Make a tidy blog name from the hostname, e.g. 'GDS blog' from gds.blog.gov.uk."""
    host = urlparse(base_url).hostname or ""
    part = host.split(".")[0].replace("-", " ").strip().title()
    return f"{part} blog" if part else "GOV.UK blog"


def upsert_blogpost(conn, blog_name, url, title, published_at, previous_url, next_url):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO BlogPost (blog_name, url, title, published_at, previous_url, next_url)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              blog_name = VALUES(blog_name),
              title = VALUES(title),
              published_at = VALUES(published_at),
              previous_url = VALUES(previous_url),
              next_url = VALUES(next_url),
              updated_at = CURRENT_TIMESTAMP
            """,
            (blog_name, url, title, published_at, previous_url, next_url),
        )
    conn.commit()


def main():
    p = argparse.ArgumentParser(description="Crawl all posts for a *.blog.gov.uk site by following the left-arrow (previous) link.")
    p.add_argument("base_url", help="Base blog URL, e.g. https://gds.blog.gov.uk/")
    p.add_argument("--max-posts", type=int, default=100000, help="Safety limit (default: 100000)")
    p.add_argument("--sleep", type=float, default=0.8, help="Seconds to sleep between requests (default: 0.8)")
    p.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    p.add_argument("--write-db", action="store_true", help="Insert/Update rows into UKGovComms.BlogPost using .env")
    args = p.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(levelname)s: %(message)s")

    # Guardrails
    if not args.base_url.endswith("/"):
        args.base_url += "/"

    session = requests.Session()

    # Optional DB connection
    conn = None
    if args.write_db:
        if not HAVE_PYMYSQL:
            logging.error("PyMySQL not installed. Install it or run without --write-db.")
            sys.exit(2)
        # Load DB creds from .env in CWD (minimal parser)
        env_path = ".env"
        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())
        db_host = os.environ.get("DB_HOST", "localhost")
        db_name = os.environ.get("DB_NAME", "UKGovComms")
        db_user = os.environ.get("DB_USER")
        db_pass = os.environ.get("DB_PASSWORD")
        if not (db_user and db_pass):
            logging.error("DB_USER/DB_PASSWORD not found in environment/.env; cannot use --write-db.")
            sys.exit(2)

        import pymysql
        conn = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name,
            charset="utf8mb4",
            autocommit=False,
        )
        logging.info("DB connection established to %s.%s as %s", db_host, db_name, db_user)

    blog_name = infer_blog_name(args.base_url)

    # 1) Find newest post
    home_html = fetch(args.base_url, session)
    latest_url = find_latest_post_url(home_html)

    seen = set()
    current = latest_url
    count = 0

    while current and current not in seen and count < args.max_posts:
        seen.add(current)
        post_html = fetch(current, session)
        title, published_at = extract_title_and_date(post_html)
        prev_url, next_url = find_prev_next_urls(post_html, current)

        # Output
        print(current)
        logging.debug("Title: %s | Date: %s | Prev: %s | Next: %s", title, published_at, prev_url, next_url)

        # Optional DB write
        if conn:
            try:
                upsert_blogpost(conn, blog_name, current, title, published_at, prev_url, next_url)
            except Exception as e:
                logging.error("DB upsert failed for %s: %s", current, e)
                conn.rollback()
                # keep going; we still can crawl

        # Move to previous (older) post
        if not prev_url:
            logging.info("No previous link found; stopping.")
            break
        current = prev_url
        count += 1
        time.sleep(args.sleep)

    if conn:
        conn.close()
        logging.info("DB connection closed.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        logging.error("Fatal error: %s", e)
        sys.exit(1)

