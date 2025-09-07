#!/usr/bin/env python3
"""
Generate a word cloud from all GDS blog post titles (BlogPost table).

Output:
- <outdir>/wordcloud_gds_blog.png

Usage:
  python3 tools/wordcloud_gds_blog.py --outdir charts/ --log-level INFO
  # Optional: restrict by date
  python3 tools/wordcloud_gds_blog.py --start 2015-01-01 --end 2025-12-31
"""

import os
import sys
import re
import argparse
import logging
from pathlib import Path
from datetime import datetime

import pymysql
import pandas as pd
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt


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


def get_conn():
    host = os.environ.get("DB_HOST", "localhost")
    name = os.environ.get("DB_NAME", "UKGovComms")
    user = os.environ.get("DB_USER")
    pwd  = os.environ.get("DB_PASSWORD")
    if not (user and pwd):
        raise RuntimeError("DB_USER/DB_PASSWORD not set in env/.env")
    return pymysql.connect(host=host, user=user, password=pwd, database=name, charset="utf8mb4")


def fetch_titles(conn, start=None, end=None):
    """
    Parameterised LIKE to avoid '%' formatting issues in PyMySQL.
    """
    params = ["https://gds.blog.gov.uk/%"]
    where = ["url LIKE %s", "title IS NOT NULL", "title <> ''"]

    if start:
        where.append("published_at >= %s")
        params.append(start)
    if end:
        where.append("published_at <= %s")
        params.append(end)

    sql = f"""
        SELECT title
        FROM BlogPost
        WHERE {' AND '.join(where)}
    """
    df = pd.read_sql(sql, conn, params=params)
    return [t for t in df["title"].astype(str).tolist() if t.strip()]


def build_stopwords(extra=None):
    sw = set(STOPWORDS)
    sw.update({
        "gds", "gov", "govuk", "gov.uk", "uk",
        "blog", "week", "weeks", "new", "day", "S",
        "and", "the", "for", "with", "from", "into", "our", "we",
    })
    if extra:
        sw.update({w.strip().lower() for w in extra.split(",") if w.strip()})
    return sw


def clean_text(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[‘’´`']", " ", s)
    s = re.sub(r"[^a-z0-9\s\-\.]", " ", s)
    s = re.sub(r"\b\d{1,4}\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def make_wordcloud(text: str, out_path: Path, width=1600, height=900, stopwords=None):
    wc = WordCloud(
        width=width,
        height=height,
        background_color="white",
        stopwords=stopwords or set(),
        collocations=True,
        prefer_horizontal=0.9
    ).generate(text)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(width/100, height/100))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout(pad=0)
    plt.savefig(out_path, dpi=150)
    plt.close()


def main():
    ap = argparse.ArgumentParser(description="Create a word cloud from GDS blog post titles.")
    ap.add_argument("--outdir", default="charts", help="Output directory (default: charts)")
    ap.add_argument("--start", help="Start date (YYYY-MM-DD)")
    ap.add_argument("--end", help="End date (YYYY-MM-DD)")
    ap.add_argument("--extra-stopwords", help="Comma-separated extra stopwords")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(levelname)s: %(message)s")

    for label, val in (("start", args.start), ("end", args.end)):
        if val:
            try:
                datetime.strptime(val, "%Y-%m-%d")
            except ValueError:
                logging.error("Invalid %s date (use YYYY-MM-DD): %s", label, val)
                sys.exit(2)

    load_env(".env")

    try:
        conn = get_conn()
    except Exception as e:
        logging.error("DB connection failed: %s", e)
        sys.exit(2)

    try:
        titles = fetch_titles(conn, start=args.start, end=args.end)
        if not titles:
            logging.error("No titles found for GDS blog in the selected range.")
            sys.exit(1)
        logging.info("Fetched %d titles.", len(titles))

        text = " ".join(clean_text(t) for t in titles)
        stopwords = build_stopwords(args.extra_stopwords)
        out_path = Path(args.outdir) / "wordcloud_gds_blog.png"
        make_wordcloud(text, out_path, stopwords=stopwords)

        logging.info("Wrote %s", out_path)
        print(out_path.as_posix())
    except Exception as e:
        logging.error("Failed: %s", e)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

