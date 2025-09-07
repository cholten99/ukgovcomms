#!/usr/bin/env python3
"""
Render charts + wordcloud for a single Source (generic).
- Works for kind='Blog' now (uses BlogPost).
- Also works for kind='YouTube' if you have a YouTubeVideo table with source_id FK.

Outputs (under --outdir / assets/sources/<slug>/):
- monthly_bars_<slug>.png
- rolling_avg_<Nd>_<slug>.png
- wordcloud_<slug>.png

Select the source by one of:
  --id <Source.id>     (preferred)
  --host <gds.blog.gov.uk>

Usage:
  python3 tools/render_source_assets.py --id 123
  python3 tools/render_source_assets.py --host gds.blog.gov.uk --rolling-days 90
"""

import os
import re
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from wordcloud import WordCloud


# ---- Stopwords (remembered list + small extras) ----
DEFAULT_STOPWORDS = {
    "gds", "gov", "govuk", "gov.uk", "uk",
    "blog", "week", "weeks", "new", "day", "s",
    "and", "the", "for", "with", "from", "into", "our", "we",
}

SUPPORTED_KINDS = {"Blog", "YouTube"}  # extend later (e.g., RSS, Podcast)


# ---------- Helpers ----------

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


def get_conn():
    host = os.environ.get("DB_HOST", "localhost")
    name = os.environ.get("DB_NAME", "UKGovComms")
    user = os.environ.get("DB_USER")
    pwd = os.environ.get("DB_PASSWORD")
    if not (user and pwd):
        raise RuntimeError("DB_USER/DB_PASSWORD not set")
    return pymysql.connect(host=host, user=user, password=pwd, database=name, charset="utf8mb4")


def slugify(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "-", (name or "").strip()).strip("-").lower() or "source"


def fetch_source(conn, id: int | None = None, host: str | None = None) -> dict:
    """
    Fetch a single enabled Source by id or host. No use of channel/external IDs here.
    """
    sql = "SELECT id, name, url, kind FROM Source WHERE is_enabled=1"
    params = []
    if id is not None:
        sql += " AND id=%s"
        params.append(id)
    elif host:
        sql += " AND SUBSTRING_INDEX(SUBSTRING_INDEX(url,'/',3),'/',-1)=%s"
        params.append(host.lower())
    else:
        raise ValueError("Provide --id or --host")
    sql += " LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    if not row:
        raise RuntimeError("Source not found with given selector.")
    sid, name, url, kind = row
    if kind not in SUPPORTED_KINDS:
        raise RuntimeError(f"Source kind '{kind}' not supported yet.")
    return {"id": sid, "name": name, "url": url, "kind": kind}


def host_from_url(url: str) -> str:
    return url.split("//", 1)[-1].split("/", 1)[0].lower()


def fetch_items_df(conn, src: dict) -> pd.DataFrame:
    """
    Return DataFrame with at least ['title','published_at'] (datetime64[ns])
    for the given Source (Blog or YouTube).
    """
    if src["kind"] == "Blog":
        host = host_from_url(src["url"])
        sql = """
          SELECT title, published_at
          FROM BlogPost
          WHERE url LIKE CONCAT('https://', %s, '/%%')
          ORDER BY published_at ASC
        """
        df = pd.read_sql(sql, conn, params=[host])
    elif src["kind"] == "YouTube":
        # Requires a YouTubeVideo table with source_id FK
        sql = """
          SELECT title, published_at
          FROM YouTubeVideo
          WHERE source_id=%s
          ORDER BY published_at ASC
        """
        df = pd.read_sql(sql, conn, params=[src["id"]])
    else:
        raise RuntimeError(f"Unsupported kind: {src['kind']}")

    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    df = df.dropna(subset=["published_at"]).sort_values("published_at")
    if df.empty:
        raise RuntimeError("No items found for this source.")
    return df


def compute_summary(df: pd.DataFrame):
    first_dt = df["published_at"].min()
    last_dt = df["published_at"].max()
    total = len(df)
    return first_dt, last_dt, total


def add_summary(ax, first_dt, last_dt, total):
    txt = f"First: {first_dt.strftime('%Y-%m-%d')}  |  Last: {last_dt.strftime('%Y-%m-%d')}  |  Total: {total}"
    ax.text(
        0.99, 0.98, txt, transform=ax.transAxes, ha="right", va="top",
        fontsize=9, bbox=dict(boxstyle="round", facecolor="white", alpha=0.7, edgecolor="none")
    )


# ---------- Charts ----------

def plot_monthly_bars(df: pd.DataFrame, out_path: Path, title: str):
    s = df.set_index("published_at").assign(count=1)["count"].resample("MS").sum()
    x = mdates.date2num(s.index.to_pydatetime())
    bar_w = 26  # days

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar(x, s.values, width=bar_w, align="center", label="Items per month")
    ax.set_title(title)
    ax.set_xlabel("Month")
    ax.set_ylabel("Items")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    fig.autofmt_xdate(rotation=45)

    first_dt, last_dt, total = compute_summary(df)
    add_summary(ax, first_dt, last_dt, total)
    ax.legend()

    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_rolling_avg(df: pd.DataFrame, out_path: Path, title: str, window_days: int = 90):
    s_daily = df.set_index("published_at").assign(count=1)["count"].resample("D").sum()
    roll = s_daily.rolling(window=f"{window_days}D", min_periods=max(5, window_days // 6)).mean()

    fig, ax = plt.subplots(figsize=(11, 5))
    roll.plot(ax=ax)
    ax.set_title(title)
    ax.set_xlabel("Date")
    ax.set_ylabel("Items per day (avg)")

    first_dt, last_dt, total = compute_summary(df)
    add_summary(ax, first_dt, last_dt, total)

    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=150)
    plt.close()


# ---------- Word cloud ----------

def clean_text(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[‘’´`']", " ", s)
    s = re.sub(r"[^a-z0-9\s\-\.]", " ", s)
    s = re.sub(r"\b\d{1,4}\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def render_wordcloud(df: pd.DataFrame, out_path: Path, stopwords=None, width=1600, height=900):
    titles = [str(t) for t in df["title"].fillna("").tolist() if str(t).strip()]
    text = " ".join(clean_text(t) for t in titles)
    sw = set(stopwords or [])
    wc = WordCloud(
        width=width, height=height, background_color="white",
        stopwords=sw, collocations=True, prefer_horizontal=0.9
    ).generate(text)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(width / 100, height / 100))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout(pad=0)
    plt.savefig(out_path, dpi=150)
    plt.close()


# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser(description="Render charts + wordcloud for a single Source.")
    ap.add_argument("--id", type=int, help="Source.id")
    ap.add_argument("--host", help="Host (e.g., gds.blog.gov.uk)")
    ap.add_argument("--outdir", default="assets/sources", help="Base output dir")
    ap.add_argument("--rolling-days", type=int, default=90, help="Window for rolling avg")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s: %(message)s"
    )

    load_env(".env")
    conn = get_conn()
    try:
        src = fetch_source(conn, id=args.id, host=args.host)
        df = fetch_items_df(conn, src)

        slug = slugify(src["name"])
        base = Path(args.outdir) / slug

        plot_monthly_bars(
            df,
            base / f"monthly_bars_{slug}.png",
            title=f"{src['name']} : Posts per month"
        )
        plot_rolling_avg(
            df,
            base / f"rolling_avg_{args.rolling_days}d_{slug}.png",
            title=f"{src['name']} : Rolling average posts/day ({args.rolling_days}-day)",
            window_days=args.rolling_days
        )
        render_wordcloud(
            df,
            base / f"wordcloud_{slug}.png",
            stopwords=DEFAULT_STOPWORDS
        )

        logging.info("Rendered assets for %s (%s) into %s", src["name"], src["kind"], base)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

