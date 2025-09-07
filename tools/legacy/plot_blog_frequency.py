#!/usr/bin/env python3
"""
Plot posting frequency for a GOV.UK blog stored in the BlogPost table.

Outputs:
- monthly_bars_<blogslug>.png            (bars per month)
- rolling_avg_<Nd>_<blogslug>.png       (rolling avg posts/day over window)
- monthly_counts_<blogslug>.csv is written temporarily, then removed

Usage:
  python3 tools/plot_blog_frequency.py --blog-name "GDS blog" --outdir charts/ --log-level INFO
  python3 tools/plot_blog_frequency.py --blog-name "GDS blog" --start 2015-01-01 --end 2025-12-31
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path
import re

import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def load_env_into_os(env_path=".env"):
    """Minimal .env loader (no extra deps)."""
    if not os.path.exists(env_path):
        logging.info(".env not found at %s (will rely on environment variables).", env_path)
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
    except Exception as e:
        logging.warning("Failed to read .env: %s", e)


def get_connection():
    host = os.environ.get("DB_HOST", "localhost")
    name = os.environ.get("DB_NAME", "UKGovComms")
    user = os.environ.get("DB_USER")
    pwd = os.environ.get("DB_PASSWORD")
    if not user or not pwd:
        raise RuntimeError("DB_USER/DB_PASSWORD not found in environment/.env.")
    return pymysql.connect(
        host=host, user=user, password=pwd, database=name, charset="utf8mb4", autocommit=False
    )


def fetch_posts_df(conn, blog_name, start=None, end=None):
    """Return DataFrame with columns: url, title, published_at (datetime64[ns])."""
    params = [blog_name]
    where = ["blog_name = %s", "published_at IS NOT NULL"]

    if start:
        where.append("published_at >= %s")
        params.append(start)
    if end:
        where.append("published_at <= %s")
        params.append(end)

    sql = f"""
        SELECT url, title, published_at
        FROM BlogPost
        WHERE {' AND '.join(where)}
        ORDER BY published_at ASC
    """
    df = pd.read_sql(sql, conn, params=params)
    if df.empty:
        raise RuntimeError(f"No rows found for blog_name='{blog_name}' in selected date range.")
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    df = df.dropna(subset=["published_at"]).sort_values("published_at")
    return df


def ensure_outdir(path):
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def slugify(name: str) -> str:
    """Make a safe filename slug from a blog name."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", name.strip()).strip("-")
    return slug.lower() or "blog"


def compute_summary(df):
    """Return (first_date, last_date, total_posts) for the given dataframe."""
    first = df["published_at"].min()
    last = df["published_at"].max()
    total = len(df)
    return first, last, total


def add_summary_annotation(ax, first_dt, last_dt, total_posts, location="top-right"):
    """Add a tidy summary box to the given axes without obscuring data."""
    # Format dates as YYYY-MM-DD
    first_s = first_dt.strftime("%Y-%m-%d")
    last_s = last_dt.strftime("%Y-%m-%d")
    text = f"First: {first_s}  |  Last: {last_s}  |  Total: {total_posts}"

    if location == "bottom-right":
        x, y, ha, va = 0.99, 0.02, "right", "bottom"
    else:  # top-right default
        x, y, ha, va = 0.99, 0.98, "right", "top"

    ax.text(
        x, y, text,
        transform=ax.transAxes,
        fontsize=9,
        va=va, ha=ha,
        bbox=dict(boxstyle="round", facecolor="white", alpha=0.7, edgecolor="none"),
    )


def plot_monthly_bars(df, outdir, blog_name):
    """
    Bar chart of posts per month (no smoothed line).
    Uses explicit bar width so bars remain visible over long ranges.
    """
    blog_slug = slugify(blog_name)
    first_dt, last_dt, total_posts = compute_summary(df)

    # Monthly counts (Month Start frequency)
    s = df.set_index("published_at").assign(count=1)["count"].resample("MS").sum()

    # Write CSV temporarily (then remove)
    out_csv = Path(outdir) / f"monthly_counts_{blog_slug}.csv"
    try:
        s.to_csv(out_csv, header=["posts"])
        logging.info("Wrote %s", out_csv)
    except Exception as e:
        logging.warning("Could not write CSV (%s): %s", out_csv, e)

    # Convert datetime index to matplotlib dates
    x = mdates.date2num(s.index.to_pydatetime())
    bar_width = 26  # days; wide enough to be visible on long ranges

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar(x, s.values, width=bar_width, align="center", label="Posts per month")

    ax.set_title(f"{blog_name} : Posts per month")
    ax.set_xlabel("Month")
    ax.set_ylabel("Posts")

    # Yearly ticks keep labels readable
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    fig.autofmt_xdate(rotation=45)

    # Add summary annotation
    add_summary_annotation(ax, first_dt, last_dt, total_posts, location="top-right")

    ax.legend()
    plt.tight_layout()

    out_png = Path(outdir) / f"monthly_bars_{blog_slug}.png"
    plt.savefig(out_png, dpi=150)
    plt.close()
    logging.info("Wrote %s", out_png)

    # Remove CSV now that the chart is saved
    try:
        if out_csv.exists():
            out_csv.unlink()
            logging.info("Removed temporary %s", out_csv)
    except Exception as e:
        logging.warning("Failed to remove temporary CSV %s: %s", out_csv, e)


def plot_rolling_avg(df, outdir, blog_name, window_days=90):
    """
    Line chart: rolling average posts/day over a time window (default 90 days).
    """
    blog_slug = slugify(blog_name)
    first_dt, last_dt, total_posts = compute_summary(df)

    s_daily = df.set_index("published_at").assign(count=1)["count"].resample("D").sum()
    roll = s_daily.rolling(window=f"{window_days}D", min_periods=max(5, window_days // 6)).mean()

    fig, ax = plt.subplots(figsize=(11, 5))
    roll.plot(ax=ax)

    ax.set_title(f"{blog_name} : Rolling average posts/day ({window_days}-day)")
    ax.set_xlabel("Date")
    ax.set_ylabel("Posts per day (avg)")

    # Add summary annotation
    add_summary_annotation(ax, first_dt, last_dt, total_posts, location="top-right")

    plt.tight_layout()
    out_png = Path(outdir) / f"rolling_avg_{window_days}d_{blog_slug}.png"
    plt.savefig(out_png, dpi=150)
    plt.close()
    logging.info("Wrote %s", out_png)


def main():
    ap = argparse.ArgumentParser(description="Plot monthly counts and rolling average for a GOV.UK blog from BlogPost.")
    ap.add_argument("--blog-name", required=True, help='e.g. "GDS blog"')
    ap.add_argument("--start", help="Start date (YYYY-MM-DD). Optional.")
    ap.add_argument("--end", help="End date (YYYY-MM-DD). Optional.")
    ap.add_argument("--outdir", default="charts", help="Output directory (default: charts)")
    ap.add_argument("--rolling-days", type=int, default=90, help="Rolling window in days (default: 90)")
    ap.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s: %(message)s"
    )

    # Validate dates if provided
    for label, val in (("start", args.start), ("end", args.end)):
        if val:
            try:
                datetime.strptime(val, "%Y-%m-%d")
            except ValueError:
                logging.error("Invalid %s date format (use YYYY-MM-DD): %s", label, val)
                sys.exit(2)

    load_env_into_os(".env")
    outdir = ensure_outdir(args.outdir)

    try:
        conn = get_connection()
    except Exception as e:
        logging.error("DB connection failed: %s", e)
        sys.exit(2)

    try:
        df = fetch_posts_df(conn, args.blog_name, start=args.start, end=args.end)
    except Exception as e:
        logging.error("Failed to fetch data: %s", e)
        conn.close()
        sys.exit(2)

    try:
        plot_monthly_bars(df, outdir, args.blog_name)
        plot_rolling_avg(df, outdir, args.blog_name, window_days=args.rolling_days)
    except Exception as e:
        logging.error("Plotting failed: %s", e)
        conn.close()
        sys.exit(2)

    conn.close()
    logging.info("Done.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        logging.error("Fatal error: %s", e)
        sys.exit(1)

