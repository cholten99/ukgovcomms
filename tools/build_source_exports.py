#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build a zipped export of data referenced by Source, grouped by Source.kind.

For each distinct kind in Source (optionally only enabled):
  - sources_<Kind>.csv : rows from Source for that kind
  - items_<Kind>.csv   : rows from that kind's "items table" joined to Source

Current mappings:
  Blog    -> BlogPost   (joined by host derived from URLs)
Future examples (auto-detected if tables/columns exist):
  YouTube -> YouTubeVideo (prefer join by channel_id if both sides have it)

Outputs:
  exports/ukgovcomms-data_YYYYMMDD_HHMMSS.zip
  symlink: exports/ukgovcomms-data-latest.zip

Retention:
  --keep N           keep the N most recent archives (default: 5)
  --max-age-days D   also delete any matching archives older than D days
"""

import os, sys, csv, zipfile, argparse, datetime as dt, tempfile, glob, time
import pymysql

# --- helpers ---------------------------------------------------------------

def env(k, d=None):
    v = os.getenv(k, d)
    if v is None:
        print(f"[WARN] missing env {k}", file=sys.stderr)
    return v

def connect(server_side=False):
    return pymysql.connect(
        host=env("DB_HOST","localhost"),
        user=env("DB_USER"),
        password=env("DB_PASSWORD"),
        database=env("DB_NAME"),
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.SSCursor if server_side else pymysql.cursors.Cursor,
    )

def table_exists(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
            LIMIT 1
        """, (env("DB_NAME"), table_name))
        return cur.fetchone() is not None

def columns_of(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position
        """, (env("DB_NAME"), table_name))
        return [r[0] for r in cur.fetchall()]

def distinct_kinds(conn, include_disabled=False):
    where = "WHERE 1=1"
    if not include_disabled:
        where += " AND is_enabled=1"
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT kind FROM Source {where} ORDER BY kind")
        return [row[0] for row in cur.fetchall() if row[0]]

def safe_write_csv(cur, out_path):
    cols = [d[0] for d in cur.description]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for row in cur:
            w.writerow([row[i] if row[i] is not None else "" for i in range(len(cols))])

# --- strategy per kind -----------------------------------------------------

def items_strategy_for_kind(conn, kind):
    """
    Return a dict describing how to export items for this kind, or None to skip.
    Strategy fields:
      table:  items table name
      join:   'channel' or 'host'
      note:   human text
    """
    kind = (kind or "").strip()

    # Known mapping
    mapping = {
        "Blog": "BlogPost",
        "Blogs": "BlogPost",
        # Future examples (will only run if tables exist):
        "YouTube": "YouTubeVideo",
        "Video": "YouTubeVideo",
        "Channel": "YouTubeVideo",
    }
    items_table = mapping.get(kind)
    if not items_table:
        return None
    if not table_exists(conn, items_table):
        return None

    cols = set(columns_of(conn, items_table))
    if "channel_id" in cols:
        return {"table": items_table, "join": "channel", "note": "join by channel_id (if present)"}
    if "url" in cols:
        return {"table": items_table, "join": "host", "note": "join by host from url"}
    return None

# --- exporters -------------------------------------------------------------

def export_sources_for_kind(conn, kind, out_path, include_disabled=False):
    where = "WHERE kind=%s"
    if not include_disabled:
        where += " AND is_enabled=1"
    sql = f"""SELECT id,name,url,kind,is_enabled,last_success,first_post_date,last_post_date,total_posts
              FROM Source
              {where}
              ORDER BY name"""
    with conn.cursor() as cur:
        cur.execute(sql, (kind,))
        safe_write_csv(cur, out_path)

def export_items_for_kind(conn, kind, strategy, out_path, include_disabled=False):
    t = strategy["table"]
    join = strategy["join"]
    if join == "channel":
        where = "s.kind=%s"
        if not include_disabled:
            where += " AND s.is_enabled=1"
        sql = f"""
            SELECT t.*
            FROM `{t}` t
            JOIN Source s
              ON s.channel_id IS NOT NULL
             AND t.channel_id IS NOT NULL
             AND t.channel_id = s.channel_id
            WHERE {where}
        """
        params = (kind,)
    elif join == "host":
        where = "s.kind=%s"
        if not include_disabled:
            where += " AND s.is_enabled=1"
        sql = f"""
            SELECT t.*
            FROM `{t}` t
            JOIN Source s
              ON LOWER(SUBSTRING_INDEX(SUBSTRING_INDEX(s.url,'/',3),'/',-1))
               = LOWER(SUBSTRING_INDEX(SUBSTRING_INDEX(t.url,'/',3),'/',-1))
            WHERE {where}
        """
        params = (kind,)
    else:
        raise ValueError("Unknown join strategy")

    with conn.cursor() as cur:
        cur.execute(sql, params)
        safe_write_csv(cur, out_path)

# --- retention -------------------------------------------------------------

def prune_exports(directory, pattern, keep=5, max_age_days=None):
    """
    Keep the newest `keep` files matching pattern, optionally also delete any
    older than `max_age_days`. Does not touch the '-latest' symlink.
    """
    paths = [p for p in glob.glob(os.path.join(directory, pattern)) if not os.path.islink(p)]
    if not paths:
        return
    # sort newest first by mtime
    paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)

    now = time.time()
    victims = set()

    # by count
    if keep is not None and keep >= 0 and len(paths) > keep:
        victims.update(paths[keep:])

    # by age
    if max_age_days is not None and max_age_days > 0:
        cutoff = now - (max_age_days * 86400)
        for p in paths:
            if os.path.getmtime(p) < cutoff:
                victims.add(p)

    for p in sorted(victims):
        try:
            os.remove(p)
            print(f"[PRUNE] removed {os.path.relpath(p)}")
        except FileNotFoundError:
            pass

# --- main ------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Export data for all kinds in Source into a single ZIP.")
    ap.add_argument("--include-disabled", action="store_true",
                    help="Include Source rows with is_enabled=0")
    ap.add_argument("--keep", type=int, default=5,
                    help="Keep the N most recent data archives (default: 5). Use 0 to keep none beyond latest.")
    ap.add_argument("--max-age-days", type=int, default=None,
                    help="Also delete any archives older than this many days.")
    args = ap.parse_args()

    os.makedirs("exports", exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_name = f"exports/ukgovcomms-data_{ts}.zip"
    latest = "exports/ukgovcomms-data-latest.zip"

    conn_meta = connect(server_side=False)
    conn_ss   = connect(server_side=True)

    kinds = distinct_kinds(conn_meta, include_disabled=args.include_disabled)
    if not kinds:
        print("[INFO] No kinds found in Source.", file=sys.stderr)
        sys.exit(0)

    print(f"[INFO] Kinds detected: {', '.join(kinds)}")

    with tempfile.TemporaryDirectory() as tmp, zipfile.ZipFile(zip_name, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # all sources (convenience)
        all_src_csv = os.path.join(tmp, "sources_all.csv")
        with conn_meta.cursor() as cur:
            where = "WHERE 1=1"
            if not args.include_disabled:
                where += " AND is_enabled=1"
            cur.execute(f"""SELECT id,name,url,kind,is_enabled,last_success,first_post_date,last_post_date,total_posts
                            FROM Source {where} ORDER BY kind,name""")
            safe_write_csv(cur, all_src_csv)
        zf.write(all_src_csv, "sources_all.csv")

        for kind in kinds:
            fn_sources = f"sources_{kind}.csv".replace(" ", "_")
            p_sources = os.path.join(tmp, fn_sources)
            export_sources_for_kind(conn_meta, kind, p_sources, include_disabled=args.include_disabled)
            zf.write(p_sources, fn_sources)

            strat = items_strategy_for_kind(conn_meta, kind)
            if strat is None:
                print(f"[INFO] No items export for kind='{kind}' (no known table/columns).")
                continue
            fn_items = f"items_{kind}.csv".replace(" ", "_")
            p_items = os.path.join(tmp, fn_items)
            export_items_for_kind(conn_ss, kind, strat, p_items, include_disabled=args.include_disabled)
            zf.write(p_items, fn_items)
            print(f"[OK] Exported {fn_sources} and {fn_items}")

    # Update "latest" symlink (best-effort)
    try:
        if os.path.islink(latest) or os.path.exists(latest):
            os.remove(latest)
        os.symlink(os.path.basename(zip_name), latest)
    except Exception:
        pass

    print(f"[DONE] Wrote {zip_name}")
    print(f"[DONE] Latest -> {latest}")

    # Retention (only for this family's archives)
    prune_exports("exports", "ukgovcomms-data_*.zip", keep=args.keep, max_age_days=args.max_age_days)

if __name__ == "__main__":
    main()

