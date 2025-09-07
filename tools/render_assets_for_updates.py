#!/usr/bin/env python3
"""
Render assets for Sources that have new items since last_success, and/or are missing assets.

Kinds supported now: Blog (via BlogPost). If YouTubeVideo table exists, YouTube is supported too.

Logic:
- Pick Sources where is_enabled=1 and kind IN ('Blog','YouTube') unless --kind filter provided.
- "Needs render" if:
   a) exists item updated_at >= COALESCE(last_success,'1970-01-01'), OR
   b) --catch-up-missing and any expected file is missing.

Usage:
  python3 tools/render_assets_for_updates.py --log-level INFO
  python3 tools/render_assets_for_updates.py --catch-up-missing
  python3 tools/render_assets_for_updates.py --only-host gds.blog.gov.uk
  python3 tools/render_assets_for_updates.py --kind Blog
"""

import os, sys, argparse, logging, re, shlex, subprocess
from pathlib import Path
from datetime import datetime
import pymysql

OUTDIR = "assets/sources"
ROLLING_DAYS = 90
SUPPORTED_KINDS = {"Blog", "YouTube"}

def load_env(env_path=".env"):
    if not os.path.exists(env_path): return
    with open(env_path,"r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            k,v=line.split("=",1); os.environ.setdefault(k.strip(), v.strip())

def get_conn():
    host=os.environ.get("DB_HOST","localhost")
    name=os.environ.get("DB_NAME","UKGovComms")
    user=os.environ.get("DB_USER"); pwd=os.environ.get("DB_PASSWORD")
    if not (user and pwd): raise RuntimeError("DB_USER/DB_PASSWORD not set")
    return pymysql.connect(host=host, user=user, password=pwd, database=name, charset="utf8mb4")

def slugify(name:str)->str:
    return re.sub(r"[^a-zA-Z0-9]+","-", (name or "").strip()).strip("-").lower() or "source"

def expected_files(name:str):
    slug=slugify(name); base=Path(OUTDIR)/slug
    return slug, [base/f"monthly_bars_{slug}.png", base/f"rolling_avg_{ROLLING_DAYS}d_{slug}.png", base/f"wordcloud_{slug}.png"]

def list_sources(conn, only_host=None, kind=None):
    sql="SELECT id,name,url,kind,COALESCE(last_success,'1970-01-01') FROM Source WHERE is_enabled=1"
    params=[]
    if kind:
        sql+=" AND kind=%s"; params.append(kind)
    else:
        sql+=f" AND kind IN ({','.join(['%s']*len(SUPPORTED_KINDS))})"
        params.extend(sorted(SUPPORTED_KINDS))
    if only_host:
        sql+=" AND SUBSTRING_INDEX(SUBSTRING_INDEX(url,'/',3),'/',-1)=%s"; params.append(only_host.lower())
    with conn.cursor() as cur:
        cur.execute(sql, params); return cur.fetchall()

def has_new_items_since(conn, kind:str, url:str, source_id:int, since):
    if kind=="Blog":
        sql="""
          SELECT 1 FROM BlogPost
          WHERE url LIKE CONCAT('https://', SUBSTRING_INDEX(SUBSTRING_INDEX(%s,'/',3),'/',-1), '/%%')
            AND updated_at >= %s
          LIMIT 1
        """
        with conn.cursor() as cur:
            cur.execute(sql, (url, since)); return cur.fetchone() is not None
    elif kind=="YouTube":
        # Requires YouTubeVideo table (proposed earlier)
        sql="SELECT 1 FROM YouTubeVideo WHERE source_id=%s AND updated_at >= %s LIMIT 1"
        with conn.cursor() as cur:
            cur.execute(sql,(source_id, since)); return cur.fetchone() is not None
    return False

def render_one(source_id:int):
    cmd=f"python3 tools/render_source_assets.py --id {source_id} --outdir {OUTDIR} --rolling-days {ROLLING_DAYS}"
    proc=subprocess.run(shlex.split(cmd), capture_output=True, text=True)
    if proc.returncode!=0:
        logging.error("Render failed for Source.id=%s: %s", source_id, proc.stderr.strip())
        return False
    return True

def main():
    ap=argparse.ArgumentParser(description="Render assets where new items exist and/or assets are missing.")
    ap.add_argument("--only-host", help="Limit to host like gds.blog.gov.uk")
    ap.add_argument("--kind", choices=sorted(SUPPORTED_KINDS), help="Limit to a single kind")
    ap.add_argument("--catch-up-missing", action="store_true", help="Also render if expected images missing")
    ap.add_argument("--log-level", default="INFO")
    args=ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(levelname)s: %(message)s")

    load_env(".env")
    conn=get_conn()
    try:
        rows=list_sources(conn, only_host=args.only_host, kind=args.kind)
        if not rows:
            logging.info("No matching sources."); return

        to_render=[]
        for sid,name,url,kind,last_success in rows:
            slug,files=expected_files(name)
            need=False
            if has_new_items_since(conn, kind, url, sid, last_success): need=True; logging.info("New items since %s for %s (%s)", last_success, name, kind)
            if args.catch_up_missing and (not all(p.exists() for p in files)):
                missing=[str(p) for p in files if not p.exists()]
                if missing: need=True; logging.info("Missing assets for %s: %s", name, ", ".join(missing))
            if need: to_render.append(sid)

        if not to_render:
            logging.info("Nothing to render."); return

        ok=sum(1 for sid in to_render if render_one(sid))
        logging.info("Rendered %d/%d sources.", ok, len(to_render))
    finally:
        conn.close()

if __name__=="__main__":
    main()

