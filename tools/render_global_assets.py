#!/usr/bin/env python3
import os, re, argparse, logging
from pathlib import Path
from datetime import datetime, timezone
import pymysql, pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from wordcloud import WordCloud

DEFAULT_STOPWORDS = {
    "gds","gov","govuk","gov.uk","uk",
    "blog","week","weeks","new","day","s",
    "and","the","for","with","from","into","our","we",
    "in","a","an","of","to","too","on","at","by","as",
    "is","are","was","were","be","been","being",
    "it","its","this","that","these","those",
    "not","no","or","but","than","then","there","here",
    "out","up","down","over","under",
    "what","how",  # <-- added
}
OUTDIR=Path("assets/global"); SLUG="all-sources"

def load_env(path=".env"):
    if not os.path.exists(path): return
    for line in open(path,"r",encoding="utf-8"):
        line=line.strip()
        if not line or line.startswith("#") or "=" not in line: continue
        k,v=line.split("=",1); os.environ.setdefault(k.strip(), v.strip())

def get_conn():
    host=os.environ.get("DB_HOST","localhost")
    name=os.environ.get("DB_NAME","UKGovComms")
    user=os.environ.get("DB_USER"); pwd=os.environ.get("DB_PASSWORD")
    if not (user and pwd): raise RuntimeError("DB creds missing")
    return pymysql.connect(host=host, user=user, password=pwd, database=name, charset="utf8mb4")

def fetch_all_items_df(conn)->pd.DataFrame:
    parts=[]
    sql_blog = """
      SELECT bp.title, bp.published_at
      FROM BlogPost bp
      WHERE bp.published_at IS NOT NULL
        AND EXISTS (
          SELECT 1 FROM Source s
          WHERE s.is_enabled=1 AND s.kind='Blog'
            AND SUBSTRING_INDEX(SUBSTRING_INDEX(bp.url,'/',3),'/',-1)
                = SUBSTRING_INDEX(SUBSTRING_INDEX(s.url,'/',3),'/',-1)
        )
      ORDER BY bp.published_at ASC"""
    parts.append(pd.read_sql(sql_blog, conn))
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES LIKE 'YouTubeVideo'")
            if cur.fetchone():
                sql_yt = """
                  SELECT yv.title, yv.published_at
                  FROM YouTubeVideo yv
                  JOIN Source s ON s.id = yv.source_id
                  WHERE s.is_enabled=1 AND s.kind='YouTube' AND yv.published_at IS NOT NULL
                  ORDER BY yv.published_at ASC"""
                parts.append(pd.read_sql(sql_yt, conn))
    except Exception:
        pass
    df=pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["title","published_at"])
    df["published_at"]=pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    df=df.dropna(subset=["published_at"]).sort_values("published_at")
    return df

def latest_item_timestamp(conn):
    latest=None
    with conn.cursor() as cur:
        cur.execute("""
          SELECT COALESCE(MAX(bp.updated_at), MAX(bp.published_at)) AS latest_ts
          FROM BlogPost bp
          WHERE EXISTS (
            SELECT 1 FROM Source s
            WHERE s.is_enabled=1 AND s.kind='Blog'
              AND SUBSTRING_INDEX(SUBSTRING_INDEX(bp.url,'/',3),'/',-1)
                  = SUBSTRING_INDEX(SUBSTRING_INDEX(s.url,'/',3),'/',-1)
          )""")
        row=cur.fetchone()
        if row and row[0]: latest=row[0]
        cur.execute("SHOW TABLES LIKE 'YouTubeVideo'")
        if cur.fetchone():
            cur.execute("""
              SELECT COALESCE(MAX(yv.updated_at), MAX(yv.published_at)) AS latest_ts
              FROM YouTubeVideo yv
              JOIN Source s ON s.id=yv.source_id
              WHERE s.is_enabled=1 AND s.kind='YouTube'""")
            r=cur.fetchone()
            if r and r[0] and (latest is None or r[0]>latest): latest=r[0]
    if latest and isinstance(latest, datetime) and latest.tzinfo is None:
        latest=latest.replace(tzinfo=timezone.utc)
    return latest

def compute_summary(df):
    return df["published_at"].min(), df["published_at"].max(), len(df)

def add_summary(ax, first_dt, last_dt, total):
    ax.text(0.99,0.98,f"First: {first_dt.date()}  |  Last: {last_dt.date()}  |  Total: {total}",
            transform=ax.transAxes,ha="right",va="top",fontsize=9,
            bbox=dict(boxstyle="round",facecolor="white",alpha=0.7,edgecolor="none"))

def plot_monthly_bars(df, out_path:Path):
    s=df.set_index("published_at").assign(count=1)["count"].resample("MS").sum()
    x=mdates.date2num(s.index.to_pydatetime())
    fig,ax=plt.subplots(figsize=(11,5))
    if len(s)==0:
        ax.text(0.5,0.5,"No data",ha="center",va="center",transform=ax.transAxes)
    else:
        ax.bar(x, s.values, width=26, align="center", label="Items per month")
        if len(x)==1: ax.set_xlim(x[0]-20, x[0]+20)
    ax.set_title("All sources : Posts per month")
    ax.set_xlabel("Month"); ax.set_ylabel("Items")
    ax.xaxis.set_major_locator(mdates.YearLocator()); ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    plt.tight_layout(); out_path.parent.mkdir(parents=True, exist_ok=True); plt.savefig(out_path,dpi=150); plt.close()

def plot_rolling_avg(df, out_path:Path, window_days:int):
    s_daily=df.set_index("published_at").assign(count=1)["count"].resample("D").sum()
    roll=s_daily.rolling(window=f"{window_days}D", min_periods=max(5, window_days//6)).mean()
    fig,ax=plt.subplots(figsize=(11,5))
    if len(roll)==0:
        ax.text(0.5,0.5,"No data",ha="center",va="center",transform=ax.transAxes)
    else:
        roll.plot(ax=ax)
        if len(roll.index)==1:
            x0=mdates.date2num(roll.index[0].to_pydatetime()); ax.set_xlim(x0-20, x0+20)
    ax.set_title(f"All sources : Rolling average posts/day ({window_days}-day)")
    ax.set_xlabel("Date"); ax.set_ylabel("Items per day (avg)")
    plt.tight_layout(); out_path.parent.mkdir(parents=True, exist_ok=True); plt.savefig(out_path,dpi=150); plt.close()

def clean_text(s:str)->str:
    s=s.lower()
    s=re.sub(r"[‘’´`']"," ",s)
    s=re.sub(r"[^a-z0-9\s\-\.]"," ",s)
    s=re.sub(r"\b\d{1,4}\b"," ",s)
    s=re.sub(r"\s+"," ",s).strip()
    return s

def render_wordcloud(df, out_path:Path, stopwords=None, width=1600, height=900):
    titles=[str(t) for t in df["title"].fillna("") if str(t).strip()]
    cleaned=[clean_text(t) for t in titles]
    sw=set(stopwords or [])
    tokens=[]
    for line in cleaned:
        for w in line.split():
            if w in sw or len(w)<3: continue
            tokens.append(w)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if not tokens:
        logging.info("No tokens for global wordcloud; writing placeholder")
        fig,ax=plt.subplots(figsize=(16,9)); ax.axis("off")
        ax.text(0.5,0.5,"No words available",ha="center",va="center",fontsize=20,transform=ax.transAxes)
        plt.tight_layout(); plt.savefig(out_path,dpi=150); plt.close(); return
    text=" ".join(tokens)
    wc=WordCloud(width=width,height=height,background_color="white",
                 stopwords=sw,collocations=True,prefer_horizontal=0.9).generate(text)
    plt.figure(figsize=(width/100, height/100)); plt.imshow(wc, interpolation="bilinear")
    plt.axis("off"); plt.tight_layout(pad=0); plt.savefig(out_path,dpi=150); plt.close()

def outputs_uptodate(conn, paths:list[Path])->bool:
    latest=latest_item_timestamp(conn)
    if latest is None: return False
    if not all(p.exists() for p in paths): return False
    return min(p.stat().st_mtime for p in paths) >= latest.timestamp()

def main():
    ap=argparse.ArgumentParser(description="Render aggregate charts + wordcloud across all enabled sources.")
    ap.add_argument("--rolling-days", type=int, default=90)
    ap.add_argument("--only-wordcloud", action="store_true", help="Render only the global wordcloud")
    ap.add_argument("--log-level", default="INFO")
    args=ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(levelname)s: %(message)s")
    load_env(".env"); conn=get_conn()
    try:
        OUTDIR.mkdir(parents=True, exist_ok=True)
        out_month=OUTDIR/f"monthly_bars_{SLUG}.png"
        out_roll =OUTDIR/f"rolling_avg_{args.rolling_days}d_{SLUG}.png"
        out_wc   =OUTDIR/f"wordcloud_{SLUG}.png"

        # freshness check respects --only-wordcloud
        check_paths=[out_wc] if args.only_wordcloud else [out_month,out_roll,out_wc]
        if outputs_uptodate(conn, check_paths):
            logging.info("Global asset(s) up-to-date; nothing to do.")
            return

        df=fetch_all_items_df(conn)
        if args.only_wordcloud:
            render_wordcloud(df, out_wc, stopwords=DEFAULT_STOPWORDS)
            logging.info("Rendered global wordcloud into %s", OUTDIR)
            return

        if df.empty:
            logging.info("No items in DB; writing placeholders.")
            fig,ax=plt.subplots(figsize=(11,5)); ax.text(0.5,0.5,"No data",ha="center",va="center",transform=ax.transAxes)
            plt.savefig(out_month,dpi=150); plt.close()
            fig,ax=plt.subplots(figsize=(11,5)); ax.text(0.5,0.5,"No data",ha="center",va="center",transform=ax.transAxes)
            plt.savefig(out_roll,dpi=150); plt.close()
            render_wordcloud(df, out_wc, stopwords=DEFAULT_STOPWORDS); return

        plot_monthly_bars(df, out_month)
        plot_rolling_avg(df, out_roll, window_days=args.rolling_days)
        render_wordcloud(df, out_wc, stopwords=DEFAULT_STOPWORDS)
        logging.info("Rendered global assets into %s", OUTDIR)
    finally:
        conn.close()

if __name__=="__main__":
    main()

