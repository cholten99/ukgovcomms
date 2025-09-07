#!/usr/bin/env python3
import os, re, argparse, logging
from pathlib import Path
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
    "what","how",   # <-- added
}
SUPPORTED_KINDS = {"Blog","YouTube"}

def load_env(path=".env"):
    if not os.path.exists(path): return
    for line in open(path, "r", encoding="utf-8"):
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
    return re.sub(r"[^a-zA-Z0-9]+","-",(name or "").strip()).strip("-").lower() or "source"

def fetch_source(conn, id=None, host=None)->dict:
    sql="SELECT id,name,url,kind FROM Source WHERE is_enabled=1"
    params=[]
    if id is not None:
        sql+=" AND id=%s"; params.append(id)
    elif host:
        sql+=" AND SUBSTRING_INDEX(SUBSTRING_INDEX(url,'/',3),'/',-1)=%s"; params.append(host.lower())
    else:
        raise ValueError("Provide --id or --host")
    sql+=" LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql, params); row=cur.fetchone()
    if not row: raise RuntimeError("Source not found.")
    sid,name,url,kind=row
    if kind not in SUPPORTED_KINDS: raise RuntimeError(f"Unsupported kind '{kind}'")
    return {"id":sid,"name":name,"url":url,"kind":kind}

def host_from_url(url:str)->str:
    return url.split("//",1)[-1].split("/",1)[0].lower()

def fetch_items_df(conn, src)->pd.DataFrame:
    if src["kind"]=="Blog":
        name=src["name"]; host=host_from_url(src["url"])
        sql_name="""
          SELECT title, published_at
          FROM BlogPost
          WHERE blog_name=%s
          ORDER BY published_at ASC"""
        sql_host="""
          SELECT title, published_at
          FROM BlogPost
          WHERE url LIKE CONCAT('https://', %s, '/%%')
          ORDER BY published_at ASC"""
        df_name=pd.read_sql(sql_name, conn, params=[name])
        df_host=pd.read_sql(sql_host, conn, params=[host])
        df = df_name if len(df_name)>=len(df_host) else df_host
    elif src["kind"]=="YouTube":
        sql="""
          SELECT title, published_at
          FROM YouTubeVideo
          WHERE source_id=%s
          ORDER BY published_at ASC"""
        df=pd.read_sql(sql, conn, params=[src["id"]])
    else:
        raise RuntimeError(f"Unsupported kind: {src['kind']}")
    df["published_at"]=pd.to_datetime(df["published_at"], errors="coerce")
    df=df.dropna(subset=["published_at"]).sort_values("published_at")
    if df.empty: raise RuntimeError("No items found for this source.")
    return df

def compute_summary(df):
    return df["published_at"].min(), df["published_at"].max(), len(df)

def add_summary(ax, first_dt, last_dt, total):
    ax.text(0.99,0.98,f"First: {first_dt.strftime('%Y-%m-%d')}  |  Last: {last_dt.strftime('%Y-%m-%d')}  |  Total: {total}",
            transform=ax.transAxes,ha="right",va="top",fontsize=9,
            bbox=dict(boxstyle="round",facecolor="white",alpha=0.7,edgecolor="none"))

def plot_monthly_bars(df, out_path:Path, title:str):
    s=df.set_index("published_at").assign(count=1)["count"].resample("MS").sum()
    x=mdates.date2num(s.index.to_pydatetime())
    fig,ax=plt.subplots(figsize=(11,5))
    if len(s)==0:
        ax.text(0.5,0.5,"No data",ha="center",va="center",transform=ax.transAxes)
    else:
        ax.bar(x, s.values, width=26, align="center", label="Items per month")
        if len(x)==1: ax.set_xlim(x[0]-20, x[0]+20)
    ax.set_title(title); ax.set_xlabel("Month"); ax.set_ylabel("Items")
    ax.xaxis.set_major_locator(mdates.YearLocator()); ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    fig.autofmt_xdate(rotation=45)
    first_dt,last_dt,total=compute_summary(df); add_summary(ax, first_dt, last_dt, total)
    if len(s)>0: ax.legend()
    plt.tight_layout(); out_path.parent.mkdir(parents=True, exist_ok=True); plt.savefig(out_path,dpi=150); plt.close()

def plot_rolling_avg(df, out_path:Path, title:str, window_days:int=90):
    s_daily=df.set_index("published_at").assign(count=1)["count"].resample("D").sum()
    roll=s_daily.rolling(window=f"{window_days}D", min_periods=max(5, window_days//6)).mean()
    fig,ax=plt.subplots(figsize=(11,5))
    if len(roll)==0:
        ax.text(0.5,0.5,"No data",ha="center",va="center",transform=ax.transAxes)
    else:
        roll.plot(ax=ax)
        if len(roll.index)==1:
            x0=mdates.date2num(roll.index[0].to_pydatetime()); ax.set_xlim(x0-20, x0+20)
    ax.set_title(title); ax.set_xlabel("Date"); ax.set_ylabel("Items per day (avg)")
    first_dt,last_dt,total=compute_summary(df); add_summary(ax, first_dt, last_dt, total)
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
        logging.info("No tokens for wordcloud; writing placeholder: %s", out_path)
        fig,ax=plt.subplots(figsize=(16,9)); ax.axis("off")
        ax.text(0.5,0.5,"No words available",ha="center",va="center",fontsize=20,transform=ax.transAxes)
        plt.tight_layout(); plt.savefig(out_path,dpi=150); plt.close(); return
    text=" ".join(tokens)
    wc=WordCloud(width=width,height=height,background_color="white",
                 stopwords=sw,collocations=True,prefer_horizontal=0.9).generate(text)
    plt.figure(figsize=(width/100, height/100)); plt.imshow(wc, interpolation="bilinear")
    plt.axis("off"); plt.tight_layout(pad=0); plt.savefig(out_path,dpi=150); plt.close()

def main():
    ap=argparse.ArgumentParser(description="Render charts + wordcloud for a single Source.")
    ap.add_argument("--id", type=int); ap.add_argument("--host")
    ap.add_argument("--outdir", default="assets/sources")
    ap.add_argument("--rolling-days", type=int, default=90)
    ap.add_argument("--only-wordcloud", action="store_true", help="Render only the wordcloud (skip charts)")
    ap.add_argument("--log-level", default="INFO")
    args=ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(levelname)s: %(message)s")
    load_env(".env"); conn=get_conn()
    try:
        src=fetch_source(conn, id=args.id, host=args.host)
        df=fetch_items_df(conn, src)
        slug=slugify(src["name"]); base=Path(args.outdir)/slug

        if args.only_wordcloud:
            render_wordcloud(df, base/f"wordcloud_{slug}.png", stopwords=DEFAULT_STOPWORDS)
            logging.info("Rendered wordcloud for %s into %s", src["name"], base)
            return

        plot_monthly_bars(df, base/f"monthly_bars_{slug}.png", title=f"{src['name']} : Posts per month")
        plot_rolling_avg(df, base/f"rolling_avg_{args.rolling_days}d_{slug}.png",
                         title=f"{src['name']} : Rolling average posts/day ({args.rolling_days}-day)",
                         window_days=args.rolling_days)
        render_wordcloud(df, base/f"wordcloud_{slug}.png", stopwords=DEFAULT_STOPWORDS)
        logging.info("Rendered assets for %s into %s", src["name"], base)
    finally:
        conn.close()

if __name__=="__main__":
    main()

