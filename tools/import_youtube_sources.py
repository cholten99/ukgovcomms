#!/usr/bin/env python3
import os, csv, pymysql, argparse

def env(k): return os.environ.get(k)

def connect():
    return pymysql.connect(
        host=env("DB_HOST"), user=env("DB_USER"),
        password=env("DB_PASSWORD"), database=env("DB_NAME"),
        charset="utf8mb4", autocommit=True
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="CSV with columns: name,url,is_enabled,kind,channel_id")
    args = ap.parse_args()

    conn = connect()
    with conn, conn.cursor() as cur, open(args.csv, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            name = row["name"].strip()
            url  = row["url"].strip()
            kind = (row.get("kind") or "YouTube").strip()
            is_enabled = 1 if (row.get("is_enabled","1").strip() in ("1","true","TRUE","yes","y")) else 0
            channel_id = (row.get("channel_id") or "").strip() or None

            # upsert by URL
            cur.execute("SELECT id FROM Source WHERE url=%s", (url,))
            exists = cur.fetchone()
            if exists:
                cur.execute("""
                  UPDATE Source
                     SET name=%s, kind=%s, is_enabled=%s, channel_id=%s
                   WHERE url=%s
                """, (name, kind, is_enabled, channel_id, url))
            else:
                cur.execute("""
                  INSERT INTO Source (name,url,kind,is_enabled,channel_id)
                  VALUES (%s,%s,%s,%s,%s)
                """, (name, url, kind, is_enabled, channel_id))
    print("OK")

if __name__ == "__main__":
    main()

