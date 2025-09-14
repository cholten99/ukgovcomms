#!/usr/bin/env python3
import os, re, json, time, urllib.parse, urllib.request, urllib.error, pymysql

API_KEY = os.environ.get("YT_API_KEY")

# Use a realistic browser UA to improve HTML fallback reliability
UA = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept-Language": "en-GB,en;q=0.9"
}

def q(s): return urllib.parse.quote_plus(s)

def http_get(url, timeout=20):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def get_json(url, timeout=20):
    try:
        data = http_get(url, timeout=timeout)
        return json.loads(data.decode("utf-8", "ignore"))
    except urllib.error.HTTPError as e:
        body = ""
        try: body = e.read().decode("utf-8", "ignore")
        except Exception: pass
        raise RuntimeError(f"HTTPError {e.code} for URL:\n{url}\nResponse:\n{body}")

def connect():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        charset="utf8mb4",
        autocommit=True
    )

def extract_hint_from_url(url:str):
    """Return dict possibly containing: channel_id | handle | username"""
    m = re.search(r"/channel/(UC[0-9A-Za-z_-]{10,})", url)
    if m: return {"channel_id": m.group(1)}
    m = re.search(r"youtube\.com/@([^/?#]+)", url)
    if m: return {"handle": m.group(1)}
    m = re.search(r"youtube\.com/user/([^/?#]+)", url)
    if m: return {"username": m.group(1)}
    return {}

# ---------- API helpers ----------

def resolve_via_handle_api(handle:str):
    # Try with and without '@' — YouTube sometimes prefers one or the other
    tries = [handle.lstrip('@'), '@' + handle.lstrip('@')]
    for term in tries:
        url = f"https://www.googleapis.com/youtube/v3/channels?part=id&forHandle={q(term)}&key={API_KEY}"
        j = get_json(url)
        items = j.get("items") or []
        if items:
            return items[0]["id"]
    return None

def resolve_via_username_api(username:str):
    url = f"https://www.googleapis.com/youtube/v3/channels?part=id&forUsername={q(username)}&key={API_KEY}"
    j = get_json(url)
    items = j.get("items") or []
    return items[0]["id"] if items else None

def fallback_search_channel_api(term:str):
    # CORRECT FIELD: items[].id.channelId
    url = f"https://www.googleapis.com/youtube/v3/search?part=id&type=channel&maxResults=1&q={q(term)}&key={API_KEY}"
    j = get_json(url)
    items = j.get("items") or []
    if items and "id" in items[0] and "channelId" in items[0]["id"]:
        return items[0]["id"]["channelId"]
    return None

# ---------- HTML fallback ----------

CID_RE = re.compile(r'"channelId"\s*:\s*"(UC[0-9A-Za-z_-]{10,})"')

def resolve_via_html(urls):
    for u in urls:
        try:
            html = http_get(u, timeout=20).decode("utf-8", "ignore")
            m = CID_RE.search(html)
            if m:
                return m.group(1)
        except Exception:
            pass
    return None

# ---------- main resolver ----------

def resolve_channel_id(url, name):
    hint = extract_hint_from_url(url)
    # 1) UC directly in URL
    if "channel_id" in hint:
        return hint["channel_id"]

    html_urls = [url]  # always try the provided URL too

    if "handle" in hint:
        handle = hint["handle"].strip()
        h = handle.lstrip('@')
        html_urls += [
            f"https://www.youtube.com/@{h}",
            f"https://www.youtube.com/@{h}/about",
            f"https://www.youtube.com/@{h}/videos",
        ]
        if API_KEY:
            cid = resolve_via_handle_api(h)
            if cid: return cid

    elif "username" in hint:
        uname = hint["username"]
        html_urls += [
            f"https://www.youtube.com/user/{uname}",
            f"https://www.youtube.com/user/{uname}/about",
            f"https://www.youtube.com/user/{uname}/videos",
        ]
        if API_KEY:
            cid = resolve_via_username_api(uname)
            if cid: return cid

    else:
        # no hint: guess a handle-ish token from the name (HTML only)
        guess = re.sub(r'[^A-Za-z0-9]+', '', name).lower()
        html_urls += [
            f"https://www.youtube.com/@{guess}",
            f"https://www.youtube.com/@{guess}/about",
        ]

    # HTML (quota-free)
    cid = resolve_via_html(html_urls)
    if cid:
        return cid

    # Last resort: API search by likely terms
    if API_KEY:
        terms = []
        if "handle" in hint:   terms.append(hint["handle"].lstrip('@'))
        if "username" in hint: terms.append(hint["username"])
        terms.append(name)
        for term in terms:
            cid = fallback_search_channel_api(term)
            if cid: return cid

    return None

# ---------- DB driver ----------

def main():
    if not (os.getenv("DB_HOST") and os.getenv("DB_USER") and os.getenv("DB_PASSWORD") and os.getenv("DB_NAME")):
        raise SystemExit("Set DB_* env vars (DB_HOST, DB_USER, DB_PASSWORD, DB_NAME)")
    if not API_KEY:
        print("[WARN] YT_API_KEY not set; will rely only on HTML fallbacks.", flush=True)

    conn = connect()
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id,name,url
            FROM Source
            WHERE kind='YouTube' AND (channel_id IS NULL OR channel_id='')
        """)
        rows = cur.fetchall()
        for sid, name, url in rows:
            try:
                cid = resolve_channel_id(url, name)
                if cid:
                    cur.execute("UPDATE Source SET channel_id=%s WHERE id=%s", (cid, sid))
                    print(f"[OK] {name}: {cid}")
                else:
                    print(f"[WARN] {name}: could not resolve (url={url})")
            except Exception as e:
                print(f"[ERR]  {name}: exception -> {e}")

if __name__ == "__main__":
    main()

