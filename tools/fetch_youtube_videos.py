#!/usr/bin/env python3
"""
Fetch YouTube videos for Source(kind='YouTube').

- Pulls uploads playlist (unless --playlists-only)
- Pulls all channel playlists (unless --uploads-only)
- Upserts into YouTubeVideo (one row per video)
- Robust to 404 playlistNotFound and continues to next source

Env required:
  DB_HOST, DB_USER, DB_PASSWORD, DB_NAME
  YT_API_KEY  (YouTube Data API v3)
"""
import os, re, json, time, argparse, logging
import urllib.parse, urllib.request, urllib.error
from datetime import datetime
import pymysql

API_BASE = "https://www.googleapis.com/youtube/v3"

UA_HEADERS = {
    "User-Agent": "ukgovcomms/yt-fetch (Python urllib)",
    "Accept-Language": "en-GB,en;q=0.9",
}

def env(k, default=None): return os.environ.get(k, default)

# ---------- DB ----------
def connect_db():
    return pymysql.connect(
        host=env("DB_HOST"),
        user=env("DB_USER"),
        password=env("DB_PASSWORD"),
        database=env("DB_NAME"),
        charset="utf8mb4",
        autocommit=True
    )

# ---------- HTTP / API ----------
def http_get_json(url, params=None, timeout=30):
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers=UA_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", "ignore"))
    except urllib.error.HTTPError as e:
        body = ""
        try: body = e.read().decode("utf-8", "ignore")
        except Exception: pass
        raise RuntimeError(f"HTTP {e.code} {e.reason}\nURL: {url}\n{body}")

def yt_get(endpoint, **params):
    key = env("YT_API_KEY")
    if not key:
        raise RuntimeError("YT_API_KEY not set in environment")
    # Strip empty/None params (e.g., pageToken=None)
    params = {k: v for k, v in params.items() if v not in (None, "", "None")}
    params["key"] = key
    return http_get_json(f"{API_BASE}/{endpoint}", params=params)

# ---------- utils ----------
def parse_rfc3339(s):
    if not s: return None
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

def parse_iso8601_duration_to_seconds(dur):
    if not dur or not dur.startswith("P"): return None
    days=hours=minutes=seconds=0
    m = re.match(r"P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?$", dur)
    if not m: return None
    if m.group("days"):    days    = int(m.group("days"))
    if m.group("hours"):   hours   = int(m.group("hours"))
    if m.group("minutes"): minutes = int(m.group("minutes"))
    if m.group("seconds"): seconds = int(m.group("seconds"))
    return days*86400 + hours*3600 + minutes*60 + seconds

def chunks(iterable, size):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf; buf = []
    if buf: yield buf

# ---------- discovery ----------
def ensure_channel_id(cur, source):
    sid, name, url, channel_id = source
    if channel_id: return channel_id
    m = re.search(r"/channel/(UC[0-9A-Za-z_-]{10,})", url or "")
    if m:
        channel_id = m.group(1)
        cur.execute("UPDATE Source SET channel_id=%s WHERE id=%s", (channel_id, sid))
        return channel_id
    raise RuntimeError(f"Missing channel_id for Source.id={sid} name={name}")

def get_uploads_playlist_id(channel_id):
    j = yt_get("channels", part="contentDetails", id=channel_id, maxResults=1)
    items = j.get("items") or []
    if not items: return None
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

def _is_playlist_not_found(err_text:str) -> bool:
    # Matches API 404 body with reason playlistNotFound
    return ("playlistNotFound" in err_text) or ("HTTP 404" in err_text and "playlistItems" in err_text)

def iter_playlist_video_ids(playlist_id, sleep=0.0, max_items=None):
    """Yield videoIds from a playlist, tolerating 404 playlistNotFound."""
    token = None
    total = 0
    while True:
        try:
            kwargs = dict(part="contentDetails", playlistId=playlist_id, maxResults=50)
            if token: kwargs["pageToken"] = token
            j = yt_get("playlistItems", **kwargs)
        except RuntimeError as e:
            msg = str(e)
            if _is_playlist_not_found(msg):
                logging.warning(f"Playlist not found, skipping: {playlist_id}")
                return
            logging.error(f"playlistItems error for {playlist_id}: {msg}")
            return
        for it in j.get("items", []):
            vid = (it.get("contentDetails") or {}).get("videoId")
            if vid:
                yield vid
                total += 1
                if max_items and total >= max_items:
                    return
        token = j.get("nextPageToken")
        if not token: break
        if sleep: time.sleep(sleep)

def iter_channel_playlists(channel_id, sleep=0.0, max_lists=None):
    """Yield playlist IDs for a channel, tolerating 4xx errors."""
    token = None
    seen = 0
    while True:
        try:
            kwargs = dict(part="id,snippet", channelId=channel_id, maxResults=50)
            if token: kwargs["pageToken"] = token
            j = yt_get("playlists", **kwargs)
        except RuntimeError as e:
            logging.error(f"playlists error for channel {channel_id}: {e}")
            return
        for it in j.get("items", []):
            pid = it.get("id")
            if pid:
                yield pid
                seen += 1
                if max_lists and seen >= max_lists: return
        token = j.get("nextPageToken")
        if not token: break
        if sleep: time.sleep(sleep)

def fetch_videos_metadata(video_ids):
    meta = {}
    for batch in chunks(video_ids, 50):
        j = yt_get(
            "videos",
            part="snippet,contentDetails,status,statistics,liveStreamingDetails",
            id=",".join(batch),
            maxResults=50
        )
        for it in j.get("items", []):
            vid = it.get("id");  snippet = it.get("snippet", {}) or {}
            content = it.get("contentDetails", {}) or {}
            status  = it.get("status", {}) or {}
            stats   = it.get("statistics", {}) or {}
            if not vid: continue
            meta[vid] = {
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "published_at": parse_rfc3339(snippet.get("publishedAt")),
                "duration_seconds": parse_iso8601_duration_to_seconds(content.get("duration")),
                "privacy_status": status.get("privacyStatus") or "unknown",
                "live_status": (snippet.get("liveBroadcastContent") or "none"),
                "view_count": int(stats["viewCount"]) if "viewCount" in stats else None,
                "like_count": int(stats["likeCount"]) if "likeCount" in stats else None,
                "comment_count": int(stats["commentCount"]) if "commentCount" in stats else None,
            }
    return meta

# ---------- persistence ----------
UPSERT_SQL = """
INSERT INTO YouTubeVideo
(source_id, channel_id, video_id, title, description, published_at, duration_seconds,
 privacy_status, live_status, discovered_via, playlist_id, view_count, like_count, comment_count, last_seen)
VALUES
(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, CURRENT_TIMESTAMP)
ON DUPLICATE KEY UPDATE
 title=VALUES(title),
 description=VALUES(description),
 published_at=COALESCE(VALUES(published_at), published_at),
 duration_seconds=VALUES(duration_seconds),
 privacy_status=VALUES(privacy_status),
 live_status=VALUES(live_status),
 discovered_via=COALESCE(discovered_via, VALUES(discovered_via)),
 playlist_id=COALESCE(playlist_id, VALUES(playlist_id)),
 view_count=VALUES(view_count),
 like_count=VALUES(like_count),
 comment_count=VALUES(comment_count),
 last_seen=CURRENT_TIMESTAMP
"""

def store_videos(conn, source_id, channel_id, infos, discovered_via, playlist_id, dry_run=False):
    if not infos: return 0
    rows = 0
    with conn.cursor() as cur:
        for vid, meta in infos.items():
            params = (
                source_id, channel_id, vid,
                meta.get("title"), meta.get("description"),
                meta.get("published_at"),
                meta.get("duration_seconds"),
                meta.get("privacy_status"),
                meta.get("live_status"),
                discovered_via, playlist_id,
                meta.get("view_count"),
                meta.get("like_count"),
                meta.get("comment_count"),
            )
            if dry_run:
                rows += 1
            else:
                cur.execute(UPSERT_SQL, params)
                rows += cur.rowcount
    return rows

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Fetch YouTube videos for Source(kind='YouTube').")
    target = ap.add_mutually_exclusive_group(required=False)
    target.add_argument("--only-source-id", type=int, help="Process a single Source by id")
    target.add_argument("--only-name", help="Process a single Source by name (exact match)")
    target.add_argument("--only-channel-id", help="Process by channel id (UC...)")
    target.add_argument("--only-url", help="Process by Source.url (exact match)")
    ap.add_argument("--max", type=int, default=None, help="Limit total videos fetched per source")
    ap.add_argument("--since", help="Only keep videos on/after YYYY-MM-DD (post-filter)")
    ap.add_argument("--uploads-only", action="store_true", help="Fetch uploads only (skip playlists)")
    ap.add_argument("--playlists-only", action="store_true", help="Fetch playlists only (skip uploads)")
    ap.add_argument("--playlists-limit", type=int, default=None, help="Limit number of playlists per channel")
    ap.add_argument("--sleep", type=float, default=0.2, help="Sleep between paginated API calls")
    ap.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    args = ap.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    if args.uploads_only and args.playlists_only:
        ap.error("Cannot use --uploads-only AND --playlists-only together.")

    since_dt = None
    if args.since:
        since_dt = datetime.strptime(args.since, "%Y-%m-%d")

    conn = connect_db()
    try:
        with conn.cursor() as cur:
            sql = "SELECT id,name,url,channel_id FROM Source WHERE kind='YouTube' AND is_enabled=1"
            params = []
            if args.only_source_id:  sql += " AND id=%s";         params.append(args.only_source_id)
            if args.only_name:       sql += " AND name=%s";       params.append(args.only_name)
            if args.only_channel_id: sql += " AND channel_id=%s"; params.append(args.only_channel_id)
            if args.only_url:        sql += " AND url=%s";        params.append(args.only_url)
            cur.execute(sql, params)
            sources = cur.fetchall()

        if not sources:
            logging.info("No matching YouTube sources."); return

        total_written = 0

        for sid, name, url, channel_id in sources:
            logging.info(f"Source #{sid} | {name}")
            try:
                with conn.cursor() as cur:
                    channel_id = ensure_channel_id(cur, (sid, name, url, channel_id))

                video_ids = []
                seen = set()

                # 1) Uploads
                if not args.playlists_only:
                    uploads_pid = get_uploads_playlist_id(channel_id)
                    if uploads_pid:
                        for vid in iter_playlist_video_ids(uploads_pid, sleep=args.sleep, max_items=args.max):
                            if vid not in seen:
                                seen.add(vid); video_ids.append(("channel", None, vid))
                    else:
                        logging.warning("No uploads playlist found")

                # 2) Playlists
                if not args.uploads_only:
                    count_lists = 0
                    for pid in iter_channel_playlists(channel_id, sleep=args.sleep, max_lists=args.playlists_limit):
                        count_lists += 1
                        for vid in iter_playlist_video_ids(pid, sleep=args.sleep, max_items=None):
                            if args.max and len(seen) >= args.max: break
                            if vid not in seen:
                                seen.add(vid); video_ids.append(("playlist:"+pid, pid, vid))
                        if args.max and len(seen) >= args.max: break
                    logging.info(f"Playlists scanned: {count_lists}")

                if not video_ids:
                    logging.info("No videos discovered."); continue

                ordered_vids = [vid for _,__,vid in video_ids]
                meta = fetch_videos_metadata(ordered_vids)

                if since_dt:
                    meta = {vid: m for vid, m in meta.items()
                            if (m.get("published_at") and m["published_at"].date() >= since_dt.date())}

                written = 0
                # channel-discovered
                chan_ids = [vid for (via, pid, vid) in video_ids if via == "channel" and vid in meta]
                infos = {vid: meta[vid] for vid in chan_ids}
                written += store_videos(conn, sid, channel_id, infos, "channel", None, dry_run=args.dry_run)

                # playlist-discovered
                pl_ids = [(pid, vid) for (via, pid, vid) in video_ids if via.startswith("playlist:") and vid in meta]
                grouped = {}
                for pid, vid in pl_ids: grouped.setdefault(pid, []).append(vid)
                for pid, vids in grouped.items():
                    infos = {vid: meta[vid] for vid in vids}
                    written += store_videos(conn, sid, channel_id, infos, f"playlist:{pid}", pid, dry_run=args.dry_run)

                logging.info(f"Wrote/updated rows: {written}")
                total_written += written

            except Exception as e:
                logging.error(f"Failed for {name}: {e}")
                continue

        logging.info(f"All done. Total rows written/updated: {total_written}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()

