#!/usr/bin/env python3
"""
Fetch YouTube videos for Source(kind='YouTube') and upsert into YouTubeVideo.
Shorts detection uses ONLY:
  1) '#shorts' marker in title/description (case-insensitive)
  2) Provenance from the Shorts playlist (UUSH... derived from channel_id)
  3) Optional HTTP check against https://www.youtube.com/shorts/{VIDEO_ID}

NO duration-based heuristic is used.

ENV required:
  DB_HOST, DB_USER, DB_PASSWORD, DB_NAME
  YT_API_KEY  (YouTube Data API v3)
(.env is loaded automatically if present.)

CLI examples:
  python tools/fetch_youtube_videos.py --log-level INFO
  python tools/fetch_youtube_videos.py --only-channel-id UCxxxx --include-shorts-playlist --shorts-http-check
  python tools/fetch_youtube_videos.py --playlists-limit 10 --sleep 0.2
"""

import os
import re
import json
import time
import argparse
import logging
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime
from contextlib import closing

# Load .env if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import mysql.connector

# ---------- Config ----------

API_KEY = os.getenv("YT_API_KEY")
YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
USER_AGENT = "Mozilla/5.0 (UKGovComms/yt-fetcher)"

UPSERT_SQL = """
INSERT INTO YouTubeVideo
(source_id, channel_id, video_id, title, description, published_at, duration_seconds,
 privacy_status, live_status, discovered_via, playlist_id, view_count, like_count, comment_count, is_short, last_seen)
VALUES
(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, CURRENT_TIMESTAMP)
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
 is_short=VALUES(is_short),
 last_seen=CURRENT_TIMESTAMP
"""

# ---------- DB / HTTP helpers ----------

def connect_db():
    host = os.getenv("DB_HOST", "localhost")
    name = os.getenv("DB_NAME", "UKGovComms")
    user = os.getenv("DB_USER")
    pwd  = os.getenv("DB_PASSWORD")
    if not (user and pwd and name):
        raise RuntimeError("Missing DB env vars. Need DB_HOST, DB_USER, DB_PASSWORD, DB_NAME (.env supported).")
    return mysql.connector.connect(
        host=host, database=name, user=user, password=pwd, autocommit=True
    )

class NoRedirect(urllib.request.HTTPErrorProcessor):
    def http_response(self, request, response): return response
    https_response = http_response

def http_request(url, headers=None, method="GET", timeout=20, follow_redirects=True):
    req = urllib.request.Request(url, headers=headers or {}, method=method)
    opener = urllib.request.build_opener() if follow_redirects else urllib.request.build_opener(NoRedirect)
    return opener.open(req, timeout=timeout)

def http_json(url, params, sleep=None, context=""):
    if API_KEY is None:
        raise RuntimeError("YT_API_KEY not set")
    q = dict(params or {})
    q["key"] = API_KEY
    full = f"{url}?{urllib.parse.urlencode(q)}"
    try:
        resp = http_request(full, headers={"User-Agent": USER_AGENT})
        data = resp.read()
        if sleep:
            time.sleep(sleep)
        return json.loads(data.decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        logging.error("YouTube API %s error %s: %s", context or url, e.code, body[:500])
        raise
    except Exception as e:
        logging.error("HTTP error for %s: %s", context or url, e)
        raise

def yt_get(endpoint, **params):
    url = f"{YOUTUBE_API_BASE}/{endpoint}"
    return http_json(url, params, sleep=None, context=endpoint)

def parse_rfc3339(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def parse_iso8601_duration_to_seconds(dur):
    if not dur or not dur.startswith("P"):
        return None
    days = hours = minutes = seconds = 0
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
            yield buf
            buf = []
    if buf:
        yield buf

# ---------- Shorts detection (no duration heuristic) ----------

def is_short_via_http(video_id):
    """HEAD /shorts/{id} without following redirects; 2xx -> treat as Short."""
    try:
        resp = http_request(
            f"https://www.youtube.com/shorts/{video_id}",
            headers={"User-Agent": USER_AGENT},
            method="HEAD",
            timeout=6,
            follow_redirects=False
        )
        return 200 <= resp.getcode() < 300
    except Exception:
        return False

def compute_is_short(meta, discovered_via=None, use_http_check=False):
    # 1) Provenance from Shorts playlist
    if discovered_via and str(discovered_via).startswith("shorts:"):
        return 1
    # 2) Marker in text
    t = ((meta.get("title") or "") + " " + (meta.get("description") or "")).lower()
    if "#shorts" in t:
        return 1
    # 3) Optional HTTP check
    if use_http_check and meta.get("video_id"):
        return 1 if is_short_via_http(meta["video_id"]) else 0
    return 0

# ---------- YouTube helpers ----------

def get_uploads_playlist_id(channel_id):
    j = yt_get("channels", part="contentDetails", id=channel_id, maxResults=1)
    items = j.get("items") or []
    if not items: return None
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

def channel_shorts_playlist_id(channel_id):
    """UUSH + channel_id[2:] heuristic for Shorts playlist (undocumented)."""
    if not channel_id or not channel_id.startswith("UC") or len(channel_id) < 3:
        return None
    return "UUSH" + channel_id[2:]

def iter_channel_playlists(channel_id, sleep=0.2, max_lists=None):
    token = None; seen = 0
    while True:
        j = yt_get("playlists", part="snippet", channelId=channel_id, maxResults=50, pageToken=token or "")
        for it in j.get("items", []):
            pid = it.get("id")
            if pid:
                yield pid
                seen += 1
                if max_lists and seen >= max_lists: return
        token = j.get("nextPageToken")
        if not token: break
        if sleep: time.sleep(sleep)

def iter_playlist_video_ids(playlist_id, sleep=0.2, max_items=None):
    token = None; count = 0
    while True:
        j = yt_get("playlistItems", part="contentDetails", playlistId=playlist_id, maxResults=50, pageToken=token or "")
        for it in j.get("items", []):
            vid = (it.get("contentDetails") or {}).get("videoId")
            if not vid: continue
            yield vid
            count += 1
            if max_items and count >= max_items: return
        token = j.get("nextPageToken")
        if not token: break
        if sleep: time.sleep(sleep)

def fetch_videos_metadata(video_ids):
    """Return dict[video_id] -> meta (title, description, published_at, duration_seconds, privacy_status, live_status, counts)."""
    meta = {}
    for batch in chunks(video_ids, 50):
        j = yt_get(
            "videos",
            part="snippet,contentDetails,status,statistics,liveStreamingDetails",
            id=",".join(batch),
            maxResults=50
        )
        for it in j.get("items", []):
            vid = it.get("id")
            snippet = it.get("snippet", {}) or {}
            content = it.get("contentDetails", {}) or {}
            status  = it.get("status", {}) or {}
            stats   = it.get("statistics", {}) or {}
            if not vid: continue
            meta[vid] = {
                "video_id": vid,
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

# ---------- Channel ID resolution ----------

HANDLE_RE = re.compile(r"/@([A-Za-z0-9._-]+)")
USER_RE   = re.compile(r"/user/([A-Za-z0-9._-]+)")
CHANNEL_RE= re.compile(r"/channel/(UC[0-9A-Za-z_-]{20,})")
CUSTOM_RE = re.compile(r"/c/([A-Za-z0-9._-]+)")

def channels_for_handle(handle: str):
    # channels.list forHandle is supported by Data API
    return yt_get("channels", part="id", forHandle=f"@{handle}", maxResults=1)

def channels_for_username(username: str):
    # channels.list forUsername still works for legacy usernames
    return yt_get("channels", part="id", forUsername=username, maxResults=1)

def search_channel_by_query(q: str):
    j = yt_get("search", part="snippet", type="channel", q=q, maxResults=1)
    items = j.get("items") or []
    if not items: return None
    return items[0]["id"]["channelId"]

def resolve_channel_id_via_url_or_name(name: str, url: str | None):
    """
    Resolve a UC... channel id from a Source url or, failing that, the Source name.
    Tries, in order: /channel/UC..., @handle, /user/USERNAME, /c/CUSTOM, search by name.
    """
    # 1) Direct UC id
    if url:
        m = CHANNEL_RE.search(url)
        if m:
            return m.group(1)

    # 2) @handle
    if url:
        m = HANDLE_RE.search(url)
        if m:
            handle = m.group(1)
            j = channels_for_handle(handle)
            items = j.get("items") or []
            if items:
                return items[0]["id"]

    # 3) /user/USERNAME
    if url:
        m = USER_RE.search(url)
        if m:
            username = m.group(1)
            j = channels_for_username(username)
            items = j.get("items") or []
            if items:
                return items[0]["id"]

    # 4) /c/CUSTOM — no direct endpoint; fall back to search
    if url:
        m = CUSTOM_RE.search(url)
        if m:
            cid = search_channel_by_query(m.group(1))
            if cid:
                return cid

    # 5) Fallback: search by the Source name
    cid = search_channel_by_query(name)
    if cid:
        return cid

    return None

# ---------- Persistence ----------

def ensure_channel_id(cur, source_row):
    """Ensure Source.channel_id is set (resolving via API if needed); return it."""
    sid, name, url, channel_id = source_row
    if channel_id:
        return channel_id
    resolved = resolve_channel_id_via_url_or_name(name, url)
    if resolved:
        cur.execute("UPDATE Source SET channel_id=%s WHERE id=%s", (resolved, sid))
        logging.info("Resolved channel id for Source[%s] '%s' -> %s", sid, name, resolved)
        return resolved
    raise RuntimeError(f"Could not resolve channel_id for Source.id={sid} name='{name}' url='{url}'")

def store_videos(conn, source_id, channel_id, infos, discovered_via, playlist_id, mark_shorts_http=False):
    """Upsert a dict of video_id -> meta into DB, computing is_short."""
    if not infos: return 0
    rows = 0
    with conn.cursor() as cur:
        for vid, meta in infos.items():
            meta["is_short"] = compute_is_short(meta, discovered_via=discovered_via, use_http_check=mark_shorts_http)
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
                meta.get("is_short"),
            )
            cur.execute(UPSERT_SQL, params)
            rows += cur.rowcount
    return rows

# ---------- Main ----------

def main():
    ap = argparse.ArgumentParser(description="Fetch YouTube videos for Source(kind='YouTube').")
    tgt = ap.add_mutually_exclusive_group(required=False)
    tgt.add_argument("--only-source-id", type=int, help="Process a single Source by id")
    tgt.add_argument("--only-name", help="Process a single Source by name (exact match)")
    tgt.add_argument("--only-channel-id", help="Process by channel id (UC...)")
    tgt.add_argument("--only-url", help="Process by Source.url (exact match)")
    ap.add_argument("--max", type=int, default=None, help="Limit total videos fetched per source")
    ap.add_argument("--since", help="Only keep videos on/after YYYY-MM-DD (post-filter)")
    ap.add_argument("--uploads-only", action="store_true", help="Fetch uploads only (skip playlists)")
    ap.add_argument("--playlists-only", action="store_true", help="Fetch playlists only (skip uploads)")
    ap.add_argument("--playlists-limit", type=int, default=None, help="Limit number of playlists per channel")
    ap.add_argument("--sleep", type=float, default=0.2, help="Sleep between paginated API calls")
    ap.add_argument("--include-shorts-playlist", action="store_true",
                    help="Also scan Shorts playlist (UUSH...) and mark discovered_via='shorts:<pid>'")
    ap.add_argument("--shorts-http-check", action="store_true",
                    help="Use an extra HTTP check against /shorts/{id} for ambiguous cases (slower)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    ap.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR"])
    args = ap.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")

    if args.uploads_only and args.playlists_only:
        ap.error("Cannot use --uploads-only AND --playlists-only together.")

    since_dt = None
    if args.since:
        since_dt = datetime.strptime(args.since, "%Y-%m-%d")

    conn = connect_db()
    total_written = 0
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

        for sid, name, url, channel_id in sources:
            try:
                logging.info("Processing Source[%s] %s", sid, name)
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
                        logging.warning("No uploads playlist found for %s", channel_id)

                # 2) Optional Shorts playlist (UUSH...)
                if args.include_shorts_playlist:
                    spid = channel_shorts_playlist_id(channel_id)
                    if spid:
                        for vid in iter_playlist_video_ids(spid, sleep=args.sleep, max_items=None):
                            if args.max and len(seen) >= args.max: break
                            if vid not in seen:
                                seen.add(vid); video_ids.append(("shorts:"+spid, spid, vid))

                # 3) Other playlists
                if not args.uploads_only:
                    count_lists = 0
                    for pid in iter_channel_playlists(channel_id, sleep=args.sleep, max_lists=args.playlists_limit):
                        count_lists += 1
                        for vid in iter_playlist_video_ids(pid, sleep=args.sleep, max_items=None):
                            if args.max and len(seen) >= args.max: break
                            if vid not in seen:
                                seen.add(vid); video_ids.append(("playlist:"+pid, pid, vid))
                        if args.max and len(seen) >= args.max: break
                    logging.info("Playlists scanned: %d", count_lists)

                if not video_ids:
                    logging.info("No videos discovered."); continue

                # Fetch metadata
                meta = fetch_videos_metadata([vid for _, _, vid in video_ids])

                # Optional post-filter by --since
                groups = {}
                for prov, pid, vid in video_ids:
                    m = meta.get(vid)
                    if not m: continue
                    if since_dt and m.get("published_at") and m["published_at"] < since_dt:
                        continue
                    groups.setdefault((prov, pid), []).append(vid)

                # Persist per provenance group
                for (prov, pid), vids in groups.items():
                    infos = {vid: meta[vid] for vid in vids if vid in meta}
                    if not infos: continue
                    if args.dry_run:
                        shorts = 0
                        for v in infos.values():
                            v["is_short"] = compute_is_short(v, discovered_via=prov, use_http_check=args.shorts_http_check)
                            shorts += v["is_short"]
                        logging.info("DRY-RUN: would upsert %d videos (%d shorts) via %s", len(infos), shorts, prov)
                        written = len(infos)
                    else:
                        written = store_videos(conn, sid, channel_id, infos, discovered_via=prov, playlist_id=pid, mark_shorts_http=args.shorts_http_check)
                    total_written += written

            except urllib.error.HTTPError as e:
                logging.error("Failed for %s: HTTP %s", name, e.code)
                continue
            except Exception as e:
                logging.error("Failed for %s: %s", name, e)
                continue

        logging.info("All done. Total rows written/updated: %d", total_written)

    finally:
        conn.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.", flush=True)
        raise

