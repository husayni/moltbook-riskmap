"""
Moltbook scraper: hot feed every 15 min, posts + incremental comments.
"""
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import create_client

# Load .env from project root
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

BASE_URL = "https://www.moltbook.com/api/v1"
RATE_DELAY = 1.1  # ~55 req/min under 60/min
INTERVAL_SEC = 15 * 60

project_ref = os.getenv("SUPABASE_PROJECT_REF")
service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
if not project_ref or not service_key:
    raise SystemExit("Set SUPABASE_PROJECT_REF and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) in .env")

supabase = create_client(
    f"https://{project_ref}.supabase.co",
    service_key,
)

# Verify Supabase connection
try:
    supabase.table("ingest_state").select("source").limit(1).execute()
except Exception as e:
    raise SystemExit(f"Supabase client failed: {e}") from e


def get(path: str, params: dict | None = None) -> dict:
    r = requests.get(f"{BASE_URL}{path}", params=params or {}, timeout=30)
    r.raise_for_status()
    time.sleep(RATE_DELAY)
    return r.json()


def flatten_comments(nodes: list, out: list | None = None) -> list:
    out = out or []
    for c in nodes or []:
        out.append(c)
        flatten_comments(c.get("replies") or c.get("children") or [], out)
    return out


def parse_author(obj: dict) -> str | None:
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        return obj.get("name") or obj.get("username")
    return None


def ensure_agent(name: str) -> None:
    if not name:
        return
    supabase.table("agents").upsert(
        {"name": name, "raw": {}},
        on_conflict="name",
    ).execute()


def run_feed() -> list[tuple[int, str, dict]]:
    """Fetch hot feed with pagination; return list of (rank, post_id, raw)."""
    items = []
    offset = None
    rank = 0
    while len(items) < 50:
        params = {"sort": "top", "limit": 50}
        if offset is not None:
            params["offset"] = offset
        data = get("/posts", params)
        posts = data.get("posts") or data.get("data") or []
        for p in posts:
            pid = p.get("id")
            if pid:
                rank += 1
                items.append((rank, pid, p))
                if rank >= 50:
                    break
        if rank >= 50 or not data.get("has_more"):
            break
        offset = data.get("next_offset")
        if offset is None:
            break
    return items[:50]


def row_post(post_id: str, data: dict) -> dict:
    author = parse_author(data.get("author") or data.get("author_name"))
    created = data.get("created_at")
    updated = data.get("updated_at")
    return {
        "id": post_id,
        "submolt_name": data.get("submolt_name"),
        "author_name": author,
        "title": data.get("title"),
        "content": data.get("content"),
        "url": data.get("url"),
        "upvotes": data.get("upvotes", 0) or 0,
        "downvotes": data.get("downvotes", 0) or 0,
        "comment_count": data.get("comment_count", 0) or 0,
        "last_comment_at": data.get("last_comment_at"),
        "created_at": created,
        "updated_at": updated,
        "raw": data,
    }


def row_comment(c: dict, post_id: str) -> dict:
    author = parse_author(c.get("author") or c.get("author_name"))
    return {
        "id": c.get("id"),
        "post_id": post_id,
        "parent_id": c.get("parent_id"),
        "author_name": author,
        "content": c.get("content"),
        "upvotes": c.get("upvotes", 0) or 0,
        "downvotes": c.get("downvotes", 0) or 0,
        "created_at": c.get("created_at"),
        "updated_at": c.get("updated_at"),
        "raw": c,
    }


def run_once() -> None:
    fetched_at = datetime.now(timezone.utc).isoformat()
    print(f"[{fetched_at}] Starting ingest...")
    feed_items = run_feed()
    print(f"  Fetched {len(feed_items)} posts from hot feed")

    for i, (rank, post_id, raw_item) in enumerate(feed_items, 1):
        print(f"  [{i}/{len(feed_items)}] Post {post_id[:8]}...", end=" ", flush=True)
        try:
            data = get(f"/posts/{post_id}")
        except Exception as e:
            print(f"skip: {e}")
            continue

        author = parse_author(data.get("author") or data.get("author_name"))
        if author:
            ensure_agent(author)

        post_row = row_post(post_id, data)
        supabase.table("posts").upsert(post_row, on_conflict="id").execute()

        comments_tree = data.get("comments") or data.get("comment_tree") or []
        flat = flatten_comments(comments_tree if isinstance(comments_tree, list) else [comments_tree])
        # Newest first typical; stop at first known id
        source = f"comments:{post_id}"
        state = supabase.table("ingest_state").select("last_seen_id").eq("source", source).execute()
        last_seen = (state.data or [{}])[0].get("last_seen_id") if state.data else None
        known = {last_seen} if last_seen else set()
        new_comments = []
        for c in flat:
            cid = c.get("id")
            if not cid:
                continue
            if cid in known or cid == last_seen:
                break
            new_comments.append(row_comment(c, post_id))
            author_name = parse_author(c.get("author") or c.get("author_name"))
            print(f"new agent {author_name}")
            if author_name:
                ensure_agent(author_name)
        if new_comments:
            supabase.table("comments").upsert(new_comments, on_conflict="id").execute()
            print(f"+{len(new_comments)} comments")
        else:
            print("ok")
        if flat:
            newest_id = flat[0].get("id")
            if newest_id:
                supabase.table("ingest_state").upsert(
                    {
                        "source": source,
                        "last_seen_id": newest_id,
                        "last_seen_created_at": flat[0].get("created_at"),
                    },
                    on_conflict="source",
                ).execute()

    for rank, post_id, raw_item in feed_items:
        supabase.table("feed_snapshots").insert(
            {
                "feed_type": "hot",
                "fetched_at": fetched_at,
                "rank": rank,
                "post_id": post_id,
                "raw": raw_item,
            }
        ).execute()

    print(f"Done: {len(feed_items)} posts snapshot saved. Next run in {INTERVAL_SEC // 60} min.")


def main() -> None:
    while True:
        try:
            run_once()
        except Exception as e:
            print(f"Run failed: {e}")
        print(f"Sleeping {INTERVAL_SEC}s...")
        time.sleep(INTERVAL_SEC)


if __name__ == "__main__":
    main()
