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


def row_agent(author: dict) -> dict:
    """Build agent row from API author object (post.author or comment.author)."""
    if not author or not author.get("name"):
        return None
    owner = author.get("owner") or {}
    return {
        "name": author.get("name"),
        "description": author.get("description"),
        "karma": author.get("karma") if author.get("karma") is not None else 0,
        "follower_count": author.get("follower_count") if author.get("follower_count") is not None else 0,
        "following_count": author.get("following_count") if author.get("following_count") is not None else 0,
        "owner_x_handle": owner.get("x_handle"),
        "owner_x_name": owner.get("x_name"),
        "owner_x_avatar": owner.get("x_avatar"),
        "owner_x_bio": owner.get("x_bio"),
        "owner_x_follower_count": owner.get("x_follower_count"),
        "owner_x_following_count": owner.get("x_following_count"),
        "owner_x_verified": owner.get("x_verified") if owner.get("x_verified") is not None else False,
        "raw": author,
    }


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

    # One query: all ingest_state for comments:<post_id>
    sources = [f"comments:{pid}" for _, pid, _ in feed_items]
    state_res = (
        supabase.table("ingest_state")
        .select("source, last_seen_id")
        .in_("source", sources)
        .execute()
    )
    last_seen_by_source = {r["source"]: r["last_seen_id"] for r in (state_res.data or []) if r.get("last_seen_id")}

    authors: set[str] = set()
    post_rows: list[dict] = []
    all_new_comments: list[dict] = []
    ingest_state_rows: list[dict] = []
    snapshot_rows: list[dict] = []
    total_new_comments = 0

    for i, (rank, post_id, raw_item) in enumerate(feed_items, 1):
        print(f"  [{i}/{len(feed_items)}] Post {post_id[:8]}...", end=" ", flush=True)
        try:
            data = get(f"/posts/{post_id}")
        except Exception as e:
            print(f"skip: {e}")
            continue

        author = parse_author(data.get("author") or data.get("author_name"))
        if author:
            authors.add(author)

        post_rows.append(row_post(post_id, data))

        comments_tree = data.get("comments") or data.get("comment_tree") or []
        flat = flatten_comments(comments_tree if isinstance(comments_tree, list) else [comments_tree])
        source = f"comments:{post_id}"
        last_seen = last_seen_by_source.get(source)
        new_comments = []
        for c in flat:
            cid = c.get("id")
            if not cid:
                continue
            if cid == last_seen:
                break
            new_comments.append(row_comment(c, post_id))
            an = parse_author(c.get("author") or c.get("author_name"))
            if an:
                authors.add(an)
        total_new_comments += len(new_comments)
        all_new_comments.extend(new_comments)
        if flat and flat[0].get("id"):
            ingest_state_rows.append(
                {
                    "source": source,
                    "last_seen_id": flat[0]["id"],
                    "last_seen_created_at": flat[0].get("created_at"),
                }
            )
        print(f"+{len(new_comments)} comments" if new_comments else "ok")
        snapshot_rows.append(
            {"feed_type": "hot", "fetched_at": fetched_at, "rank": rank, "post_id": post_id, "raw": raw_item}
        )

    # Batch writes (agents first: FK from posts/comments)
    if authors:
        supabase.table("agents").upsert(
            [{"name": n, "raw": {}} for n in authors],
            on_conflict="name",
        ).execute()
    if post_rows:
        supabase.table("posts").upsert(post_rows, on_conflict="id").execute()
    if all_new_comments:
        supabase.table("comments").upsert(all_new_comments, on_conflict="id").execute()
    if ingest_state_rows:
        supabase.table("ingest_state").upsert(ingest_state_rows, on_conflict="source").execute()
    if snapshot_rows:
        supabase.table("feed_snapshots").insert(snapshot_rows).execute()

    print(f"Done: {len(post_rows)} posts, {total_new_comments} new comments, {len(authors)} agents. Next run in {INTERVAL_SEC // 60} min.")


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