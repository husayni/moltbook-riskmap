# Moltbook Scraper Plan + DB Design (Final)

Date: 2026-01-31

## Scope (Concrete)
- Every 15 minutes, ingest the top 50 hot posts.
- Only ingest posts that appear in this feed ("all posts" means all posts we have scraped so far, not full site backfill).
- For each ingested post, fetch only new comments since the last scrape (do not re-scrape old comments).
- Build agent profiles only from agents who post or comment on these posts (no separate top-agents feed).
- Persist snapshots so we can see what was trending at each run.

## API Surface (Moltbook)
Base URL: https://www.moltbook.com/api/v1

Required endpoints
- Trending feed:
  - GET /posts?sort=hot&limit=50 (see pagination note below)
  - Example: https://www.moltbook.com/api/v1/posts?sort=hot&limit=50
- Post detail:
  - GET /posts/{post_id}
  - Example: https://www.moltbook.com/api/v1/posts/74b073fd-37db-4a32-a9e1-c7652e5c0d59
- Agent profile (only for agents discovered in posts/comments):
  - GET /agents/profile?name={agent_name}
  - Example: https://www.moltbook.com/api/v1/agents/profile?name=Shellraiser

## Crawl Strategy

### Job A - Trending ingest (every 15 minutes)
1) Fetch feed:
- /posts?sort=hot&limit=50
 - If response includes `has_more: true` and `next_offset`, fetch additional pages until you collect 50 posts or `has_more` is false.

2) Snapshot this feed run:
- store (feed_type="hot", fetched_at, rank, post_id, raw)

3) For each post_id from the feed:
- GET /posts/{post_id}
- upsert post
- fetch only new comments via post detail:
  - use /posts/{post_id} (this endpoint returns comments + nested replies)
  - flatten the returned comment tree to a list
  - stop once you encounter a known comment_id for that post
- upsert comments (dedupe by comment.id)
- collect distinct author names from post + comments for agent profile refresh

4) Incremental guard:
- store last_seen_comment_id and/or last_seen_comment_created_at per post in ingest_state
- if post comment_count has not increased since last run, skip comment refresh

### Job B - Agent profile refresh (every 60 minutes)
1) Build agent list from distinct authors seen in posts + comments from Job A.
2) For each agent:
- GET /agents/profile?name={agent_name}
- upsert agent record

### Job C - Comment late-replies refresh (daily)
- Re-scan comments for posts that were trending in the last 7–14 days.

## Rate Limiting + Safety
- Target <= 60 req/min (under the 100 req/min limit)
- Priority order:
  1) trending feed
  2) comments for newly seen trending posts
  3) comment refresh only if comment_count grew
  4) agent profile refresh
- On 429, respect retry hints and back off

## Scheduling
- scrape trending every 15 minutes
- refresh agent profiles every 60 minutes
- scrape refresh-comments daily

## DB Design (Supabase / Postgres)

### Core tables

1) agents
- name TEXT PRIMARY KEY
- description TEXT
- karma INT
- follower_count INT
- following_count INT
- is_claimed BOOLEAN
- is_active BOOLEAN
- created_at TIMESTAMP WITH TIME ZONE
- last_active TIMESTAMP WITH TIME ZONE
- owner_x_handle TEXT
- owner_x_name TEXT
- owner_x_avatar TEXT
- owner_x_bio TEXT
- owner_x_follower_count INT
- owner_x_following_count INT
- owner_x_verified BOOLEAN
- raw JSONB

2) posts
- id TEXT PRIMARY KEY
- submolt_name TEXT
- author_name TEXT REFERENCES agents(name)
- title TEXT
- content TEXT
- url TEXT
- upvotes INT
- downvotes INT
- comment_count INT  -- if provided by API
- last_comment_at TIMESTAMP WITH TIME ZONE  -- if provided by API
- created_at TIMESTAMP WITH TIME ZONE
- updated_at TIMESTAMP WITH TIME ZONE
- raw JSONB

3) comments
- id TEXT PRIMARY KEY
- post_id TEXT REFERENCES posts(id)
- parent_id TEXT REFERENCES comments(id)
- author_name TEXT REFERENCES agents(name)
- content TEXT
- upvotes INT
- downvotes INT
- created_at TIMESTAMP WITH TIME ZONE
- updated_at TIMESTAMP WITH TIME ZONE
- raw JSONB

4) ingest_state
- source TEXT PRIMARY KEY  -- feed:hot, comments:<post_id>
- cursor TEXT
- last_seen_id TEXT
- last_seen_created_at TIMESTAMP WITH TIME ZONE
- updated_at TIMESTAMP WITH TIME ZONE

### Snapshot tables

1) feed_snapshots
- id BIGSERIAL PRIMARY KEY
- feed_type TEXT NOT NULL  -- hot
- fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
- rank INT NOT NULL
- post_id TEXT REFERENCES posts(id)
- raw JSONB NOT NULL

Indexes:
- (feed_type, fetched_at DESC)
- (post_id, fetched_at DESC)

## Ingestion Notes
- Upsert all entities by primary key.
- Store raw payload JSON in raw for forward compatibility.
- Limit scope to hot feed posts and their comments.
- Agent profiles are derived only from agents who appear in those posts/comments.
- Never re-scrape a post record if unchanged; re-fetch only to check comment_count / updated_at.
- Comments are incremental: only fetch pages until you encounter a known comment_id.

## Supabase MCP Setup
Docs: https://supabase.com/docs/guides/getting-started/mcp

MCP config snippet:
```
{
  "mcpServers": {
    "supabase": {
      "type": "http",
      "url": "https://mcp.supabase.com/mcp?project_ref=${SUPABASE_PROJECT_REF}",
      "headers": {
        "Authorization": "Bearer ${SUPABASE_ACCESS_TOKEN}"
      }
    }
  }
}
```

Environment variables required:
- SUPABASE_ACCESS_TOKEN
- SUPABASE_PROJECT_REF

## Open Questions
- Which MCP client are we targeting (Cursor, Claude Desktop, etc.)?
