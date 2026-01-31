-- Moltbook Scraper Database Schema
-- Created: 2026-01-31
-- Database: Supabase / PostgreSQL

-- Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- CORE TABLES
-- ============================================================================

-- 1. Agents table
-- Stores agent/user profiles discovered from posts and comments
CREATE TABLE IF NOT EXISTS agents (
    name TEXT PRIMARY KEY,
    description TEXT,
    karma INTEGER DEFAULT 0,
    follower_count INTEGER DEFAULT 0,
    following_count INTEGER DEFAULT 0,
    is_claimed BOOLEAN DEFAULT false,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE,
    last_active TIMESTAMP WITH TIME ZONE,
    owner_x_handle TEXT,
    owner_x_name TEXT,
    owner_x_avatar TEXT,
    owner_x_bio TEXT,
    owner_x_follower_count INTEGER,
    owner_x_following_count INTEGER,
    owner_x_verified BOOLEAN DEFAULT false,
    raw JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Metadata
    first_seen_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    last_updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- 2. Posts table
-- Stores posts from the trending feed
CREATE TABLE IF NOT EXISTS posts (
    id TEXT PRIMARY KEY,
    submolt_name TEXT,
    author_name TEXT REFERENCES agents(name) ON DELETE SET NULL,
    title TEXT,
    content TEXT,
    url TEXT,
    upvotes INTEGER DEFAULT 0,
    downvotes INTEGER DEFAULT 0,
    comment_count INTEGER DEFAULT 0,
    last_comment_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    raw JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Metadata
    first_scraped_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    last_scraped_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- 3. Comments table
-- Stores comments and nested replies from posts
CREATE TABLE IF NOT EXISTS comments (
    id TEXT PRIMARY KEY,
    post_id TEXT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    parent_id TEXT REFERENCES comments(id) ON DELETE CASCADE,
    author_name TEXT REFERENCES agents(name) ON DELETE SET NULL,
    content TEXT,
    upvotes INTEGER DEFAULT 0,
    downvotes INTEGER DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    raw JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Metadata
    first_scraped_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    last_scraped_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- 4. Ingest state table
-- Tracks crawl progress and incremental update state
CREATE TABLE IF NOT EXISTS ingest_state (
    source TEXT PRIMARY KEY,  -- e.g., 'feed:hot', 'comments:post_id'
    cursor TEXT,
    last_seen_id TEXT,
    last_seen_created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- ============================================================================
-- SNAPSHOT TABLES
-- ============================================================================

-- Feed snapshots table
-- Stores historical snapshots of trending feed rankings
CREATE TABLE IF NOT EXISTS feed_snapshots (
    id BIGSERIAL PRIMARY KEY,
    feed_type TEXT NOT NULL,  -- 'hot'
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    rank INTEGER NOT NULL,
    post_id TEXT REFERENCES posts(id) ON DELETE SET NULL,
    raw JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Ensure we don't duplicate the same post at the same rank in the same fetch
    CONSTRAINT unique_feed_snapshot UNIQUE (feed_type, fetched_at, rank)
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Agents indexes
CREATE INDEX IF NOT EXISTS idx_agents_karma ON agents(karma DESC);
CREATE INDEX IF NOT EXISTS idx_agents_follower_count ON agents(follower_count DESC);
CREATE INDEX IF NOT EXISTS idx_agents_last_active ON agents(last_active DESC);
CREATE INDEX IF NOT EXISTS idx_agents_is_claimed ON agents(is_claimed) WHERE is_claimed = true;

-- Posts indexes
CREATE INDEX IF NOT EXISTS idx_posts_author_name ON posts(author_name);
CREATE INDEX IF NOT EXISTS idx_posts_submolt_name ON posts(submolt_name);
CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_posts_upvotes ON posts(upvotes DESC);
CREATE INDEX IF NOT EXISTS idx_posts_comment_count ON posts(comment_count DESC);
CREATE INDEX IF NOT EXISTS idx_posts_last_comment_at ON posts(last_comment_at DESC);
CREATE INDEX IF NOT EXISTS idx_posts_last_scraped_at ON posts(last_scraped_at DESC);

-- Comments indexes
CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id);
CREATE INDEX IF NOT EXISTS idx_comments_parent_id ON comments(parent_id);
CREATE INDEX IF NOT EXISTS idx_comments_author_name ON comments(author_name);
CREATE INDEX IF NOT EXISTS idx_comments_created_at ON comments(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_comments_post_created ON comments(post_id, created_at DESC);

-- Feed snapshots indexes
CREATE INDEX IF NOT EXISTS idx_feed_snapshots_type_fetched ON feed_snapshots(feed_type, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_feed_snapshots_post_fetched ON feed_snapshots(post_id, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_feed_snapshots_fetched_at ON feed_snapshots(fetched_at DESC);

-- Ingest state indexes
CREATE INDEX IF NOT EXISTS idx_ingest_state_updated_at ON ingest_state(updated_at DESC);

-- ============================================================================
-- TRIGGERS FOR AUTOMATIC TIMESTAMP UPDATES
-- ============================================================================

-- Function to update last_updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to update last_scraped_at timestamp
CREATE OR REPLACE FUNCTION update_last_scraped_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_scraped_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for agents table
DROP TRIGGER IF EXISTS trigger_agents_updated_at ON agents;
CREATE TRIGGER trigger_agents_updated_at
    BEFORE UPDATE ON agents
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger for posts table
DROP TRIGGER IF EXISTS trigger_posts_scraped_at ON posts;
CREATE TRIGGER trigger_posts_scraped_at
    BEFORE UPDATE ON posts
    FOR EACH ROW
    EXECUTE FUNCTION update_last_scraped_at_column();

-- Trigger for comments table
DROP TRIGGER IF EXISTS trigger_comments_scraped_at ON comments;
CREATE TRIGGER trigger_comments_scraped_at
    BEFORE UPDATE ON comments
    FOR EACH ROW
    EXECUTE FUNCTION update_last_scraped_at_column();

-- Trigger for ingest_state table
DROP TRIGGER IF EXISTS trigger_ingest_state_updated_at ON ingest_state;
CREATE TRIGGER trigger_ingest_state_updated_at
    BEFORE UPDATE ON ingest_state
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- USEFUL VIEWS
-- ============================================================================

-- View: Latest trending snapshot
CREATE OR REPLACE VIEW latest_trending_feed AS
SELECT 
    fs.rank,
    fs.fetched_at,
    p.*
FROM feed_snapshots fs
JOIN posts p ON fs.post_id = p.id
WHERE fs.fetched_at = (
    SELECT MAX(fetched_at) 
    FROM feed_snapshots 
    WHERE feed_type = 'hot'
)
AND fs.feed_type = 'hot'
ORDER BY fs.rank;

-- View: Top agents by karma
CREATE OR REPLACE VIEW top_agents_by_karma AS
SELECT 
    name,
    karma,
    follower_count,
    is_claimed,
    owner_x_handle,
    last_active
FROM agents
WHERE is_active = true
ORDER BY karma DESC
LIMIT 100;

-- View: Posts with comment stats
CREATE OR REPLACE VIEW posts_with_stats AS
SELECT 
    p.*,
    COUNT(c.id) as actual_comment_count,
    MAX(c.created_at) as latest_comment_at,
    COUNT(DISTINCT c.author_name) as unique_commenters
FROM posts p
LEFT JOIN comments c ON p.id = c.post_id
GROUP BY p.id;

-- View: Comment threads (hierarchical)
CREATE OR REPLACE VIEW comment_threads AS
WITH RECURSIVE thread AS (
    -- Root comments
    SELECT 
        id,
        post_id,
        parent_id,
        author_name,
        content,
        created_at,
        upvotes,
        0 as depth,
        ARRAY[id] as path
    FROM comments
    WHERE parent_id IS NULL
    
    UNION ALL
    
    -- Nested replies
    SELECT 
        c.id,
        c.post_id,
        c.parent_id,
        c.author_name,
        c.content,
        c.created_at,
        c.upvotes,
        t.depth + 1,
        t.path || c.id
    FROM comments c
    JOIN thread t ON c.parent_id = t.id
)
SELECT * FROM thread
ORDER BY path;

-- ============================================================================
-- USEFUL FUNCTIONS
-- ============================================================================

-- Function to get posts needing comment refresh
CREATE OR REPLACE FUNCTION get_posts_needing_comment_refresh()
RETURNS TABLE (
    post_id TEXT,
    last_scraped_at TIMESTAMP WITH TIME ZONE,
    comment_count INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.id,
        p.last_scraped_at,
        p.comment_count
    FROM posts p
    WHERE p.last_scraped_at < now() - INTERVAL '1 hour'
    OR p.last_scraped_at IS NULL
    ORDER BY p.last_comment_at DESC NULLS LAST;
END;
$$ LANGUAGE plpgsql;

-- Function to get agents needing profile refresh
CREATE OR REPLACE FUNCTION get_agents_needing_refresh()
RETURNS TABLE (
    agent_name TEXT,
    last_updated_at TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        a.name,
        a.last_updated_at
    FROM agents a
    WHERE a.last_updated_at < now() - INTERVAL '1 hour'
    OR a.last_updated_at IS NULL
    ORDER BY a.last_active DESC NULLS LAST;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- ROW LEVEL SECURITY (RLS) - Optional
-- ============================================================================

-- Enable RLS on tables (uncomment if you need RLS)
-- ALTER TABLE agents ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE posts ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE comments ENABLE ROW LEVEL SECURITY;
-- ALTER TABLE feed_snapshots ENABLE ROW LEVEL SECURITY;

-- Example policy for read-only public access (uncomment if needed)
-- CREATE POLICY "Allow public read access" ON agents FOR SELECT USING (true);
-- CREATE POLICY "Allow public read access" ON posts FOR SELECT USING (true);
-- CREATE POLICY "Allow public read access" ON comments FOR SELECT USING (true);
-- CREATE POLICY "Allow public read access" ON feed_snapshots FOR SELECT USING (true);

-- ============================================================================
-- GRANTS (adjust based on your needs)
-- ============================================================================

-- Grant usage on schema
-- GRANT USAGE ON SCHEMA public TO anon, authenticated;

-- Grant select on all tables to anon and authenticated roles
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon, authenticated;

-- Grant all privileges to service role (for scraper)
-- GRANT ALL ON ALL TABLES IN SCHEMA public TO service_role;
-- GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO service_role;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE agents IS 'Agent/user profiles discovered from Moltbook posts and comments';
COMMENT ON TABLE posts IS 'Posts scraped from the Moltbook hot/trending feed';
COMMENT ON TABLE comments IS 'Comments and replies from scraped posts';
COMMENT ON TABLE ingest_state IS 'Tracks scraper state for incremental updates';
COMMENT ON TABLE feed_snapshots IS 'Historical snapshots of trending feed rankings';

COMMENT ON COLUMN posts.comment_count IS 'Comment count from API (may differ from actual count)';
COMMENT ON COLUMN posts.last_comment_at IS 'Timestamp of last comment from API';
COMMENT ON COLUMN ingest_state.source IS 'Source identifier: feed:hot or comments:<post_id>';

-- ============================================================================
-- SAMPLE QUERIES FOR TESTING
-- ============================================================================

/*
-- Get latest trending posts
SELECT * FROM latest_trending_feed LIMIT 10;

-- Get top agents
SELECT * FROM top_agents_by_karma LIMIT 10;

-- Get posts with most comments in last 24 hours
SELECT * FROM posts_with_stats 
WHERE created_at > now() - INTERVAL '24 hours'
ORDER BY actual_comment_count DESC
LIMIT 10;

-- Get comment thread for a post
SELECT * FROM comment_threads
WHERE post_id = 'some-post-id'
LIMIT 100;

-- Get posts needing refresh
SELECT * FROM get_posts_needing_comment_refresh() LIMIT 20;

-- Get agents needing refresh
SELECT * FROM get_agents_needing_refresh() LIMIT 50;
*/
