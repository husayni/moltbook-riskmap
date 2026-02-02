#!/usr/bin/env python3
"""
Supabase Data Seeder for Moltbook Sentinel Dashboard
=====================================================

This script loads dashboard_analytics.json and populates the Supabase database
with all the analytics data for the Risk Surveillance dashboard.

Requirements:
    pip install supabase python-dotenv

Usage:
    1. Create a .env file with your Supabase credentials:
       SUPABASE_URL=https://your-project.supabase.co
       SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
    
    2. Run the script:
       python seed_supabase.py path/to/dashboard_analytics.json

    Or provide credentials via command line:
       python seed_supabase.py dashboard_analytics.json --url YOUR_URL --key YOUR_KEY
"""

import json
import argparse
import os
import sys
from datetime import datetime
from typing import Any

try:
    from supabase import create_client, Client
except ImportError:
    print("Error: supabase package not installed. Run: pip install supabase")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv is optional


def get_supabase_client(url: str | None, key: str | None) -> Client:
    """Create and return a Supabase client."""
    supabase_url = url or os.getenv("SUPABASE_URL")
    supabase_key = key or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    
    if not supabase_url or not supabase_key:
        print("Error: Supabase credentials not found.")
        print("Please provide them via:")
        print("  1. Environment variables: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
        print("  2. A .env file in the current directory")
        print("  3. Command line arguments: --url and --key")
        sys.exit(1)
    
    return create_client(supabase_url, supabase_key)


def clear_existing_data(supabase: Client) -> None:
    """Clear all existing data from analytics tables."""
    tables = [
        "time_series_hourly",
        "radar_categories",
        "submolt_scatter_points",
        "graph_nodes",
        "graph_edges",
        "top_misaligned_agents",
        "analytics_snapshots",
    ]
    
    print("Clearing existing data...")
    for table in tables:
        try:
            supabase.table(table).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
            print(f"  Cleared: {table}")
        except Exception as e:
            print(f"  Warning: Could not clear {table}: {e}")


def insert_snapshot(supabase: Client, data: dict[str, Any]) -> str:
    """Insert the main analytics snapshot and return its ID."""
    print("Inserting analytics snapshot...")
    
    snapshot = {
        "generated_at": data.get("generated_at", datetime.utcnow().isoformat()),
        "totals": data.get("totals", {}),
        "notes": data.get("notes", ""),
    }
    
    result = supabase.table("analytics_snapshots").insert(snapshot).execute()
    snapshot_id = result.data[0]["id"]
    print(f"  Created snapshot: {snapshot_id}")
    return snapshot_id


def insert_time_series(supabase: Client, snapshot_id: str, data: dict[str, Any]) -> None:
    """Insert time series data."""
    time_series = data.get("time_series", {}).get("hourly", [])
    if not time_series:
        print("No time series data found.")
        return
    
    print(f"Inserting {len(time_series)} time series records...")
    
    records = []
    for entry in time_series:
        records.append({
            "snapshot_id": snapshot_id,
            "hour": entry.get("hour"),
            "agents_added": entry.get("agents_added", 0),
            "agents_total": entry.get("agents_total", 0),
            "posts_added": entry.get("posts_added", 0),
            "posts_total": entry.get("posts_total", 0),
            "analyzed_posts_added": entry.get("analyzed_posts_added", 0),
            "analyzed_posts_total": entry.get("analyzed_posts_total", 0),
            "flagged_posts_added": entry.get("flagged_posts_added", 0),
            "flagged_posts_total": entry.get("flagged_posts_total", 0),
            "flagged_content_added": entry.get("flagged_content_added", 0),
            "flagged_content_total": entry.get("flagged_content_total", 0),
        })
    
    # Insert in batches of 500
    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        supabase.table("time_series_hourly").insert(batch).execute()
        print(f"  Inserted batch {i // batch_size + 1}/{(len(records) + batch_size - 1) // batch_size}")


def insert_radar_categories(supabase: Client, snapshot_id: str, data: dict[str, Any]) -> None:
    """Insert global radar categories."""
    categories = data.get("radar_overall", {}).get("categories", [])
    if not categories:
        print("No radar categories found.")
        return
    
    print(f"Inserting {len(categories)} radar categories...")
    
    records = []
    for cat in categories:
        records.append({
            "snapshot_id": snapshot_id,
            "key": cat.get("key"),
            "avg_severity": cat.get("avg_severity", 0),
            "max_severity": cat.get("max_severity", 0),
            "nonzero_records": cat.get("nonzero_records", 0),
            "level_counts": cat.get("level_counts", {}),
            "top_examples": cat.get("top_examples", []),
        })
    
    supabase.table("radar_categories").insert(records).execute()
    print("  Done.")


def insert_submolt_scatter(supabase: Client, snapshot_id: str, data: dict[str, Any]) -> None:
    """Insert submolt scatter points."""
    points = data.get("submolt_scatter", {}).get("points", [])
    if not points:
        print("No submolt scatter points found.")
        return
    
    print(f"Inserting {len(points)} submolt scatter points...")
    
    records = []
    for point in points:
        records.append({
            "snapshot_id": snapshot_id,
            "submolt_id": point.get("submolt_id"),
            "submolt_name": point.get("submolt_name", ""),
            "total_posts": point.get("total_posts", 0),
            "total_comments": point.get("total_comments", 0),
            "total_content": point.get("total_content", 0),
            "misalignment_incidents": point.get("misalignment_incidents", 0),
            "misalignment_rate_per_1000": point.get("misalignment_rate_per_1000", 0),
            "post_counts": point.get("post_counts", {}),
            "comment_counts": point.get("comment_counts", {}),
        })
    
    # Insert in batches
    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        supabase.table("submolt_scatter_points").insert(batch).execute()
    print("  Done.")


def insert_graph_data(supabase: Client, snapshot_id: str, data: dict[str, Any]) -> None:
    """Insert network graph nodes and edges."""
    graph = data.get("interaction_graph", {})
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])
    
    if nodes:
        print(f"Inserting {len(nodes)} graph nodes...")
        node_records = []
        for node in nodes:
            node_records.append({
                "snapshot_id": snapshot_id,
                "node_id": node.get("id"),
                "name": node.get("name", ""),
                "interaction_out": node.get("interaction_out", 0),
                "interaction_in": node.get("interaction_in", 0),
                "total_posts": node.get("total_posts", 0),
                "total_comments": node.get("total_comments", 0),
                "post_counts": node.get("post_counts", {}),
                "comment_counts": node.get("comment_counts", {}),
                "risk_score_max": node.get("risk_score_max", 0),
                "risk_score_avg": node.get("risk_score_avg", 0),
                "risk_primary_category": node.get("risk_primary_category", ""),
                "risk_records": node.get("risk_records", 0),
                "risk_nonzero_records": node.get("risk_nonzero_records", 0),
            })
        
        batch_size = 500
        for i in range(0, len(node_records), batch_size):
            batch = node_records[i:i + batch_size]
            supabase.table("graph_nodes").insert(batch).execute()
        print("  Done.")
    
    if edges:
        print(f"Inserting {len(edges)} graph edges...")
        edge_records = []
        for edge in edges:
            edge_records.append({
                "snapshot_id": snapshot_id,
                "source": edge.get("source"),
                "target": edge.get("target"),
                "weight": edge.get("weight", 0),
                "comment_count": edge.get("comment_count", 0),
                "reply_count": edge.get("reply_count", 0),
            })
        
        batch_size = 500
        for i in range(0, len(edge_records), batch_size):
            batch = edge_records[i:i + batch_size]
            supabase.table("graph_edges").insert(batch).execute()
            print(f"  Inserted batch {i // batch_size + 1}/{(len(edge_records) + batch_size - 1) // batch_size}")
        print("  Done.")


def insert_top_agents(supabase: Client, snapshot_id: str, data: dict[str, Any]) -> None:
    """Insert top misaligned agents."""
    agents = data.get("top_misaligned_agents", [])
    if not agents:
        print("No top misaligned agents found.")
        return
    
    print(f"Inserting {len(agents)} top misaligned agents...")
    
    records = []
    for agent in agents:
        records.append({
            "snapshot_id": snapshot_id,
            "agent_id": agent.get("agent_id"),
            "agent_name": agent.get("agent_name", ""),
            "risk_score_max": agent.get("risk_score_max", 0),
            "risk_score_avg": agent.get("risk_score_avg", 0),
            "risk_primary_category": agent.get("risk_primary_category", ""),
            "risk_records": agent.get("risk_records", 0),
            "risk_nonzero_records": agent.get("risk_nonzero_records", 0),
            "radar": agent.get("radar", {}),
            "top_content": agent.get("top_content", []),
            "top_flagged_content": agent.get("top_flagged_content", []),
            "risk_intent": agent.get("risk_intent", ""),
            "social_account": agent.get("social_account"),
        })
    
    supabase.table("top_misaligned_agents").insert(records).execute()
    print("  Done.")


def main():
    parser = argparse.ArgumentParser(
        description="Seed Supabase database with dashboard analytics data"
    )
    parser.add_argument(
        "json_file",
        help="Path to the dashboard_analytics.json file"
    )
    parser.add_argument(
        "--url",
        help="Supabase project URL (or set SUPABASE_URL env var)"
    )
    parser.add_argument(
        "--key",
        help="Supabase service role key (or set SUPABASE_SERVICE_ROLE_KEY env var)"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing data before inserting"
    )
    
    args = parser.parse_args()
    
    # Load JSON data
    print(f"Loading data from {args.json_file}...")
    try:
        with open(args.json_file, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found: {args.json_file}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON: {e}")
        sys.exit(1)
    
    print(f"  Generated at: {data.get('generated_at', 'unknown')}")
    print(f"  Totals: {data.get('totals', {})}")
    
    # Create Supabase client
    supabase = get_supabase_client(args.url, args.key)
    
    # Clear existing data if requested
    if args.clear:
        clear_existing_data(supabase)
    
    # Insert all data
    try:
        snapshot_id = insert_snapshot(supabase, data)
        insert_time_series(supabase, snapshot_id, data)
        insert_radar_categories(supabase, snapshot_id, data)
        insert_submolt_scatter(supabase, snapshot_id, data)
        insert_graph_data(supabase, snapshot_id, data)
        insert_top_agents(supabase, snapshot_id, data)
        
        print("\n" + "=" * 50)
        print("SUCCESS! All data has been seeded to Supabase.")
        print(f"Snapshot ID: {snapshot_id}")
        print("=" * 50)
        
    except Exception as e:
        print(f"\nError during seeding: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
