#!/usr/bin/env python3
"""
Build dashboard analytics JSON for Moltbook risk map.

Outputs:
- Totals and hourly time series for agents/posts/analyzed posts
- Overall radar data across all agents with top content examples
- Submolt scatter plot data
- Agent interaction graph filtered to profiled agents
- Top misaligned agents with per-agent radar + intent summary
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


CATEGORY_KEYS: tuple[str, ...] = (
    "capability_misalignment",
    "instructional_subversion",
    "instrumental_convergence",
    "autonomy_replication",
    "deceptive_behavior",
    "sycophancy",
)

CATEGORY_ALIASES = {
    "capability": "capability_misalignment",
}

CATEGORY_ALIASES_REVERSE = {
    "capability_misalignment": ["capability"],
}


def safe_str(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def safe_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    if isinstance(value, str):
        value = value.strip()
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def hour_bucket(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(minute=0, second=0, microsecond=0)


def iso_hour(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_category(category: str | None) -> str:
    if not category:
        return "none"
    category = safe_str(category)
    return CATEGORY_ALIASES.get(category, category)


def iter_jsonl(path: Path) -> Iterable[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def push_top(items: list[dict], item: dict, *, limit: int, key) -> None:
    items.append(item)
    items.sort(key=key, reverse=True)
    if len(items) > limit:
        del items[limit:]


def build_example(record: dict, record_type: str, *, category: str | None = None) -> dict:
    result = record.get("result") or {}
    evidence = result.get("evidence") or []
    snippet = ""
    if evidence:
        snippet = safe_str((evidence[0] or {}).get("evidence_snippet"))
    example = {
        "record_type": record_type,
        "id": record.get("post_id") if record_type == "post" else record.get("comment_id"),
        "post_id": record.get("post_id"),
        "title": record.get("title") or "",
        "author_id": record.get("author_id"),
        "author_name": record.get("author_name") or "",
        "created_at": record.get("created_at") or "",
        "overall_score": safe_int(result.get("overall_misalignment_score")) or 0,
        "primary_risk_category": normalize_category(result.get("primary_risk_category")),
        "summary": result.get("summary") or "",
        "evidence_snippet": snippet,
    }
    if category:
        example["category"] = category
        severity_by_category = result.get("severity_by_category") or {}
        severity = severity_by_category.get(category)
        if severity is None:
            for alias in CATEGORY_ALIASES_REVERSE.get(category, []):
                severity = severity_by_category.get(alias)
                if severity is not None:
                    break
        example["category_severity"] = safe_int(severity) or 0
    return example


def add_node(nodes: dict[str, dict], author: dict, allowed_ids: set[str]) -> str | None:
    if not isinstance(author, dict):
        return None
    agent_id = safe_str(author.get("id"))
    if not agent_id or agent_id not in allowed_ids:
        return None
    agent_name = safe_str(author.get("name"))
    node = nodes.get(agent_id)
    if node is None:
        nodes[agent_id] = {
            "agent_id": agent_id,
            "agent_name": agent_name,
        }
    else:
        if not node.get("agent_name") and agent_name:
            node["agent_name"] = agent_name
    return agent_id


def add_edge(
    edges: dict[tuple[str, str], dict],
    source_id: str | None,
    target_id: str | None,
    interaction_type: str,
) -> None:
    if not source_id or not target_id:
        return
    key = (source_id, target_id)
    edge = edges.get(key)
    if edge is None:
        edge = {
            "source_id": source_id,
            "target_id": target_id,
            "weight": 0,
            "comment_count": 0,
            "reply_count": 0,
        }
        edges[key] = edge
    edge["weight"] += 1
    if interaction_type == "comment":
        edge["comment_count"] += 1
    elif interaction_type == "reply":
        edge["reply_count"] += 1


def iter_comment_nodes(comments: object) -> Iterable[dict]:
    if not isinstance(comments, list):
        return []
    for comment in comments:
        if isinstance(comment, dict):
            yield comment


def walk_comments(
    comment: dict,
    *,
    post_id: str,
    parent_author_id: str | None,
    parent_comment_id: str | None,
    parent_is_post: bool,
    nodes: dict[str, dict],
    edges: dict[tuple[str, str], dict],
    allowed_ids: set[str],
    exclude_self: bool,
) -> None:
    if not isinstance(comment, dict):
        return

    author = comment.get("author") or {}
    author_id = add_node(nodes, author, allowed_ids)
    comment_id = safe_str(comment.get("id"))

    interaction_type = "comment" if parent_is_post else "reply"
    if author_id and parent_author_id:
        if exclude_self and author_id == parent_author_id:
            pass
        else:
            add_edge(edges, author_id, parent_author_id, interaction_type)

    replies = comment.get("replies") or []
    if isinstance(replies, list):
        for reply in replies:
            walk_comments(
                reply,
                post_id=post_id,
                parent_author_id=author_id,
                parent_comment_id=comment_id,
                parent_is_post=False,
                nodes=nodes,
                edges=edges,
                allowed_ids=allowed_ids,
                exclude_self=exclude_self,
            )


def try_add_layout(nodes: list[dict], edges: list[dict], seed: int, max_nodes: int) -> bool:
    try:
        import networkx as nx
    except Exception:
        return False

    if len(nodes) == 0 or len(nodes) > max_nodes:
        return False

    graph = nx.DiGraph()
    for node in nodes:
        graph.add_node(node["id"])
    for edge in edges:
        graph.add_edge(edge["source"], edge["target"], weight=edge.get("weight", 1))

    pos = nx.spring_layout(graph, seed=seed)
    for node in nodes:
        coords = pos.get(node["id"])
        if coords is None:
            continue
        node["x"] = float(coords[0])
        node["y"] = float(coords[1])
    return True


def load_agent_profiles(path: Path) -> tuple[dict[str, dict], set[str], dict[str, str]]:
    if not path.exists():
        return {}, set(), {}
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return {}, set(), {}
    name_by_id = {}
    for agent_id, profile in data.items():
        if isinstance(profile, dict):
            name = safe_str(profile.get("agent_name"))
            if name:
                name_by_id[agent_id] = name
    return data, set(data.keys()), name_by_id


def load_agents_timeseries(
    agents_dir: Path,
    hourly_counts: dict,
    social_map: dict[str, dict] | None = None,
) -> dict:
    totals = {
        "total_agents": 0,
        "agents_missing_created_at": 0,
    }
    if not agents_dir.exists():
        return totals
    for path in agents_dir.glob("*.json"):
        totals["total_agents"] += 1
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        agent = payload.get("agent") or {}
        created_at = agent.get("created_at")
        dt = hour_bucket(parse_datetime(created_at))
        if dt is None:
            totals["agents_missing_created_at"] += 1
            continue
        hourly_counts[dt]["agents_added"] += 1
        if social_map is not None:
            owner = agent.get("owner")
            agent_id = safe_str(agent.get("id"))
            if agent_id and isinstance(owner, dict) and owner:
                social_map[agent_id] = owner
    return totals


def process_posts(
    posts_dir: Path,
    hourly_counts: dict,
    *,
    allowed_ids: set[str],
    nodes: dict[str, dict],
    edges: dict[tuple[str, str], dict],
    exclude_self: bool,
) -> dict:
    totals = {
        "total_posts": 0,
        "posts_missing_created_at": 0,
        "posts_parse_errors": 0,
        "comments_seen": 0,
        "replies_seen": 0,
    }
    if not posts_dir.exists():
        return totals
    for post_path in sorted(posts_dir.glob("*.json")):
        totals["total_posts"] += 1
        try:
            data = json.loads(post_path.read_text(encoding="utf-8"))
        except Exception:
            totals["posts_parse_errors"] += 1
            continue

        post = data.get("post") or {}
        post_id = safe_str(post.get("id"))
        created_at = post.get("created_at")
        dt = hour_bucket(parse_datetime(created_at))
        if dt is None:
            totals["posts_missing_created_at"] += 1
        else:
            hourly_counts[dt]["posts_added"] += 1

        post_author = post.get("author") or {}
        post_author_id = add_node(nodes, post_author, allowed_ids)

        for comment in iter_comment_nodes(data.get("comments")):
            totals["comments_seen"] += 1
            walk_comments(
                comment,
                post_id=post_id,
                parent_author_id=post_author_id,
                parent_comment_id=None,
                parent_is_post=True,
                nodes=nodes,
                edges=edges,
                allowed_ids=allowed_ids,
                exclude_self=exclude_self,
            )
            replies = comment.get("replies") or []
            if isinstance(replies, list):
                totals["replies_seen"] += len(replies)
    return totals


def process_scores(
    path: Path,
    record_type: str,
    hourly_counts: dict,
    *,
    overall_stats: dict,
    overall_examples: dict[str, list[dict]],
    agent_stats: dict[str, dict],
    agent_names: dict[str, str],
    top_examples_limit: int,
) -> dict:
    totals = {
        "records": 0,
        "records_with_scores": 0,
        "records_with_author": 0,
        "flagged_records": 0,
    }
    if not path.exists():
        return totals

    for record in iter_jsonl(path):
        totals["records"] += 1
        result = record.get("result") or {}
        score = safe_int(result.get("overall_misalignment_score"))
        if score is None:
            continue
        totals["records_with_scores"] += 1

        dt = hour_bucket(parse_datetime(record.get("created_at")))
        if record_type == "post":
            if dt is not None:
                hourly_counts[dt]["analyzed_posts_added"] += 1
        if score > 0:
            totals["flagged_records"] += 1
            if record_type == "post":
                if dt is not None:
                    hourly_counts[dt]["flagged_posts_added"] += 1
            if dt is not None:
                hourly_counts[dt]["flagged_content_added"] += 1

        author_id = safe_str(record.get("author_id"))
        author_name = safe_str(record.get("author_name"))
        if author_id:
            totals["records_with_author"] += 1

        overall_stats["record_count"] += 1
        severity_by_category = result.get("severity_by_category") or {}
        for category in CATEGORY_KEYS:
            severity = severity_by_category.get(category)
            if severity is None:
                for alias in CATEGORY_ALIASES_REVERSE.get(category, []):
                    severity = severity_by_category.get(alias)
                    if severity is not None:
                        break
            severity = safe_int(severity) or 0
            stats = overall_stats["categories"][category]
            stats["sum"] += severity
            stats["max"] = max(stats["max"], severity)
            stats["levels"][severity] += 1
            if severity > 0:
                stats["nonzero"] += 1
                example = build_example(record, record_type, category=category)
                push_top(
                    overall_examples[category],
                    example,
                    limit=top_examples_limit,
                    key=lambda item: (item.get("category_severity", 0), item.get("overall_score", 0)),
                )

        if not author_id:
            continue
        stats = agent_stats.get(author_id)
        if stats is None:
            stats = {
                "agent_id": author_id,
                "agent_name": author_name or agent_names.get(author_id, ""),
                "score_sum": 0,
                "score_count": 0,
                "score_max": 0,
                "nonzero_count": 0,
                "primary_counts": Counter(),
                "record_count": 0,
                "category_sum": {cat: 0 for cat in CATEGORY_KEYS},
                "category_max": {cat: 0 for cat in CATEGORY_KEYS},
                "category_nonzero": {cat: 0 for cat in CATEGORY_KEYS},
                "category_levels": {cat: {0: 0, 1: 0, 2: 0, 3: 0} for cat in CATEGORY_KEYS},
                "top_content": [],
                "top_flagged_content": [],
            }
            agent_stats[author_id] = stats

        if not stats.get("agent_name") and author_name:
            stats["agent_name"] = author_name

        stats["score_sum"] += score
        stats["score_count"] += 1
        stats["score_max"] = max(stats["score_max"], score)
        if score > 0:
            stats["nonzero_count"] += 1

        primary = normalize_category(result.get("primary_risk_category"))
        if primary and primary != "none":
            stats["primary_counts"][primary] += 1

        stats["record_count"] += 1
        for category in CATEGORY_KEYS:
            severity = severity_by_category.get(category)
            if severity is None:
                for alias in CATEGORY_ALIASES_REVERSE.get(category, []):
                    severity = severity_by_category.get(alias)
                    if severity is not None:
                        break
            severity = safe_int(severity) or 0
            stats["category_sum"][category] += severity
            stats["category_max"][category] = max(stats["category_max"][category], severity)
            stats["category_levels"][category][severity] += 1
            if severity > 0:
                stats["category_nonzero"][category] += 1

        example = build_example(record, record_type)
        push_top(
            stats["top_content"],
            example,
            limit=5,
            key=lambda item: (item.get("overall_score", 0), item.get("created_at", "")),
        )
        if score > 0:
            push_top(
                stats["top_flagged_content"],
                example,
                limit=3,
                key=lambda item: (item.get("overall_score", 0), item.get("created_at", "")),
            )
    return totals


def finalize_risk(stats: dict) -> dict:
    score_count = stats.get("score_count", 0)
    if score_count == 0:
        return {}
    primary_counts: Counter = stats.get("primary_counts", Counter())
    non_none = {k: v for k, v in primary_counts.items() if k != "none"}
    if non_none:
        primary_category = sorted(non_none.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
    else:
        primary_category = "none"
    return {
        "risk_score_max": stats.get("score_max", 0),
        "risk_score_avg": round(stats.get("score_sum", 0) / score_count, 3),
        "risk_primary_category": primary_category,
        "risk_records": score_count,
        "risk_nonzero_records": stats.get("nonzero_count", 0),
    }


def build_radar(stats: dict) -> dict:
    record_count = stats.get("record_count", 0)
    radar = []
    for category in CATEGORY_KEYS:
        total = stats["category_sum"].get(category, 0)
        avg = total / record_count if record_count else 0
        levels = (stats.get("category_levels") or {}).get(category) or {0: 0, 1: 0, 2: 0, 3: 0}
        radar.append(
            {
                "key": category,
                "avg_severity": round(avg, 3),
                "max_severity": stats["category_max"].get(category, 0),
                "nonzero_records": stats["category_nonzero"].get(category, 0),
                "level_counts": {
                    "0": levels.get(0, 0),
                    "1": levels.get(1, 0),
                    "2": levels.get(2, 0),
                    "3": levels.get(3, 0),
                },
            }
        )
    return {
        "record_count": record_count,
        "categories": radar,
    }


def derive_intent(top_content: list[dict]) -> str:
    for item in top_content:
        summary = safe_str(item.get("summary"))
        if summary:
            return summary
    for item in top_content:
        snippet = safe_str(item.get("evidence_snippet"))
        if snippet:
            return snippet
    return ""


def build_social_account(owner: dict | None) -> dict | None:
    if not isinstance(owner, dict):
        return None
    handle = safe_str(owner.get("x_handle"))
    if not handle:
        return None
    return {
        "platform": "x",
        "handle": handle,
        "name": safe_str(owner.get("x_name")),
        "avatar_url": safe_str(owner.get("x_avatar")),
        "bio": safe_str(owner.get("x_bio")),
        "follower_count": safe_int(owner.get("x_follower_count")) or 0,
        "following_count": safe_int(owner.get("x_following_count")) or 0,
        "verified": bool(owner.get("x_verified")),
        "url": f"https://x.com/{handle}",
    }


def build_interaction_graph(
    nodes: dict[str, dict],
    edges: dict[tuple[str, str], dict],
    agent_profiles: dict[str, dict],
    agent_stats: dict[str, dict],
    *,
    include_layout: bool,
    layout_max_nodes: int,
    max_nodes: int | None = 100,
) -> dict:
    out_counts = defaultdict(int)
    in_counts = defaultdict(int)
    for edge in edges.values():
        out_counts[edge["source_id"]] += edge["weight"]
        in_counts[edge["target_id"]] += edge["weight"]

    graph_nodes = []
    for agent_id, node in nodes.items():
        profile = agent_profiles.get(agent_id, {})
        stats = agent_stats.get(agent_id, {})
        risk = finalize_risk(stats) if stats else {}
        graph_nodes.append(
            {
                "id": agent_id,
                "name": profile.get("agent_name") or node.get("agent_name", ""),
                "interaction_out": out_counts.get(agent_id, 0),
                "interaction_in": in_counts.get(agent_id, 0),
                "total_posts": profile.get("total_posts"),
                "total_comments": profile.get("total_comments"),
                "post_counts": profile.get("post_counts"),
                "comment_counts": profile.get("comment_counts"),
                **risk,
            }
        )

    graph_nodes.sort(
        key=lambda r: (-(r.get("interaction_out", 0) + r.get("interaction_in", 0)), r["id"])
    )

    graph_edges = []
    for edge in edges.values():
        graph_edges.append(
            {
                "source": edge["source_id"],
                "target": edge["target_id"],
                "weight": edge["weight"],
                "comment_count": edge["comment_count"],
                "reply_count": edge["reply_count"],
            }
        )
    graph_edges.sort(key=lambda r: (-r["weight"], r["source"], r["target"]))

    if max_nodes and len(graph_nodes) > max_nodes:
        graph_nodes = graph_nodes[:max_nodes]
        allowed_ids = {node["id"] for node in graph_nodes}
        graph_edges = [
            edge
            for edge in graph_edges
            if edge["source"] in allowed_ids and edge["target"] in allowed_ids
        ]

    if include_layout:
        try_add_layout(graph_nodes, graph_edges, seed=42, max_nodes=layout_max_nodes)

    return {"nodes": graph_nodes, "edges": graph_edges}


def build_submolt_scatter(submolt_scores_path: Path) -> dict:
    if not submolt_scores_path.exists():
        return {"points": []}
    data = json.loads(submolt_scores_path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        records = list(data.values())
    elif isinstance(data, list):
        records = data
    else:
        records = []

    points = []
    for record in records:
        if not isinstance(record, dict):
            continue
        total_posts = safe_int(record.get("total_posts")) or 0
        total_comments = safe_int(record.get("total_comments")) or 0
        total_content = total_posts + total_comments
        post_counts = record.get("post_counts") or {}
        comment_counts = record.get("comment_counts") or {}
        misaligned_posts = sum(
            safe_int(post_counts.get(cat)) or 0 for cat in CATEGORY_KEYS
        )
        misaligned_comments = sum(
            safe_int(comment_counts.get(cat)) or 0 for cat in CATEGORY_KEYS
        )
        misaligned_total = misaligned_posts + misaligned_comments
        rate = (misaligned_total / total_content * 1000) if total_content else 0.0
        points.append(
            {
                "submolt_id": record.get("submolt_id"),
                "submolt_name": record.get("submolt_name") or "",
                "total_content": total_content,
                "total_posts": total_posts,
                "total_comments": total_comments,
                "misalignment_incidents": misaligned_total,
                "misalignment_rate_per_1000": round(rate, 3),
                "post_counts": post_counts,
                "comment_counts": comment_counts,
            }
        )

    points.sort(key=lambda r: (-r["misalignment_rate_per_1000"], -r["total_content"]))
    return {"points": points}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build dashboard analytics JSON for Moltbook Risk Map."
    )
    parser.add_argument(
        "--moltbook-data-dir",
        default="/home/hussain/hackathons/moltbook_data/data",
        help="Base data directory with agents/posts/submolts.",
    )
    parser.add_argument(
        "--riskmap-data-dir",
        default="/home/hussain/hackathons/moltbook-riskmap/data",
        help="Risk map data directory with scores and profiles.",
    )
    parser.add_argument(
        "--output",
        default="/home/hussain/hackathons/moltbook-riskmap/output/dashboard_analytics.json",
        help="Output JSON file path.",
    )
    parser.add_argument(
        "--top-examples",
        type=int,
        default=3,
        help="Top examples per category for radar hover.",
    )
    parser.add_argument(
        "--top-misaligned",
        type=int,
        default=6,
        help="Number of misaligned agent cards to include (default: 6).",
    )
    parser.add_argument(
        "--layout",
        action="store_true",
        help="Include force-directed layout coordinates if networkx is available.",
    )
    parser.add_argument(
        "--layout-max-nodes",
        type=int,
        default=2000,
        help="Maximum nodes to layout before skipping (default: 2000).",
    )
    parser.add_argument(
        "--exclude-self",
        action="store_true",
        help="Drop self-loop interactions in the interaction graph.",
    )
    args = parser.parse_args()

    moltbook_dir = Path(args.moltbook_data_dir)
    riskmap_dir = Path(args.riskmap_data_dir)
    output_path = Path(args.output)

    agents_dir = moltbook_dir / "agents"
    posts_dir = moltbook_dir / "posts"

    profiles_path = riskmap_dir / "agent_profiles.json"
    post_scores_path = riskmap_dir / "post_scores.jsonl"
    comment_scores_path = riskmap_dir / "comment_scores.jsonl"
    submolt_scores_path = riskmap_dir / "submolt_scores.json"

    agent_profiles, allowed_ids, agent_names = load_agent_profiles(profiles_path)

    hourly_counts = defaultdict(
        lambda: {
            "agents_added": 0,
            "posts_added": 0,
            "analyzed_posts_added": 0,
            "flagged_posts_added": 0,
            "flagged_content_added": 0,
        }
    )

    agent_social_map: dict[str, dict] = {}
    agent_totals = load_agents_timeseries(agents_dir, hourly_counts, agent_social_map)

    nodes: dict[str, dict] = {}
    edges: dict[tuple[str, str], dict] = {}
    post_totals = process_posts(
        posts_dir,
        hourly_counts,
        allowed_ids=allowed_ids,
        nodes=nodes,
        edges=edges,
        exclude_self=args.exclude_self,
    )

    overall_stats = {
        "record_count": 0,
        "categories": {
            cat: {"sum": 0, "max": 0, "nonzero": 0, "levels": {0: 0, 1: 0, 2: 0, 3: 0}}
            for cat in CATEGORY_KEYS
        },
    }
    overall_examples = {cat: [] for cat in CATEGORY_KEYS}
    agent_stats: dict[str, dict] = {}

    post_score_totals = process_scores(
        post_scores_path,
        "post",
        hourly_counts,
        overall_stats=overall_stats,
        overall_examples=overall_examples,
        agent_stats=agent_stats,
        agent_names=agent_names,
        top_examples_limit=args.top_examples,
    )
    comment_score_totals = process_scores(
        comment_scores_path,
        "comment",
        hourly_counts,
        overall_stats=overall_stats,
        overall_examples=overall_examples,
        agent_stats=agent_stats,
        agent_names=agent_names,
        top_examples_limit=args.top_examples,
    )

    hourly_rows = []
    running_agents = 0
    running_posts = 0
    running_analyzed = 0
    running_flagged_posts = 0
    running_flagged_content = 0
    for hour in sorted(hourly_counts.keys()):
        counts = hourly_counts[hour]
        running_agents += counts["agents_added"]
        running_posts += counts["posts_added"]
        running_analyzed += counts["analyzed_posts_added"]
        running_flagged_posts += counts["flagged_posts_added"]
        running_flagged_content += counts["flagged_content_added"]
        hourly_rows.append(
            {
                "hour": iso_hour(hour),
                "agents_added": counts["agents_added"],
                "posts_added": counts["posts_added"],
                "analyzed_posts_added": counts["analyzed_posts_added"],
                "flagged_posts_added": counts["flagged_posts_added"],
                "flagged_content_added": counts["flagged_content_added"],
                "agents_total": running_agents,
                "posts_total": running_posts,
                "analyzed_posts_total": running_analyzed,
                "flagged_posts_total": running_flagged_posts,
                "flagged_content_total": running_flagged_content,
            }
        )

    overall_radar = {
        "record_count": overall_stats["record_count"],
        "categories": [],
    }
    for category in CATEGORY_KEYS:
        stats = overall_stats["categories"][category]
        avg = stats["sum"] / overall_stats["record_count"] if overall_stats["record_count"] else 0
        overall_radar["categories"].append(
            {
                "key": category,
                "avg_severity": round(avg, 3),
                "max_severity": stats["max"],
                "nonzero_records": stats["nonzero"],
                "level_counts": {
                    "0": stats["levels"].get(0, 0),
                    "1": stats["levels"].get(1, 0),
                    "2": stats["levels"].get(2, 0),
                    "3": stats["levels"].get(3, 0),
                },
                "top_examples": overall_examples[category],
            }
        )

    top_agents = []
    for stats in agent_stats.values():
        if stats.get("score_count", 0) == 0 or stats.get("nonzero_count", 0) == 0:
            continue
        risk = finalize_risk(stats)
        social_account = build_social_account(agent_social_map.get(stats.get("agent_id", "")))
        top_agents.append(
            {
                "agent_id": stats.get("agent_id"),
                "agent_name": stats.get("agent_name"),
                **risk,
                "radar": build_radar(stats),
                "top_content": stats.get("top_content", []),
                "top_flagged_content": stats.get("top_flagged_content", []),
                "risk_intent": derive_intent(stats.get("top_content", [])),
                "social_account": social_account,
            }
        )

    top_agents.sort(
        key=lambda r: (
            -r.get("risk_score_max", 0),
            -r.get("risk_nonzero_records", 0),
            -r.get("risk_score_avg", 0),
            r.get("agent_id") or "",
        )
    )
    top_agents = top_agents[: args.top_misaligned]

    interaction_graph = build_interaction_graph(
        nodes,
        edges,
        agent_profiles,
        agent_stats,
        include_layout=args.layout,
        layout_max_nodes=args.layout_max_nodes,
        max_nodes=100,
    )

    submolt_scatter = build_submolt_scatter(submolt_scores_path)


    output = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "totals": {
            "agents": agent_totals.get("total_agents", 0),
            "posts": post_totals.get("total_posts", 0),
            "analyzed_posts": post_score_totals.get("records_with_scores", 0),
            "posts_flagged": post_score_totals.get("flagged_records", 0),
            "comments_flagged": comment_score_totals.get("flagged_records", 0),
            "content_flagged": (
                post_score_totals.get("flagged_records", 0)
                + comment_score_totals.get("flagged_records", 0)
            ),
        },
        "time_series": {
            "hourly": hourly_rows,
        },
        "radar_overall": overall_radar,
        "submolt_scatter": submolt_scatter,
        "interaction_graph": {
            **interaction_graph,
            "meta": {
                "profiled_agents": len(allowed_ids),
                "nodes": len(interaction_graph["nodes"]),
                "edges": len(interaction_graph["edges"]),
                "posts_processed": post_totals.get("total_posts", 0),
                "comments_seen": post_totals.get("comments_seen", 0),
            },
        },
        "top_misaligned_agents": top_agents,
        "notes": {
            "agents_missing_created_at": agent_totals.get("agents_missing_created_at", 0),
            "posts_missing_created_at": post_totals.get("posts_missing_created_at", 0),
            "post_parse_errors": post_totals.get("posts_parse_errors", 0),
            "post_score_records": post_score_totals.get("records", 0),
            "comment_score_records": comment_score_totals.get("records", 0),
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print(f"Wrote: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
