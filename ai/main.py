from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Iterable, Iterator, Literal

from pydantic import BaseModel, conint, confloat
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider

# Allow importing prompt.py when running as a script.
HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from prompt import SYSTEM_PROMPT  # noqa: E402

CATEGORY_KEYS: tuple[str, ...] = (
    "none",
    "capability_misalignment",
    "instructional_subversion",
    "instrumental_convergence",
    "autonomy_replication",
    "deceptive_behavior",
    "sycophancy",
)


class SeverityByCategory(BaseModel):
    capability_misalignment: conint(ge=0, le=3)
    instructional_subversion: conint(ge=0, le=3)
    instrumental_convergence: conint(ge=0, le=3)
    autonomy_replication: conint(ge=0, le=3)
    deceptive_behavior: conint(ge=0, le=3)
    sycophancy: conint(ge=0, le=3)


class EvidenceItem(BaseModel):
    category: str
    severity: conint(ge=0, le=3)
    evidence_snippet: str
    explanation: str


class RiskResult(BaseModel):
    overall_misalignment_score: conint(ge=0, le=3)
    primary_risk_category: Literal[
        "none",
        "capability_misalignment",
        "instructional_subversion",
        "instrumental_convergence",
        "autonomy_replication",
        "deceptive_behavior",
        "sycophancy",
    ]
    severity_by_category: SeverityByCategory
    confidence: confloat(ge=0, le=1)
    evidence: list[EvidenceItem]
    summary: str
    recommended_status: Literal["ignore", "log", "review", "escalate"]


def resolve_posts_dir(input_path: Path) -> Path:
    if input_path.is_file():
        return input_path
    posts_dir = input_path / "data" / "posts"
    if posts_dir.is_dir():
        return posts_dir
    return input_path


def iter_post_files(input_path: Path) -> Iterator[Path]:
    resolved = resolve_posts_dir(input_path)
    if resolved.is_file():
        yield resolved
        return
    for path in sorted(resolved.glob("*.json")):
        yield path


def normalize_text(text: str) -> str:
    return " ".join(text.split())


def truncate(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return text
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def flatten_comments(comments: Iterable[dict]) -> Iterator[dict]:
    queue = list(comments)
    index = 0
    while index < len(queue):
        comment = queue[index]
        index += 1
        yield comment
        replies = comment.get("replies") or []
        if isinstance(replies, list):
            queue.extend(replies)

def build_comment_index(comments: Iterable[dict]) -> tuple[dict[str, dict], dict[str, str | None]]:
    by_id: dict[str, dict] = {}
    parent_of: dict[str, str | None] = {}

    def visit(comment: dict, parent_id: str | None) -> None:
        comment_id = comment.get("id")
        if not comment_id:
            return
        parent = comment.get("parent_id") or parent_id
        by_id[comment_id] = comment
        parent_of[comment_id] = parent
        replies = comment.get("replies") or []
        if isinstance(replies, list):
            for reply in replies:
                visit(reply, comment_id)

    for comment in comments:
        visit(comment, None)

    return by_id, parent_of


def find_thread_root_id(comment_id: str, parent_of: dict[str, str | None]) -> str:
    current = comment_id
    seen: set[str] = set()
    while current and current not in seen:
        seen.add(current)
        parent = parent_of.get(current)
        if not parent:
            break
        current = parent
    return current


def build_post_reference(
    payload: dict,
    *,
    max_content_chars: int,
    max_author_chars: int,
) -> list[str]:
    post = payload.get("post") or {}
    author = post.get("author") or {}
    submolt = post.get("submolt") or {}

    title = normalize_text(post.get("title") or "")
    content = normalize_text(post.get("content") or "")
    content = truncate(content, max_content_chars)

    author_name = normalize_text(author.get("name") or "")
    author_desc = normalize_text(author.get("description") or "")
    author_desc = truncate(author_desc, max_author_chars)

    submolt_name = normalize_text(submolt.get("display_name") or submolt.get("name") or "")

    parts = [
        f"Post ID: {post.get('id') or ''}",
        f"Created At: {post.get('created_at') or ''}",
        f"Submolt: {submolt_name}",
        f"Title: {title}",
        f"Content: {content}",
    ]

    if author_name or author_desc:
        parts.append(f"Author: {author_name}")
        if author_desc:
            parts.append(f"Author Description: {author_desc}")

    return parts


def build_post_prompt(
    payload: dict,
    *,
    max_content_chars: int,
    max_author_chars: int,
) -> str:
    parts = build_post_reference(
        payload, max_content_chars=max_content_chars, max_author_chars=max_author_chars
    )
    return "\n".join(["Post:"] + parts).strip()


def format_comment_line(comment: dict, max_chars: int, *, mark_target: bool) -> str:
    comment_id = comment.get("id") or ""
    parent_id = comment.get("parent_id") or ""
    author = comment.get("author") or {}
    author_name = normalize_text(author.get("name") or "")
    author_id = author.get("id") or ""
    author_label = author_name or author_id or "unknown"
    content = normalize_text(comment.get("content") or "")
    content = truncate(content, max_chars)
    prefix = "TARGET " if mark_target else ""
    return f"{prefix}[id={comment_id} parent={parent_id}] {author_label}: {content}"


def build_comment_prompt(
    payload: dict,
    *,
    root_comment: dict,
    target_comment_id: str,
    max_content_chars: int,
    max_author_chars: int,
    max_thread_comments: int,
    max_comment_chars: int,
) -> str:
    post_reference = build_post_reference(
        payload, max_content_chars=max_content_chars, max_author_chars=max_author_chars
    )
    root_id = root_comment.get("id") or ""
    lines = [
        "Post Reference:",
        *post_reference,
        "",
        f"Comment Thread (root_id={root_id}, target_id={target_comment_id}):",
    ]

    count = 0
    for comment in flatten_comments([root_comment]):
        if max_thread_comments and count >= max_thread_comments:
            break
        lines.append(
            f"- {format_comment_line(comment, max_comment_chars, mark_target=comment.get('id') == target_comment_id)}"
        )
        count += 1

    return "\n".join(lines).strip()


def load_seen_ids(output_path: Path, id_key: str) -> set[str]:
    seen: set[str] = set()
    if not output_path.exists():
        return seen
    with output_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            record_id = data.get(id_key)
            if record_id:
                seen.add(record_id)
    return seen


def empty_category_counts() -> dict[str, int]:
    return {key: 0 for key in CATEGORY_KEYS}


def ensure_profile(
    profiles: dict[str, dict],
    agent_id: str,
    *,
    agent_name: str | None,
) -> dict:
    if agent_id not in profiles:
        profiles[agent_id] = {
            "agent_id": agent_id,
            "agent_name": agent_name or "",
            "total_posts": 0,
            "total_comments": 0,
            "post_counts": empty_category_counts(),
            "comment_counts": empty_category_counts(),
        }
    profile = profiles[agent_id]
    if agent_name and not profile.get("agent_name"):
        profile["agent_name"] = agent_name
    return profile


def normalize_category(category: str | None) -> str:
    if not category:
        return "none"
    legacy_map = {
        "capability": "capability_misalignment",
        "instrumental": "instrumental_convergence",
        "autonomy": "autonomy_replication",
        "deceptive": "deceptive_behavior",
    }
    mapped = legacy_map.get(category, category)
    if mapped not in CATEGORY_KEYS:
        return "none"
    return mapped


def update_profile(
    profiles: dict[str, dict],
    *,
    agent_id: str | None,
    agent_name: str | None,
    category: str | None,
    record_type: Literal["post", "comment"],
) -> None:
    if not agent_id:
        return
    profile = ensure_profile(profiles, agent_id, agent_name=agent_name)
    normalized = normalize_category(category)
    if record_type == "post":
        profile["total_posts"] += 1
        profile["post_counts"][normalized] += 1
    else:
        profile["total_comments"] += 1
        profile["comment_counts"][normalized] += 1


def ensure_submolt(
    scores: dict[str, dict],
    submolt_id: str,
    *,
    submolt_name: str | None,
) -> dict:
    if submolt_id not in scores:
        scores[submolt_id] = {
            "submolt_id": submolt_id,
            "submolt_name": submolt_name or "",
            "total_posts": 0,
            "total_comments": 0,
            "post_counts": empty_category_counts(),
            "comment_counts": empty_category_counts(),
        }
    entry = scores[submolt_id]
    if submolt_name and not entry.get("submolt_name"):
        entry["submolt_name"] = submolt_name
    return entry


def update_submolt(
    scores: dict[str, dict],
    *,
    submolt_id: str | None,
    submolt_name: str | None,
    category: str | None,
    record_type: Literal["post", "comment"],
) -> None:
    if not submolt_id:
        return
    entry = ensure_submolt(scores, submolt_id, submolt_name=submolt_name)
    normalized = normalize_category(category)
    if record_type == "post":
        entry["total_posts"] += 1
        entry["post_counts"][normalized] += 1
    else:
        entry["total_comments"] += 1
        entry["comment_counts"][normalized] += 1


def load_agent_profiles(profile_path: Path) -> dict[str, dict]:
    if not profile_path.exists():
        return {}
    try:
        data = json.loads(profile_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    profiles: dict[str, dict] = {}
    for agent_id, profile in (data or {}).items():
        if not isinstance(profile, dict):
            continue
        profile.setdefault("agent_id", agent_id)
        profile.setdefault("agent_name", "")
        profile.setdefault("total_posts", 0)
        profile.setdefault("total_comments", 0)
        profile.setdefault("post_counts", empty_category_counts())
        profile.setdefault("comment_counts", empty_category_counts())
        for key in CATEGORY_KEYS:
            profile["post_counts"].setdefault(key, 0)
            profile["comment_counts"].setdefault(key, 0)
        profiles[agent_id] = profile
    return profiles


def load_submolt_scores(scores_path: Path) -> dict[str, dict]:
    if not scores_path.exists():
        return {}
    try:
        data = json.loads(scores_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    scores: dict[str, dict] = {}
    for submolt_id, entry in (data or {}).items():
        if not isinstance(entry, dict):
            continue
        entry.setdefault("submolt_id", submolt_id)
        entry.setdefault("submolt_name", "")
        entry.setdefault("total_posts", 0)
        entry.setdefault("total_comments", 0)
        entry.setdefault("post_counts", empty_category_counts())
        entry.setdefault("comment_counts", empty_category_counts())
        for key in CATEGORY_KEYS:
            entry["post_counts"].setdefault(key, 0)
            entry["comment_counts"].setdefault(key, 0)
        scores[submolt_id] = entry
    return scores


def rebuild_submolt_scores(
    *,
    post_output_path: Path,
    comment_output_path: Path,
) -> dict[str, dict]:
    scores: dict[str, dict] = {}
    post_to_submolt: dict[str, tuple[str, str]] = {}

    def ingest_posts(path: Path) -> None:
        if not path.exists():
            return
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                post_id = data.get("post_id")
                submolt_name = data.get("submolt") or "unknown"
                submolt_id = data.get("submolt_id") or submolt_name
                if post_id:
                    post_to_submolt[post_id] = (submolt_id, submolt_name)
                result = data.get("result") or {}
                update_submolt(
                    scores,
                    submolt_id=submolt_id,
                    submolt_name=submolt_name,
                    category=result.get("primary_risk_category"),
                    record_type="post",
                )

    def ingest_comments(path: Path) -> None:
        if not path.exists():
            return
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                post_id = data.get("post_id")
                if not post_id:
                    continue
                submolt_entry = post_to_submolt.get(post_id)
                if not submolt_entry:
                    continue
                submolt_id, submolt_name = submolt_entry
                result = data.get("result") or {}
                update_submolt(
                    scores,
                    submolt_id=submolt_id,
                    submolt_name=submolt_name,
                    category=result.get("primary_risk_category"),
                    record_type="comment",
                )

    ingest_posts(post_output_path)
    ingest_comments(comment_output_path)
    return scores


def write_json(path: Path, data: dict[str, dict]) -> None:
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

def rebuild_agent_profiles(
    *,
    post_output_path: Path,
    comment_output_path: Path,
) -> dict[str, dict]:
    profiles: dict[str, dict] = {}

    def ingest(path: Path, record_type: Literal["post", "comment"]) -> None:
        if not path.exists():
            return
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                result = data.get("result") or {}
                category = result.get("primary_risk_category")
                update_profile(
                    profiles,
                    agent_id=data.get("author_id"),
                    agent_name=data.get("author_name"),
                    category=category,
                    record_type=record_type,
                )

    ingest(post_output_path, "post")
    ingest(comment_output_path, "comment")
    return profiles


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze Moltbook posts and comments with PydanticAI."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("/home/hussain/hackathons/moltbook_data"),
        help="Path to Moltbook data root, posts directory, or a single post JSON file.",
    )
    parser.add_argument(
        "--output",
        dest="post_output",
        type=Path,
        help="Deprecated alias for --post-output.",
    )
    parser.add_argument(
        "--post-output",
        type=Path,
        default=Path("/home/hussain/hackathons/moltbook-riskmap/data/post_scores.jsonl"),
        help="Output JSONL path for post analysis results.",
    )
    parser.add_argument(
        "--comment-output",
        type=Path,
        default=Path("/home/hussain/hackathons/moltbook-riskmap/data/comment_scores.jsonl"),
        help="Output JSONL path for comment analysis results.",
    )
    parser.add_argument(
        "--agent-profile-output",
        type=Path,
        default=Path("/home/hussain/hackathons/moltbook-riskmap/data/agent_profiles.json"),
        help="Output JSON path for agent profile aggregates.",
    )
    parser.add_argument(
        "--submolt-output",
        type=Path,
        default=Path("/home/hussain/hackathons/moltbook-riskmap/data/submolt_scores.json"),
        help="Output JSON path for submolt aggregates.",
    )
    parser.add_argument(
        "--model",
        default=os.getenv("OPENROUTER_MODEL")
        or os.getenv("INFERENCE_GATEWAY_MODEL")
        or os.getenv("OLLAMA_MODEL")
        or "openai/gpt-oss-120b",
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("OPENROUTER_BASE_URL")
        or os.getenv("INFERENCE_GATEWAY_BASE_URL")
        or os.getenv("OLLAMA_BASE_URL")
        or "https://openrouter.ai/api/v1",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("OPENROUTER_API_KEY")
        or os.getenv("INFERENCE_GATEWAY_API_KEY")
        or os.getenv("PROJECTSQ_API_KEY")
        or os.getenv("OLLAMA_API_KEY")
        or "ollama",
    )
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument(
        "--limit",
        dest="post_limit",
        type=int,
        help="Deprecated alias for --post-limit.",
    )
    parser.add_argument(
        "--post-limit",
        dest="post_limit",
        type=int,
        default=0,
        help="Maximum number of posts to process (0 = no limit).",
    )
    parser.add_argument(
        "--comment-limit",
        type=int,
        default=0,
        help="Maximum number of comments to process (0 = no limit).",
    )
    parser.add_argument("--offset", type=int, default=0, help="Number of posts to skip from the start.")
    parser.add_argument(
        "--resume",
        dest="resume",
        action="store_true",
        default=True,
        help="Skip posts already present in output JSONL (default: true).",
    )
    parser.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Reprocess posts even if they exist in output JSONL.",
    )
    parser.add_argument("--sleep", type=float, default=0.0, help="Seconds to sleep between requests.")
    parser.add_argument("--max-content-chars", type=int, default=4000)
    parser.add_argument(
        "--max-comments",
        dest="max_thread_comments",
        type=int,
        help="Deprecated alias for --max-thread-comments.",
    )
    parser.add_argument(
        "--max-thread-comments",
        dest="max_thread_comments",
        type=int,
        default=20,
        help="Maximum number of comments to include per comment thread prompt.",
    )
    parser.add_argument("--max-comment-chars", type=int, default=600)
    parser.add_argument("--dry-run", action="store_true", help="Print prompt for the first post and exit.")
    parser.set_defaults(
        post_output=Path("/home/hussain/hackathons/moltbook-riskmap/data/post_scores.jsonl"),
        post_limit=0,
        max_thread_comments=20,
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.dry_run and not args.api_key:
        print(
            "Missing API key. Set OPENROUTER_API_KEY "
            "(or INFERENCE_GATEWAY_API_KEY/PROJECTSQ_API_KEY) "
            "or pass --api-key.",
            file=sys.stderr,
        )
        return 2

    provider = OpenAIProvider(base_url=args.base_url, api_key=args.api_key)
    model = OpenAIChatModel(args.model, provider=provider)
    agent = Agent(model, system_prompt=SYSTEM_PROMPT, output_type=RiskResult)

    post_output_path: Path = args.post_output
    comment_output_path: Path = args.comment_output
    profile_output_path: Path = args.agent_profile_output
    submolt_output_path: Path = args.submolt_output

    post_output_path.parent.mkdir(parents=True, exist_ok=True)
    comment_output_path.parent.mkdir(parents=True, exist_ok=True)
    profile_output_path.parent.mkdir(parents=True, exist_ok=True)
    submolt_output_path.parent.mkdir(parents=True, exist_ok=True)

    seen_ids: set[str] = set()
    if args.resume:
        seen_ids = load_seen_ids(post_output_path, "post_id")

    seen_comment_ids: set[str] = set()
    if args.resume:
        seen_comment_ids = load_seen_ids(comment_output_path, "comment_id")

    profiles = load_agent_profiles(profile_output_path)
    if args.resume and not profiles and (post_output_path.exists() or comment_output_path.exists()):
        profiles = rebuild_agent_profiles(
            post_output_path=post_output_path, comment_output_path=comment_output_path
        )

    submolt_scores = load_submolt_scores(submolt_output_path)
    if args.resume and not submolt_scores and (post_output_path.exists() or comment_output_path.exists()):
        submolt_scores = rebuild_submolt_scores(
            post_output_path=post_output_path, comment_output_path=comment_output_path
        )

    files = iter_post_files(args.input)
    post_processed = 0
    comment_processed = 0
    scanned = 0
    comment_limit_reached = False

    with post_output_path.open("a", encoding="utf-8") as post_out, comment_output_path.open(
        "a", encoding="utf-8"
    ) as comment_out:
        for path in files:
            scanned += 1
            if args.offset and scanned <= args.offset:
                continue
            if args.post_limit and post_processed >= args.post_limit:
                break

            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue

            post = payload.get("post") or {}
            post_id = post.get("id")
            if not post_id:
                continue

            author = post.get("author") or {}
            submolt_data = post.get("submolt") or {}
            submolt_id = submolt_data.get("id") or submolt_data.get("name") or ""
            submolt_name = (
                submolt_data.get("display_name") or submolt_data.get("name") or submolt_id
            )
            author_id = author.get("id")
            author_name = normalize_text(author.get("name") or "")

            if args.dry_run:
                print(
                    build_post_prompt(
                        payload,
                        max_content_chars=args.max_content_chars,
                        max_author_chars=args.max_comment_chars,
                    )
                )
                comments = payload.get("comments") or []
                if comments:
                    by_id, parent_of = build_comment_index(comments)
                    first_comment = next(flatten_comments(comments), None)
                    if first_comment and first_comment.get("id") in by_id:
                        root_id = find_thread_root_id(first_comment["id"], parent_of)
                        root_comment = by_id.get(root_id, first_comment)
                        print()
                        print(
                            build_comment_prompt(
                                payload,
                                root_comment=root_comment,
                                target_comment_id=first_comment["id"],
                                max_content_chars=args.max_content_chars,
                                max_author_chars=args.max_comment_chars,
                                max_thread_comments=args.max_thread_comments,
                                max_comment_chars=args.max_comment_chars,
                            )
                        )
                return 0

            if post_id not in seen_ids:
                post_prompt = build_post_prompt(
                    payload,
                    max_content_chars=args.max_content_chars,
                    max_author_chars=args.max_comment_chars,
                )
                try:
                    run_result = agent.run_sync(
                        post_prompt, model_settings={"temperature": args.temperature}
                    )
                    result_data = (
                        run_result.output if hasattr(run_result, "output") else run_result
                    )
                except Exception as exc:  # noqa: BLE001
                    error_record = {
                        "record_type": "post",
                        "post_id": post_id,
                        "source_file": str(path),
                        "error": str(exc),
                        "model": args.model,
                    }
                    post_out.write(json.dumps(error_record, ensure_ascii=False) + "\n")
                    post_out.flush()
                    if args.sleep:
                        time.sleep(args.sleep)
                else:
                    record = {
                        "record_type": "post",
                        "post_id": post_id,
                        "title": post.get("title"),
                        "created_at": post.get("created_at"),
                        "submolt_id": submolt_id,
                        "submolt": submolt_name,
                        "author_id": author_id,
                        "author_name": author_name,
                        "model": args.model,
                        "result": result_data.model_dump(),
                    }
                    post_out.write(json.dumps(record, ensure_ascii=False) + "\n")
                    post_out.flush()
                    seen_ids.add(post_id)
                    post_processed += 1
                    update_profile(
                        profiles,
                        agent_id=author_id,
                        agent_name=author_name,
                        category=result_data.primary_risk_category,
                        record_type="post",
                    )
                    update_submolt(
                        submolt_scores,
                        submolt_id=submolt_id,
                        submolt_name=submolt_name,
                        category=result_data.primary_risk_category,
                        record_type="post",
                    )
                    write_json(profile_output_path, profiles)
                    write_json(submolt_output_path, submolt_scores)
                    if args.sleep:
                        time.sleep(args.sleep)

            if comment_limit_reached:
                continue

            comments = payload.get("comments") or []
            if not comments:
                continue

            by_id, parent_of = build_comment_index(comments)
            if not by_id:
                continue

            for comment in flatten_comments(comments):
                if args.comment_limit and comment_processed >= args.comment_limit:
                    comment_limit_reached = True
                    break
                comment_id = comment.get("id")
                if not comment_id:
                    continue
                if comment_id in seen_comment_ids:
                    continue

                root_id = find_thread_root_id(comment_id, parent_of)
                root_comment = by_id.get(root_id)
                if not root_comment:
                    continue

                comment_prompt = build_comment_prompt(
                    payload,
                    root_comment=root_comment,
                    target_comment_id=comment_id,
                    max_content_chars=args.max_content_chars,
                    max_author_chars=args.max_comment_chars,
                    max_thread_comments=args.max_thread_comments,
                    max_comment_chars=args.max_comment_chars,
                )

                try:
                    run_result = agent.run_sync(
                        comment_prompt, model_settings={"temperature": args.temperature}
                    )
                    result_data = (
                        run_result.output if hasattr(run_result, "output") else run_result
                    )
                except Exception as exc:  # noqa: BLE001
                    error_record = {
                        "record_type": "comment",
                        "comment_id": comment_id,
                        "post_id": post_id,
                        "source_file": str(path),
                        "error": str(exc),
                        "model": args.model,
                    }
                    comment_out.write(json.dumps(error_record, ensure_ascii=False) + "\n")
                    comment_out.flush()
                    if args.sleep:
                        time.sleep(args.sleep)
                    continue

                comment_author = comment.get("author") or {}
                comment_author_id = comment_author.get("id")
                comment_author_name = normalize_text(comment_author.get("name") or "")

                record = {
                    "record_type": "comment",
                    "comment_id": comment_id,
                    "post_id": post_id,
                    "thread_root_id": root_id,
                    "parent_id": comment.get("parent_id"),
                    "created_at": comment.get("created_at"),
                    "author_id": comment_author_id,
                    "author_name": comment_author_name,
                    "model": args.model,
                    "result": result_data.model_dump(),
                }
                comment_out.write(json.dumps(record, ensure_ascii=False) + "\n")
                comment_out.flush()
                seen_comment_ids.add(comment_id)
                comment_processed += 1
                update_profile(
                    profiles,
                    agent_id=comment_author_id,
                    agent_name=comment_author_name,
                    category=result_data.primary_risk_category,
                    record_type="comment",
                )
                update_submolt(
                    submolt_scores,
                    submolt_id=submolt_id,
                    submolt_name=submolt_name,
                    category=result_data.primary_risk_category,
                    record_type="comment",
                )
                write_json(profile_output_path, profiles)
                write_json(submolt_output_path, submolt_scores)
                if args.sleep:
                    time.sleep(args.sleep)

    write_json(profile_output_path, profiles)
    write_json(submolt_output_path, submolt_scores)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
