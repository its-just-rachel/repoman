#!/usr/bin/env python3
"""
Frontier Tech — Ingestion Pipeline v1
======================================
Fetches signals from:
  - HuggingFace Daily Papers API     (academic, daily)
  - HuggingFace Hub Models API       (api, real-time polling)
  - Active RSS sources               (loaded dynamically from Airtable Sources table)

For each source:
  1. Fetch raw items
  2. Skip anything already in Airtable (URL dedup)
  3. Skip items older than LOOKBACK_DAYS
  4. Enrich surviving items in batches via Claude Haiku (cheap, fast)
  5. Write enriched signals to Airtable
  6. Log an Evaluation Run record

Usage:
  python main.py                  # run all sources
  python main.py --dry-run        # fetch + enrich, skip Airtable writes
  python main.py --source papers  # run one source (papers | models | rss)
"""

import argparse
import calendar
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from html import unescape
from typing import Any

import feedparser
import requests
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("frontier_tech")


# ──────────────────────────────────────────────────────────────────────────────
# ENVIRONMENT
# ──────────────────────────────────────────────────────────────────────────────

AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN", "")
ANTHROPIC_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")

if not AIRTABLE_TOKEN:
    sys.exit("ERROR: AIRTABLE_TOKEN is not set. Copy .env.example → .env and fill it in.")
if not ANTHROPIC_KEY:
    sys.exit("ERROR: ANTHROPIC_API_KEY is not set. Copy .env.example → .env and fill it in.")


# ──────────────────────────────────────────────────────────────────────────────
# AIRTABLE CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────

BASE_ID      = "appwe0lxRHbASBgG2"
T_SIGNALS    = "tblvgGIVerqksC5FP"
T_SOURCES    = "tblWu4dga8m9mKijj"
T_EVAL_RUNS  = "tbltRbDSKKvMSRYJM"

# Signal field IDs
SF = dict(
    title                = "fldQ9K0UMFs6jWpGe",
    url                  = "fldXOHzAs204eXeSs",
    author               = "fldifKZdKcXjXjV2S",
    published_at         = "fldloljvjzyBUSsIg",
    captured_at          = "fldawWCM7vZX2uzEi",
    raw_excerpt          = "fldKNOO5saBFPlyOR",
    summary              = "fldna78zA1fOq0DnK",
    quadrant             = "fldLM1e8FBlDPGhwE",
    verticals            = "fld24KF4Nvn9l9xzh",
    pipeline_status      = "fldkQQ3r2Gf0oISBr",
    confidence_score     = "fldJSJSvAkqiu6Bp1",
    fidelity_score       = "fldODAwYaCg5sSgzP",
    availability_score   = "fldqnTmusDhhnWlDr",
    corroboration_count  = "fldGk8nnfbqG5PchE",
    enrichment_rationale = "fldbimOOeZk5cf2MN",
    pipeline_suggestion  = "fldXSJhsDdORTGafM",
    source               = "fldvqlOKqvlIOM7FP",   # multipleRecordLinks → Sources
    tl_dr                = "fldRDTrdresooZpZC",   # singleSelect: Tech Lead / Don't Read
    why_read_skip        = "fldgcrntMCFY4uXsF",   # long text: Why Read or Why Skip rationale
    submission_source    = "fldr1cfzD7V3bpjFg",   # singleSelect: Pipeline / Submitted
)

# Evaluation Run field IDs
EF = dict(
    run_at              = "fld7fB7iw969CQelz",
    run_type            = "flduFxW5mun7DgoeN",
    sources_evaluated   = "fld3RoLAn0Z7r0Qv9",
    sources_discovered  = "fld4eaCDHDQVSpIed",
    sources_deactivated = "fldxIAJMGBEUkeUGA",
    tier_changes        = "fld4WqpDEcZvKFmhR",
    errors              = "fldQvZWDkYOvPBJ16",
    model_used          = "fldEnmx3yLpI3wTr6",
    prompt_version      = "fldm6VwVW4QuiDo9Q",
)


# ──────────────────────────────────────────────────────────────────────────────
# SOURCE DEFINITIONS
# ──────────────────────────────────────────────────────────────────────────────

HF_PAPERS_SOURCE_ID  = "recDq5d2NTrgWmwp9"
HF_MODELS_SOURCE_ID  = "recPC61U0alTeD28K"
ARXIV_SOURCE_ID      = "recWRTkdybGkatYWz"
HARMONIC_SOURCE_ID   = "recUUe3LWP24VlO8a"
HN_SOURCE_ID         = "reciB2Pf2VfL7JyvB"
GITHUB_SOURCE_ID     = "reczmrDR8GsUCNv4i"
HF_LEADERBOARD_SRC   = "recFnNrVms6gy0Z5d"
ARENA_SOURCE_ID      = "recGAVr2eaGsPzhHY"


# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE SETTINGS
# ──────────────────────────────────────────────────────────────────────────────

# Only consider items published within this window
LOOKBACK_DAYS = 14

# ArXiv categories to monitor
ARXIV_CATEGORIES  = ["cs.AI", "cs.LG", "cs.CL", "cs.CV", "cs.RO"]
ARXIV_MAX_RESULTS = 30    # per run across all categories

# Harmonic Scout reports directory (relative to this file)
HARMONIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "..", "sources", "harmonic")

# HF Models noise filters
HF_MODELS_LIMIT     = 30   # max models to inspect per run
HF_MODELS_MIN_LIKES = 10   # skip models with fewer likes than this

# HackerNews (Algolia API — no auth required)
HN_MIN_POINTS  = 20   # skip stories with fewer points
HN_AI_QUERIES  = [    # run each query; results are merged + deduped by story ID
    "large language model LLM",
    "AI agent reasoning",
    "diffusion model image generation",
    "transformer architecture neural network",
    "foundation model multimodal",
]

# GitHub Topics Search API
GITHUB_TOPICS    = ["llm", "ai-agent", "large-language-model",
                    "diffusion-model", "ai-safety", "transformer"]
GITHUB_MIN_STARS = 100   # skip repos below this star count
GITHUB_PER_TOPIC = 15    # max repos per topic per run
# Set GITHUB_TOKEN in .env for 5000 req/hr (vs 60 unauthenticated)

# Leaderboards — snapshot-diff (only emit signals for NEW top-N entries)
LEADERBOARD_TOP_N      = 20   # track top N models per leaderboard
LEADERBOARD_SNAPSHOT   = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "sources", "leaderboard_snapshot.json",
)

# Enrichment — Haiku for classification + brief summary (all signals)
ENRICH_MODEL   = "claude-haiku-4-5-20251001"
PROMPT_VERSION = "v1.1"
BATCH_SIZE     = 10          # items per Haiku call

# Insight pass — Sonnet for deep "why it matters" summaries (high-relevance signals only)
INSIGHT_MODEL          = "claude-sonnet-4-6"
INSIGHT_STATUSES       = {"Surface", "Research"}   # only signals worth the spend
INSIGHT_MIN_CONFIDENCE = 0.65                       # skip uncertain classifications
INSIGHT_BATCH_SIZE     = 5                          # smaller batches; Sonnet is slower

# Valid classification choices (must match Airtable singleSelect/multipleSelects options)
VALID_QUADRANTS = ["Tools", "Techniques", "Platforms", "Frameworks & Languages"]
VALID_VERTICALS = [
    "AI Systems", "NLP / Language", "Computer Vision", "Audio", "Multimodal",
    "Reinforcement Learning", "Infrastructure", "Security & Trust", "Edge / On-Device",
]
VALID_NEW_STATUSES = ["Surface", "Research", "Hold", "Dismissed"]


# ──────────────────────────────────────────────────────────────────────────────
# ENRICHMENT PROMPTS
# ──────────────────────────────────────────────────────────────────────────────

ENRICH_SYSTEM = """You are classifying AI and emerging-technology signals for a research pipeline.
The pipeline uses a Tech Radar model. Quadrant definitions:
- Tools: concrete products, APIs, or libraries you can use today
- Techniques: approaches, algorithms, methods, training strategies, architectures
- Platforms: infrastructure, cloud services, hardware, deployment environments
- Frameworks & Languages: SDKs, programming models, foundation model families

Return only valid JSON. Be concise."""

ENRICH_PROMPT = """\
Classify each signal and return a JSON array — one object per signal, same order, no other text.

Required keys per object:
  quadrant             one of {quadrants}
  verticals            array, 1–3 items from {verticals}
  pipeline_status      "Surface" for most new signals | "Research" if already well-established |
                       "Hold" if low relevance | "Dismissed" if clearly off-topic
  confidence_score     float 0.0–1.0 — confidence in your classification
  fidelity_score       float 0–10 — how validated is the underlying claim
                         (10 = peer-reviewed & reproducible; 1 = speculation/rumor)
  availability_score   float 0–10 — how deployable/accessible today
                         (10 = open-source, production-ready; 1 = theoretical/unavailable)
  summary              2 sentences: what it is (be specific — name the method/tool/approach)
  enrichment_rationale 1 sentence explaining quadrant + vertical choices
  pipeline_suggestion  recommended stage after human review — same options as pipeline_status
                       plus "Prototype" or "Advise" for mature signals

Signals to classify:
{signals_json}"""

# ── Insight pass prompts (Sonnet — high-relevance signals only) ───────────────

INSIGHT_SYSTEM = """You write technology signal summaries for an AI research radar used by \
senior engineers and technical leaders who make technology decisions. \
Your readers are experienced practitioners — they want substance, specifics, and implications, \
not marketing language or vague praise."""

INSIGHT_PROMPT = """\
Write a deep insight for each signal. Cover these three things in 4–6 sentences total:
1. What it is — the core technical contribution. Name the specific approach, method, model, \
or capability. Be concrete.
2. Why it matters — technical significance, what problem it solves or advances, \
how it moves the needle relative to prior work or current practice.
3. Practitioner take — what should an engineer or technical leader do with this? \
Watch it, evaluate it, prototype with it, or act on it now? Why?

No vague superlatives. No "this is exciting." Write like you're briefing a skeptical \
staff engineer who has seen a lot of hype.

Also classify each signal for executive leadership:
- "Tech Lead": leadership SHOULD know about this — novel capability, market-moving development, \
competitive threat, or paradigm shift that would embarrass them not to know a quarter from now.
- "Don't Read": will circulate widely but safe to skip — known hype cycle, incremental update, \
too niche, or too early-stage to act on.
- null: insufficient signal to classify confidently.

Return a JSON array — one object per signal, same order as input, no other text.
Each object:
{{
  "insight": "4-6 sentence deep summary",
  "tl_dr": "Tech Lead" or "Don't Read" or null,
  "why_read_skip": "2-3 sentence exec-readable Why Read or Why Skip rationale"
}}

Signals:
{signals_json}"""


# ──────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ──────────────────────────────────────────────────────────────────────────────

def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", text or "")
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _iso(dt: datetime) -> str:
    """Format a datetime as an Airtable-compatible ISO 8601 string."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _parse_iso(s: str):
    """Parse an ISO 8601 string; return None on failure."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _struct_to_dt(st):
    """Convert a feedparser struct_time to an aware datetime (UTC)."""
    if st is None:
        return None
    try:
        return datetime.fromtimestamp(calendar.timegm(st), tz=timezone.utc)
    except Exception:
        return None


def _clamp(v, lo, hi, default):
    try:
        return max(lo, min(hi, float(v)))
    except (TypeError, ValueError):
        return default


# ──────────────────────────────────────────────────────────────────────────────
# AIRTABLE HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _at_headers() -> dict:
    return {"Authorization": f"Bearer {AIRTABLE_TOKEN}", "Content-Type": "application/json"}


def _at_url(table: str) -> str:
    return f"https://api.airtable.com/v0/{BASE_ID}/{table}"


def get_existing_urls() -> set[str]:
    """
    Page through the Signals table and collect every URL.
    Used to deduplicate incoming items before enrichment.
    """
    urls: set[str] = set()
    # Airtable returns fields keyed by field NAME (not ID), so query by name
    params: dict[str, Any] = {"fields[]": ["URL"], "pageSize": 100}

    while True:
        resp = requests.get(_at_url(T_SIGNALS), headers=_at_headers(), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for rec in data.get("records", []):
            url = rec.get("fields", {}).get("URL")
            if url:
                urls.add(url)
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset

    log.info("Loaded %d existing signal URLs for dedup", len(urls))
    return urls


def load_rss_sources() -> list[dict]:
    """
    Fetch all active RSS sources from the Sources table.
    Returns [{"id": record_id, "name": name, "url": feed_url}, ...]

    To add or remove an RSS source, toggle the Active checkbox in Airtable —
    no code changes needed.
    """
    sources = []
    params: dict[str, Any] = {
        "filterByFormula": 'AND({Active}, {Source Type} = "rss")',
        "fields[]": ["Name", "URL"],
        "pageSize": 100,
    }
    while True:
        resp = requests.get(_at_url(T_SOURCES), headers=_at_headers(), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            name = fields.get("Name", "")
            url  = fields.get("URL", "")
            if name and url:
                sources.append({"id": rec["id"], "name": name, "url": url})
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset
    log.info("Loaded %d active RSS sources from Airtable", len(sources))
    return sources


def write_signals(signals: list[dict], dry_run: bool = False) -> int:
    """
    Write enriched signals to Airtable in batches of 10.
    Returns the number of records written (0 if dry_run).
    """
    if not signals:
        return 0

    if dry_run:
        log.info("[DRY RUN] Would write %d signals — skipping Airtable writes", len(signals))
        for s in signals[:3]:
            log.info("  Sample: %s | %s | %s",
                     s.get(SF["title"], "")[:60],
                     s.get(SF["quadrant"], ""),
                     s.get(SF["pipeline_status"], ""))
        return 0

    written = 0
    for i in range(0, len(signals), 10):
        batch = signals[i : i + 10]
        payload = {"records": [{"fields": s} for s in batch]}
        resp = requests.post(_at_url(T_SIGNALS), headers=_at_headers(), json=payload, timeout=30)
        resp.raise_for_status()
        written += len(batch)
        if i + 10 < len(signals):
            time.sleep(0.25)  # stay within Airtable's 5 req/sec limit

    return written


def write_eval_run(
    run_type: str,
    sources_evaluated: int,
    sources_discovered: int,
    errors: list[str],
    dry_run: bool = False,
) -> None:
    if dry_run:
        return
    fields = {
        EF["run_at"]:              _iso(datetime.now(timezone.utc)),
        EF["run_type"]:            run_type,
        EF["sources_evaluated"]:   sources_evaluated,
        EF["sources_discovered"]:  sources_discovered,
        EF["sources_deactivated"]: 0,
        EF["model_used"]:          ENRICH_MODEL,
        EF["prompt_version"]:      PROMPT_VERSION,
    }
    if errors:
        fields[EF["errors"]] = "\n".join(errors)

    resp = requests.post(
        _at_url(T_EVAL_RUNS),
        headers=_at_headers(),
        json={"records": [{"fields": fields}]},
        timeout=30,
    )
    resp.raise_for_status()


# ──────────────────────────────────────────────────────────────────────────────
# SOURCE FETCHERS — return list of raw item dicts
#
# Each raw item has:
#   _url          str   canonical URL (used for dedup + Airtable)
#   _source_id    str   Airtable record ID of the Source row
#   _title        str
#   _excerpt      str   raw text ≤ 1000 chars (goes to raw_excerpt + enrichment input)
#   _author       str   optional
#   _published_at str   ISO 8601 or ""
# ──────────────────────────────────────────────────────────────────────────────

def fetch_hf_papers(existing_urls: set[str]) -> tuple[list[dict], list[str]]:
    raw, errors = [], []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    try:
        resp = requests.get(
            "https://huggingface.co/api/daily_papers",
            params={"limit": 50},
            timeout=30,
        )
        resp.raise_for_status()
        papers = resp.json()
    except Exception as e:
        msg = f"HF Papers fetch failed: {e}"
        log.error(msg)
        return [], [msg]

    for item in papers:
        # The API wraps each entry: { "paper": {...}, "publishedAt": "...", "upvotes": N }
        p = item.get("paper", item)
        arxiv_id = p.get("id", "")
        url = f"https://huggingface.co/papers/{arxiv_id}" if arxiv_id else ""
        if not url or url in existing_urls:
            continue

        # Date filter
        pub_str = p.get("publishedAt") or item.get("publishedAt", "")
        pub_dt = _parse_iso(pub_str)
        if pub_dt and pub_dt < cutoff:
            continue

        authors = [a.get("name", "") for a in p.get("authors", [])[:3]]
        excerpt = (p.get("summary") or p.get("abstract") or "")[:1000]

        raw.append({
            "_url":          url,
            "_source_id":    HF_PAPERS_SOURCE_ID,
            "_title":        p.get("title", arxiv_id),
            "_excerpt":      excerpt,
            "_author":       ", ".join(filter(None, authors)),
            "_published_at": _iso(pub_dt) if pub_dt else "",
        })

    log.info("HF Papers:  %d new items", len(raw))
    return raw, errors


def fetch_hf_models(existing_urls: set[str]) -> tuple[list[dict], list[str]]:
    raw, errors = [], []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    try:
        resp = requests.get(
            "https://huggingface.co/api/models",
            params={
                "sort":      "lastModified",
                "direction": -1,
                "limit":     HF_MODELS_LIMIT,
                "full":      "true",
            },
            timeout=30,
        )
        resp.raise_for_status()
        models = resp.json()
    except Exception as e:
        msg = f"HF Models fetch failed: {e}"
        log.error(msg)
        return [], [msg]

    for model in models:
        model_id = model.get("modelId") or model.get("id", "")
        if not model_id:
            continue

        url = f"https://huggingface.co/{model_id}"
        if url in existing_urls:
            continue

        # Noise filter
        if (model.get("likes") or 0) < HF_MODELS_MIN_LIKES:
            continue

        # Date filter
        mod_str = model.get("lastModified", "")
        mod_dt = _parse_iso(mod_str)
        if mod_dt and mod_dt < cutoff:
            continue

        # Build a descriptive excerpt from model metadata
        parts = []
        if model.get("pipeline_tag"):
            parts.append(f"Task: {model['pipeline_tag']}")
        tags = model.get("tags") or []
        if tags:
            parts.append(f"Tags: {', '.join(tags[:8])}")
        parts.append(f"Likes: {model.get('likes', 0)}  Downloads: {model.get('downloads', 0)}")
        card = (model.get("cardData") or {}).get("model_summary", "")
        if card:
            parts.insert(0, card[:300])

        author = model.get("author") or (model_id.split("/")[0] if "/" in model_id else "")

        raw.append({
            "_url":          url,
            "_source_id":    HF_MODELS_SOURCE_ID,
            "_title":        model_id,
            "_excerpt":      " | ".join(parts)[:1000],
            "_author":       author,
            "_published_at": _iso(mod_dt) if mod_dt else "",
        })

    log.info("HF Models:  %d new items (after filters)", len(raw))
    return raw, errors


def load_reddit_sources() -> list[dict]:
    """
    Fetch all active community sources (subreddits) from the Sources table.
    Returns [{"id": record_id, "name": name, "url": subreddit_base_url}, ...]
    """
    sources = []
    params: dict[str, Any] = {
        "filterByFormula": 'AND({Active}, {Source Type} = "community")',
        "fields[]": ["Name", "URL"],
        "pageSize": 100,
    }
    while True:
        resp = requests.get(_at_url(T_SOURCES), headers=_at_headers(), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            name = fields.get("Name", "")
            url  = fields.get("URL", "")
            if name and url and "reddit.com" in url:
                sources.append({"id": rec["id"], "name": name, "url": url})
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset
    log.info("Loaded %d Reddit sources from Airtable", len(sources))
    return sources


def fetch_reddit(existing_urls: set[str], reddit_sources: list[dict]) -> tuple[list[dict], list[str]]:
    """
    Fetch top posts from the past week for each subreddit via Reddit JSON API.
    Uses unauthenticated access (60 req/min limit) — sufficient for daily pipeline.
    To upgrade to OAuth, set REDDIT_CLIENT_ID + REDDIT_CLIENT_SECRET in .env.
    """
    raw, errors = [], []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    reddit_id     = os.environ.get("REDDIT_CLIENT_ID", "")
    reddit_secret = os.environ.get("REDDIT_CLIENT_SECRET", "")
    use_oauth     = bool(reddit_id and reddit_secret)

    session = requests.Session()
    session.headers.update({"User-Agent": "frontier-tech-pipeline/1.0 (signal intelligence pipeline)"})

    if use_oauth:
        try:
            token_resp = session.post(
                "https://www.reddit.com/api/v1/access_token",
                auth=(reddit_id, reddit_secret),
                data={"grant_type": "client_credentials"},
                timeout=15,
            )
            token_resp.raise_for_status()
            token = token_resp.json().get("access_token", "")
            session.headers.update({"Authorization": f"Bearer {token}"})
            api_base = "https://oauth.reddit.com"
            log.info("Reddit: using OAuth (authenticated)")
        except Exception as e:
            log.warning("Reddit OAuth failed (%s) — falling back to unauthenticated", e)
            use_oauth = False
            api_base = "https://www.reddit.com"
    else:
        api_base = "https://www.reddit.com"
        log.info("Reddit: using unauthenticated access")

    for source in reddit_sources:
        source_count = 0
        # Extract subreddit path from URL (e.g. https://www.reddit.com/r/MachineLearning → /r/MachineLearning)
        subreddit_path = source["url"].replace("https://www.reddit.com", "").rstrip("/")
        api_url = f"{api_base}{subreddit_path}/top.json"

        try:
            resp = session.get(api_url, params={"t": "week", "limit": 100}, timeout=30)
            resp.raise_for_status()
            posts = resp.json().get("data", {}).get("children", [])

            for post in posts:
                p = post.get("data", {})

                # Use reddit permalink for self-posts, external URL for link posts
                is_self    = p.get("is_self", False)
                ext_url    = p.get("url", "")
                permalink  = f"https://www.reddit.com{p.get('permalink', '')}"
                signal_url = permalink if is_self else ext_url

                if not signal_url or signal_url in existing_urls:
                    continue

                # Date filter
                created_utc = p.get("created_utc", 0)
                pub_dt = datetime.fromtimestamp(created_utc, tz=timezone.utc) if created_utc else None
                if pub_dt and pub_dt < cutoff:
                    continue

                # Build excerpt: selftext body + engagement stats + link if external
                parts = []
                selftext = (p.get("selftext") or "").strip()
                if selftext and selftext not in ("[deleted]", "[removed]"):
                    parts.append(selftext[:600])
                parts.append(f"Score: {p.get('score', 0)} | Comments: {p.get('num_comments', 0)}")
                if not is_self and ext_url:
                    parts.append(f"Link: {ext_url}")

                raw.append({
                    "_url":          signal_url,
                    "_source_id":    source["id"],
                    "_title":        p.get("title", ""),
                    "_excerpt":      " | ".join(parts)[:1000],
                    "_author":       p.get("author", ""),
                    "_published_at": _iso(pub_dt) if pub_dt else "",
                })
                source_count += 1

            log.info("Reddit %-24s %d new items", source["name"] + ":", source_count)
            time.sleep(0.5)  # gentle pacing between subreddit requests

        except Exception as e:
            msg = f"Reddit {source['name']} failed: {e}"
            log.warning(msg)
            errors.append(msg)

    return raw, errors


def fetch_rss_feeds(existing_urls: set[str], rss_sources: list[dict]) -> tuple[list[dict], list[str]]:
    raw, errors = [], []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    for source in rss_sources:
        source_count = 0
        try:
            feed = feedparser.parse(source["url"])
            if feed.bozo and not feed.entries:
                raise ValueError(f"feedparser error: {feed.bozo_exception}")

            for entry in feed.entries:
                url = entry.get("link", "")
                if not url or url in existing_urls:
                    continue

                # Date filter — try published, then updated
                pub_dt = _struct_to_dt(
                    entry.get("published_parsed") or entry.get("updated_parsed")
                )
                if pub_dt and pub_dt < cutoff:
                    continue

                title   = entry.get("title", "")
                summary = _strip_html(entry.get("summary") or entry.get("description") or "")
                author  = entry.get("author", "")

                raw.append({
                    "_url":          url,
                    "_source_id":    source["id"],
                    "_title":        title,
                    "_excerpt":      summary[:1000],
                    "_author":       author,
                    "_published_at": _iso(pub_dt) if pub_dt else "",
                })
                source_count += 1

            log.info("RSS %-22s %d new items", source["name"] + ":", source_count)

        except Exception as e:
            msg = f"RSS {source['name']} failed: {e}"
            log.warning(msg)
            errors.append(msg)

    return raw, errors


def fetch_hn(existing_urls: set[str]) -> tuple[list[dict], list[str]]:
    """
    Fetch recent AI/ML stories from HackerNews via the Algolia search API.
    Runs multiple keyword queries and deduplicates by HN story ID.
    No API key required; rate limit is generous for daily pipelines.
    """
    raw, errors = [], []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    seen_ids: set[str] = set()

    for query in HN_AI_QUERIES:
        try:
            resp = requests.get(
                "https://hn.algolia.com/api/v1/search_by_date",
                params={
                    "tags":           "story",
                    "query":          query,
                    "numericFilters": (
                        f"points>={HN_MIN_POINTS},"
                        f"created_at_i>{int(cutoff.timestamp())}"
                    ),
                    "hitsPerPage":    50,
                },
                headers={"User-Agent": "frontier-tech-pipeline/1.0"},
                timeout=30,
            )
            resp.raise_for_status()
            hits = resp.json().get("hits", [])

            for hit in hits:
                story_id = hit.get("objectID", "")
                if not story_id or story_id in seen_ids:
                    continue
                seen_ids.add(story_id)

                # Prefer the linked URL; fall back to HN discussion page
                ext_url = hit.get("url", "")
                hn_url  = f"https://news.ycombinator.com/item?id={story_id}"
                url     = ext_url if ext_url else hn_url
                if url in existing_urls:
                    continue

                created_at = hit.get("created_at", "")
                pub_dt = _parse_iso(created_at) if created_at else None

                points      = hit.get("points", 0)
                num_comments = hit.get("num_comments", 0)
                story_text  = _strip_html(hit.get("story_text") or "")

                parts = []
                if story_text:
                    parts.append(story_text[:500])
                parts.append(f"HN Points: {points} | Comments: {num_comments}")
                if ext_url:
                    parts.append(f"HN Discussion: {hn_url}")

                raw.append({
                    "_url":          url,
                    "_source_id":    HN_SOURCE_ID,
                    "_title":        hit.get("title", ""),
                    "_excerpt":      " | ".join(parts)[:1000],
                    "_author":       hit.get("author", ""),
                    "_published_at": _iso(pub_dt) if pub_dt else "",
                })

            time.sleep(0.25)   # Algolia rate limit: generous but be polite

        except Exception as e:
            msg = f"HackerNews query '{query[:30]}' failed: {e}"
            log.warning(msg)
            errors.append(msg)

    log.info("HackerNews: %d new items (%d unique stories seen)", len(raw), len(seen_ids))
    return raw, errors


def fetch_github(existing_urls: set[str]) -> tuple[list[dict], list[str]]:
    """
    Fetch recently-active AI/ML repositories via the GitHub Search Topics API.
    Filters by star count and recent push date; deduplicates by repo ID.

    Set GITHUB_TOKEN in .env to increase rate limit from 60 → 5000 req/hr.
    """
    raw, errors = [], []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    seen_ids: set[int] = set()

    session = requests.Session()
    session.headers.update({
        "Accept":               "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent":           "frontier-tech-pipeline/1.0",
    })
    github_token = os.environ.get("GITHUB_TOKEN", "")
    if github_token:
        session.headers["Authorization"] = f"Bearer {github_token}"
        log.info("GitHub: authenticated (5000 req/hr)")
    else:
        log.info("GitHub: unauthenticated (60 req/hr) — set GITHUB_TOKEN in .env to increase")

    cutoff_date = cutoff.strftime("%Y-%m-%d")

    for topic in GITHUB_TOPICS:
        try:
            resp = session.get(
                "https://api.github.com/search/repositories",
                params={
                    "q":        f"topic:{topic} stars:>={GITHUB_MIN_STARS} pushed:>{cutoff_date}",
                    "sort":     "stars",
                    "order":    "desc",
                    "per_page": GITHUB_PER_TOPIC,
                },
                timeout=30,
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])

            for repo in items:
                repo_id = repo.get("id")
                if not repo_id or repo_id in seen_ids:
                    continue
                seen_ids.add(repo_id)

                url = repo.get("html_url", "")
                if not url or url in existing_urls:
                    continue

                pushed_at = repo.get("pushed_at", "")
                pub_dt = _parse_iso(pushed_at) if pushed_at else None
                if pub_dt and pub_dt < cutoff:
                    continue

                description = (repo.get("description") or "").strip()
                stars    = repo.get("stargazers_count", 0)
                forks    = repo.get("forks_count", 0)
                language = repo.get("language") or ""
                topics   = repo.get("topics", [])

                parts = []
                if description:
                    parts.append(description[:400])
                parts.append(f"Stars: {stars:,} | Forks: {forks:,}")
                if language:
                    parts.append(f"Language: {language}")
                if topics:
                    parts.append(f"Topics: {', '.join(topics[:8])}")

                raw.append({
                    "_url":          url,
                    "_source_id":    GITHUB_SOURCE_ID,
                    "_title":        repo.get("full_name", ""),
                    "_excerpt":      " | ".join(parts)[:1000],
                    "_author":       (repo.get("owner") or {}).get("login", ""),
                    "_published_at": _iso(pub_dt) if pub_dt else "",
                })

            log.info("GitHub %-28s %d new repos", f"topic:{topic}:", len(raw))
            time.sleep(1.0)   # GitHub search API: 30 req/min for auth, 10 for anon

        except Exception as e:
            msg = f"GitHub topic '{topic}' failed: {e}"
            log.warning(msg)
            errors.append(msg)

    log.info("GitHub:     %d new items total (%d repos seen)", len(raw), len(seen_ids))
    return raw, errors


def _load_leaderboard_snapshot() -> dict:
    try:
        with open(LEADERBOARD_SNAPSHOT, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_leaderboard_snapshot(snapshot: dict) -> None:
    os.makedirs(os.path.dirname(LEADERBOARD_SNAPSHOT), exist_ok=True)
    try:
        with open(LEADERBOARD_SNAPSHOT, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2)
    except Exception as e:
        log.warning("Could not save leaderboard snapshot: %s", e)


def fetch_leaderboards(existing_urls: set[str]) -> tuple[list[dict], list[str]]:
    """
    Snapshot-diff fetcher for two leaderboards:

    1. HF Open LLM Leaderboard v2 — downloads the public parquet file
       (~1.1 MB), sorts by composite average score, emits signals for
       models newly entering the top N.  Requires: pandas, pyarrow.

    2. Chatbot Arena (MT-bench) — fetches the most recent
       leaderboard_table_*.csv from the LMSYS HF Space, emits signals
       for models newly entering the top N by MT-bench score.
       Note: lmarena.ai API is not publicly accessible; this source uses
       the LMSYS HF Space snapshots (last updated ~Aug 2025 at time of
       writing — switch to lmarena.ai API if/when they open it).

    Only emits signals for models *new* since the last run.
    Snapshot stored at sources/leaderboard_snapshot.json.
    """
    try:
        import io
        import pandas as pd
    except ImportError:
        msg = "Leaderboard fetcher requires pandas + pyarrow — run: pip install pandas pyarrow"
        log.error(msg)
        return [], [msg]

    raw, errors = [], []
    snapshot     = _load_leaderboard_snapshot()
    new_snapshot = dict(snapshot)

    # ── 1. HF Open LLM Leaderboard v2 (parquet) ──────────────────────────────
    try:
        # Resolve current parquet URL via the datasets-server (avoids hardcoding SHA)
        meta = requests.get(
            "https://datasets-server.huggingface.co/parquet",
            params={"dataset": "open-llm-leaderboard/contents"},
            timeout=15,
        )
        meta.raise_for_status()
        parquet_url = meta.json()["parquet_files"][0]["url"]

        resp = requests.get(parquet_url, timeout=60)
        resp.raise_for_status()
        df = pd.read_parquet(io.BytesIO(resp.content))

        # Sort by composite average score, take top N
        score_col = "Average ⬆️"
        df = df.sort_values(score_col, ascending=False).head(LEADERBOARD_TOP_N * 2)

        known_hf = set(snapshot.get("hf_leaderboard", []))
        new_hf: list[str] = []
        rank = 0

        for _, row in df.iterrows():
            if rank >= LEADERBOARD_TOP_N:
                break
            model_id = str(row.get("fullname", "")).strip()
            if not model_id:
                continue
            rank += 1

            if model_id in known_hf:
                continue

            url = f"https://huggingface.co/{model_id}"
            if url in existing_urls:
                known_hf.add(model_id)
                continue

            avg_score = row.get(score_col, 0)
            sub_date  = str(row.get("Submission Date", "")).strip()
            params_b  = row.get("#Params (B)", "")
            arch      = str(row.get("Architecture", "")).strip()

            # Gather individual benchmark scores for the excerpt
            bench_scores = []
            for bench in ["IFEval", "BBH", "MATH Lvl 5", "GPQA", "MUSR", "MMLU-PRO"]:
                val = row.get(bench)
                if val is not None and not (isinstance(val, float) and val != val):
                    bench_scores.append(f"{bench}: {val:.1f}")

            excerpt_parts = [
                f"HF Open LLM Leaderboard rank #{rank} | Avg score: {avg_score:.2f}",
            ]
            if bench_scores:
                excerpt_parts.append(" | ".join(bench_scores[:4]))
            if arch:
                excerpt_parts.append(f"Architecture: {arch}")
            if params_b:
                excerpt_parts.append(f"Params: {params_b}B")

            pub_str = sub_date if sub_date and sub_date != "nan" else ""
            pub_dt  = _parse_iso(pub_str) if pub_str else None

            raw.append({
                "_url":          url,
                "_source_id":    HF_LEADERBOARD_SRC,
                "_title":        f"[HF Leaderboard #{rank}] {model_id}",
                "_excerpt":      " | ".join(excerpt_parts)[:1000],
                "_author":       model_id.split("/")[0] if "/" in model_id else "",
                "_published_at": _iso(pub_dt) if pub_dt else "",
            })
            new_hf.append(model_id)
            known_hf.add(model_id)

        new_snapshot["hf_leaderboard"] = list(known_hf)
        log.info("HF Leaderboard:  %d new models (top %d by avg score)", len(new_hf), LEADERBOARD_TOP_N)

    except Exception as e:
        msg = f"HF Leaderboard fetch failed: {e}"
        log.error(msg)
        errors.append(msg)

    # ── 2. Chatbot Arena — most recent leaderboard CSV from LMSYS HF Space ───
    # Note: lmarena.ai API is not publicly accessible. This uses LMSYS HF Space
    # snapshots (leaderboard_table_YYYYMMDD.csv).  If the space stops updating,
    # this source goes quiet until a replacement API is available.
    try:
        # Dynamically find the most recent leaderboard_table_*.csv in the space
        space_meta = requests.get(
            "https://huggingface.co/api/spaces/lmsys/chatbot-arena-leaderboard",
            timeout=15,
        )
        space_meta.raise_for_status()
        siblings = space_meta.json().get("siblings", [])

        csv_files = sorted(
            [s["rfilename"] for s in siblings
             if s["rfilename"].startswith("leaderboard_table_") and s["rfilename"].endswith(".csv")],
            reverse=True,
        )
        if not csv_files:
            raise ValueError("No leaderboard CSVs found in LMSYS HF Space")

        latest_csv = csv_files[0]
        csv_date   = latest_csv.replace("leaderboard_table_", "").replace(".csv", "")
        log.info("Chatbot Arena: using %s", latest_csv)

        csv_resp = requests.get(
            f"https://huggingface.co/spaces/lmsys/chatbot-arena-leaderboard/resolve/main/{latest_csv}",
            timeout=30,
        )
        csv_resp.raise_for_status()

        # Parse CSV: key,Model,MT-bench (score),MMLU,...,Organization,Link
        import csv as csv_mod
        reader     = csv_mod.DictReader(csv_resp.text.splitlines())
        arena_rows = list(reader)

        # Sort by MT-bench score descending
        def _mt(r):
            try:
                return float(r.get("MT-bench (score)", 0) or 0)
            except ValueError:
                return 0.0

        arena_rows.sort(key=_mt, reverse=True)

        known_arena = set(snapshot.get("chatbot_arena", []))
        new_arena: list[str] = []

        for rank, row in enumerate(arena_rows[:LEADERBOARD_TOP_N], start=1):
            model_name = row.get("Model", "").strip()
            if not model_name or model_name in known_arena:
                continue

            safe_key = model_name.replace(" ", "-").replace("/", "-").replace(":", "")
            url = f"arena://chatbot-arena/{safe_key}"
            if url in existing_urls:
                known_arena.add(model_name)
                continue

            mt_score = row.get("MT-bench (score)", "")
            mmlu     = row.get("MMLU", "")
            org      = row.get("Organization", "")
            license_ = row.get("License", "")
            link     = row.get("Link", "")

            excerpt = (
                f"Chatbot Arena rank #{rank} (MT-bench) | "
                f"MT-bench: {mt_score} | MMLU: {mmlu} | "
                f"Org: {org} | License: {license_} | Data: {csv_date}"
            )

            raw.append({
                "_url":          url,
                "_source_id":    ARENA_SOURCE_ID,
                "_title":        f"[Chatbot Arena #{rank}] {model_name}",
                "_excerpt":      excerpt[:1000],
                "_author":       org,
                "_published_at": "",
            })
            new_arena.append(model_name)
            known_arena.add(model_name)

        new_snapshot["chatbot_arena"] = list(known_arena)
        log.info("Chatbot Arena:   %d new models (top %d by MT-bench)", len(new_arena), LEADERBOARD_TOP_N)

    except Exception as e:
        msg = f"Chatbot Arena fetch failed: {e}"
        log.warning(msg)
        errors.append(msg)

    _save_leaderboard_snapshot(new_snapshot)
    log.info("Leaderboards: %d new items total", len(raw))
    return raw, errors


# ──────────────────────────────────────────────────────────────────────────────
# ENRICHMENT
# ──────────────────────────────────────────────────────────────────────────────

def _build_signal_record(item: dict, cls: dict) -> dict:
    """
    Merge a raw item + LLM classification into an Airtable-ready field dict.
    All field IDs are used (not names) for safety.
    """
    now_str = _iso(datetime.now(timezone.utc))

    # Validate quadrant
    quadrant = cls.get("quadrant", "")
    if quadrant not in VALID_QUADRANTS:
        quadrant = "Tools"

    # Validate verticals
    verticals = [v for v in (cls.get("verticals") or []) if v in VALID_VERTICALS]
    if not verticals:
        verticals = ["AI Systems"]

    # Validate status
    status = cls.get("pipeline_status", "Surface")
    if status not in VALID_NEW_STATUSES:
        status = "Surface"

    record: dict = {
        SF["title"]:                item["_title"],
        SF["url"]:                  item["_url"],
        SF["captured_at"]:          now_str,
        SF["pipeline_status"]:      status,
        SF["quadrant"]:             quadrant,
        SF["verticals"]:            verticals,
        SF["confidence_score"]:     _clamp(cls.get("confidence_score"), 0.0, 1.0, 0.5),
        SF["fidelity_score"]:       _clamp(cls.get("fidelity_score"),   0.0, 10.0, 5.0),
        SF["availability_score"]:   _clamp(cls.get("availability_score"), 0.0, 10.0, 5.0),
        SF["corroboration_count"]:  0,
        SF["source"]:               [item["_source_id"]],
        SF["submission_source"]:    "Pipeline",
    }

    # Optional fields — only set when non-empty
    if item.get("_published_at"):
        record[SF["published_at"]] = item["_published_at"]
    if item.get("_author"):
        record[SF["author"]] = item["_author"]
    if item.get("_excerpt"):
        record[SF["raw_excerpt"]] = item["_excerpt"]
    if cls.get("summary"):
        record[SF["summary"]] = cls["summary"]
    if cls.get("enrichment_rationale"):
        record[SF["enrichment_rationale"]] = cls["enrichment_rationale"]
    if cls.get("pipeline_suggestion"):
        record[SF["pipeline_suggestion"]] = cls["pipeline_suggestion"]

    return record


def enrich_batch(items: list[dict], client: Anthropic) -> list[dict]:
    """
    Send a batch of raw items to Claude Haiku for classification.
    Returns Airtable-ready field dicts, one per input item.
    Falls back to minimal defaults if the LLM call fails.
    """
    signals_input = [
        {"index": i, "title": item["_title"], "excerpt": item["_excerpt"]}
        for i, item in enumerate(items)
    ]

    prompt = ENRICH_PROMPT.format(
        quadrants=json.dumps(VALID_QUADRANTS),
        verticals=json.dumps(VALID_VERTICALS),
        signals_json=json.dumps(signals_input, ensure_ascii=False, indent=2),
    )

    try:
        response = client.messages.create(
            model=ENRICH_MODEL,
            max_tokens=4096,
            system=ENRICH_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()

        # Strip markdown fences if Haiku wraps output in ```json ... ```
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*\n?", "", raw)
            raw = re.sub(r"\n?\s*```\s*$", "", raw)

        classifications: list[dict] = json.loads(raw)

    except Exception as e:
        log.error("Enrichment LLM call failed: %s — using defaults for this batch", e)
        classifications = [{} for _ in items]

    return [_build_signal_record(item, classifications[i] if i < len(classifications) else {})
            for i, item in enumerate(items)]


def fetch_arxiv(existing_urls: set[str]) -> tuple[list[dict], list[str]]:
    """
    Fetch recent preprints from the ArXiv API across ARXIV_CATEGORIES.
    Uses the Atom feed endpoint which includes full abstracts.
    """
    raw, errors = [], []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    cat_query = " OR ".join(f"cat:{c}" for c in ARXIV_CATEGORIES)

    try:
        resp = requests.get(
            "https://export.arxiv.org/api/query",
            params={
                "search_query": cat_query,
                "start":        0,
                "max_results":  ARXIV_MAX_RESULTS,
                "sortBy":       "submittedDate",
                "sortOrder":    "descending",
            },
            timeout=30,
        )
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception as e:
        msg = f"ArXiv fetch failed: {e}"
        log.error(msg)
        return [], [msg]

    for entry in feed.entries:
        # Canonical URL: prefer https://arxiv.org/abs/{id}
        link = entry.get("link", "")
        arxiv_id = link.rstrip("/").split("/")[-1]
        url = f"https://arxiv.org/abs/{arxiv_id}" if arxiv_id else link
        if not url or url in existing_urls:
            continue

        pub_dt = _struct_to_dt(entry.get("published_parsed"))
        if pub_dt and pub_dt < cutoff:
            continue

        authors = [a.get("name", "") for a in entry.get("authors", [])[:3]]
        # feedparser puts abstract in 'summary'
        abstract = _strip_html(entry.get("summary", ""))[:1000]
        title    = _strip_html(entry.get("title", ""))

        raw.append({
            "_url":          url,
            "_source_id":    ARXIV_SOURCE_ID,
            "_title":        title,
            "_excerpt":      abstract,
            "_author":       ", ".join(filter(None, authors)),
            "_published_at": _iso(pub_dt) if pub_dt else "",
        })

    log.info("ArXiv:      %d new items", len(raw))
    return raw, errors


# ── Harmonic Scout prompts ────────────────────────────────────────────────────

HARMONIC_EXTRACT_SYSTEM = """You extract discrete technology signals from market intelligence reports.
A signal is a specific technology, tool, technique, platform, or trend that an engineering team
should be aware of. Return only valid JSON."""

HARMONIC_EXTRACT_PROMPT = """\
Extract individual technology signals from this Harmonic Scout market intelligence report.
Each signal should be something concrete and trackable — a tool, platform, technique, model family,
or clearly emerging trend backed by market data in the report.

Return a JSON array of up to 15 signals, each with:
  title    short name (≤ 10 words)
  excerpt  2–3 sentences of key facts from the report about this signal

Omit vague macro-trends. Focus on things an engineering team could evaluate or act on.

Report title: {title}

Report content:
{content}"""


def fetch_harmonic_reports(existing_urls: set[str], client: Anthropic) -> tuple[list[dict], list[str]]:
    """
    Read .md files from sources/harmonic/, extract individual signals via Haiku,
    and return them as raw items for the normal enrichment pipeline.

    Each report file is processed once — subsequent runs skip it via URL dedup
    using a stable synthetic URL: harmonic://report/<filename>/<signal_index>
    """
    raw, errors = [], []

    if not os.path.isdir(HARMONIC_DIR):
        log.warning("Harmonic directory not found: %s", HARMONIC_DIR)
        return raw, errors

    md_files = sorted(f for f in os.listdir(HARMONIC_DIR) if f.endswith(".md") and f != "README.md")
    if not md_files:
        log.info("Harmonic:   no .md reports found")
        return raw, errors

    for filename in md_files:
        filepath = os.path.join(HARMONIC_DIR, filename)
        with open(filepath, encoding="utf-8") as f:
            content = f.read()

        report_title = filename.replace(".md", "").replace("-", " ").replace("_", " ").title()
        log.info("Harmonic:   extracting signals from '%s'…", filename)

        # Extract signals via Haiku
        try:
            prompt = HARMONIC_EXTRACT_PROMPT.format(
                title   = report_title,
                content = content[:6000],   # cap to avoid token overflow
            )
            response = client.messages.create(
                model      = ENRICH_MODEL,
                max_tokens = 2048,
                system     = HARMONIC_EXTRACT_SYSTEM,
                messages   = [{"role": "user", "content": prompt}],
            )
            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = re.sub(r"^```(?:json)?\s*\n?", "", text)
                text = re.sub(r"\n?\s*```\s*$", "", text)
            signals: list[dict] = json.loads(text)
        except Exception as e:
            msg = f"Harmonic signal extraction failed for {filename}: {e}"
            log.error(msg)
            errors.append(msg)
            continue

        new_count = 0
        for i, sig in enumerate(signals):
            url = f"harmonic://report/{filename}/{i}"
            if url in existing_urls:
                continue
            title_text   = sig.get("title",   "")
            excerpt_text = sig.get("excerpt", "")
            if not title_text:
                continue
            raw.append({
                "_url":          url,
                "_source_id":    HARMONIC_SOURCE_ID,
                "_title":        f"[Harmonic] {title_text}",
                "_excerpt":      excerpt_text[:1000],
                "_author":       "Harmonic.ai Scout",
                "_published_at": "",
            })
            new_count += 1

        log.info("Harmonic:   %d new signals extracted from '%s'", new_count, filename)

    return raw, errors


def load_harmonic_context() -> str:
    """
    Load all Harmonic Scout .md reports into a single context string
    for injection into the Sonnet insight prompt.

    Caps each report at 3000 chars and total context at 8000 chars
    to stay within reasonable token budgets.
    """
    if not os.path.isdir(HARMONIC_DIR):
        return ""

    parts = []
    total = 0
    for filename in sorted(os.listdir(HARMONIC_DIR)):
        if not filename.endswith(".md") or filename == "README.md":
            continue
        filepath = os.path.join(HARMONIC_DIR, filename)
        with open(filepath, encoding="utf-8") as f:
            text = f.read(3000)
        report_name = filename.replace(".md", "").replace("-", " ").replace("_", " ").title()
        chunk = f"### {report_name}\n\n{text}"
        parts.append(chunk)
        total += len(chunk)
        if total >= 8000:
            break

    if not parts:
        return ""

    return "## Harmonic.ai Market Intelligence\n\n" + "\n\n---\n\n".join(parts)


def generate_insights(records: list[dict], client: Anthropic, context: str = "") -> list[dict]:
    """
    Second-pass enrichment using Sonnet.

    For signals classified as Surface or Research with confidence >= INSIGHT_MIN_CONFIDENCE,
    replaces the brief Haiku summary with a substantive 4–6 sentence insight covering
    what it is, why it matters, and what practitioners should do with it.

    Signals below the threshold keep their Haiku summary unchanged.
    """
    candidates = [
        (i, r) for i, r in enumerate(records)
        if r.get(SF["pipeline_status"]) in INSIGHT_STATUSES
        and r.get(SF["confidence_score"], 0) >= INSIGHT_MIN_CONFIDENCE
    ]

    if not candidates:
        log.info("Insight pass: no high-relevance signals — skipping Sonnet call")
        return records

    log.info(
        "Insight pass: %d/%d signals qualify for Sonnet deep summary",
        len(candidates), len(records),
    )

    updated = list(records)   # shallow copy of list; dicts updated individually

    n_batches = -(-len(candidates) // INSIGHT_BATCH_SIZE)
    for i in range(0, len(candidates), INSIGHT_BATCH_SIZE):
        batch = candidates[i : i + INSIGHT_BATCH_SIZE]
        log.info(
            "  Insight batch %d/%d  (%d signals)…",
            i // INSIGHT_BATCH_SIZE + 1, n_batches, len(batch),
        )

        signals_input = [
            {
                "index": j,
                "title":         r.get(SF["title"], ""),
                "excerpt":       r.get(SF["raw_excerpt"], "")[:800],
                "haiku_summary": r.get(SF["summary"], ""),
                "quadrant":      r.get(SF["quadrant"], ""),
                "verticals":     r.get(SF["verticals"], []),
            }
            for j, (_, r) in enumerate(batch)
        ]

        prompt = INSIGHT_PROMPT.format(
            signals_json=json.dumps(signals_input, ensure_ascii=False, indent=2)
        )

        system = INSIGHT_SYSTEM
        if context:
            system += (
                "\n\nAdditional market intelligence from Harmonic.ai Scout reports "
                "— use this to ground your 'why it matters' analysis in real adoption "
                "and investment signals where relevant:\n\n" + context[:6000]
            )

        try:
            response = client.messages.create(
                model=INSIGHT_MODEL,
                max_tokens=4096,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = response.content[0].text.strip()
            if raw.startswith("```"):
                raw = re.sub(r"^```(?:json)?\s*\n?", "", raw)
                raw = re.sub(r"\n?\s*```\s*$", "", raw)
            insights: list[dict] = json.loads(raw)

            for k, (orig_idx, _) in enumerate(batch):
                if k < len(insights):
                    ins = insights[k]
                    insight_text = ins.get("insight", "").strip()
                    if insight_text:
                        updated[orig_idx] = dict(updated[orig_idx])   # copy before mutating
                        updated[orig_idx][SF["summary"]] = insight_text
                    tl_dr = ins.get("tl_dr")
                    if tl_dr in ("Tech Lead", "Don't Read"):
                        updated[orig_idx][SF["tl_dr"]] = tl_dr
                    why = ins.get("why_read_skip", "").strip()
                    if why:
                        updated[orig_idx][SF["why_read_skip"]] = why

        except Exception as e:
            log.error(
                "Sonnet insight batch %d failed: %s — keeping Haiku summaries for this batch",
                i // INSIGHT_BATCH_SIZE + 1, e,
            )

        if i + INSIGHT_BATCH_SIZE < len(candidates):
            time.sleep(1)

    return updated


# ──────────────────────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ──────────────────────────────────────────────────────────────────────────────

def run(sources: str = "all", dry_run: bool = False) -> None:
    divider = "━" * 62
    log.info(divider)
    log.info("Frontier Tech Ingestion  %s%s",
             datetime.now().strftime("%Y-%m-%d %H:%M"),
             "  [DRY RUN]" if dry_run else "")
    log.info(divider)

    client = Anthropic(api_key=ANTHROPIC_KEY)
    all_errors: list[str] = []
    all_raw:    list[dict] = []

    # ── 1. Load existing URLs for dedup ──────────────────────────────────────
    existing_urls = get_existing_urls()

    # ── 2. Fetch from requested sources ──────────────────────────────────────
    sources_evaluated = 0

    if sources in ("all", "papers"):
        raw, errs = fetch_hf_papers(existing_urls)
        all_raw.extend(raw)
        all_errors.extend(errs)
        sources_evaluated += 1

    if sources in ("all", "models"):
        raw, errs = fetch_hf_models(existing_urls)
        all_raw.extend(raw)
        all_errors.extend(errs)
        sources_evaluated += 1

    if sources in ("all", "rss"):
        rss_sources = load_rss_sources()
        raw, errs = fetch_rss_feeds(existing_urls, rss_sources)
        all_raw.extend(raw)
        all_errors.extend(errs)
        sources_evaluated += len(rss_sources)

    if sources in ("all", "arxiv"):
        raw, errs = fetch_arxiv(existing_urls)
        all_raw.extend(raw)
        all_errors.extend(errs)
        sources_evaluated += 1

    if sources in ("all", "reddit"):
        reddit_sources = load_reddit_sources()
        raw, errs = fetch_reddit(existing_urls, reddit_sources)
        all_raw.extend(raw)
        all_errors.extend(errs)
        sources_evaluated += len(reddit_sources)

    if sources in ("all", "hn"):
        raw, errs = fetch_hn(existing_urls)
        all_raw.extend(raw)
        all_errors.extend(errs)
        sources_evaluated += 1

    if sources in ("all", "github"):
        raw, errs = fetch_github(existing_urls)
        all_raw.extend(raw)
        all_errors.extend(errs)
        sources_evaluated += len(GITHUB_TOPICS)

    if sources in ("all", "leaderboards"):
        raw, errs = fetch_leaderboards(existing_urls)
        all_raw.extend(raw)
        all_errors.extend(errs)
        sources_evaluated += 2   # HF Leaderboard + Chatbot Arena

    if sources in ("all", "harmonic"):
        raw, errs = fetch_harmonic_reports(existing_urls, client)
        all_raw.extend(raw)
        all_errors.extend(errs)
        sources_evaluated += 1

    log.info(divider)
    log.info("New items to enrich: %d  (across %d sources evaluated)",
             len(all_raw), sources_evaluated)

    if not all_raw:
        log.info("Nothing new — run complete.")
        write_eval_run("discovery", sources_evaluated, 0, all_errors, dry_run)
        return

    # ── 3. Enrich in batches ──────────────────────────────────────────────────
    all_enriched: list[dict] = []
    n_batches = -(-len(all_raw) // BATCH_SIZE)  # ceiling division

    for i in range(0, len(all_raw), BATCH_SIZE):
        batch = all_raw[i : i + BATCH_SIZE]
        log.info("Enriching batch %d/%d  (%d items)…",
                 i // BATCH_SIZE + 1, n_batches, len(batch))
        all_enriched.extend(enrich_batch(batch, client))
        if i + BATCH_SIZE < len(all_raw):
            time.sleep(1)   # gentle pacing between LLM calls

    # ── 4. Sonnet insight pass (high-relevance signals only) ──────────────────
    harmonic_context = load_harmonic_context()
    if harmonic_context:
        log.info("Harmonic context loaded (%d chars) — injecting into Sonnet pass", len(harmonic_context))
    all_enriched = generate_insights(all_enriched, client, context=harmonic_context)

    # ── 5. Write to Airtable ──────────────────────────────────────────────────
    written = write_signals(all_enriched, dry_run=dry_run)
    if not dry_run:
        log.info("✓ Wrote %d signals to Airtable", written)

    # ── 6. Log evaluation run ─────────────────────────────────────────────────
    try:
        write_eval_run("full", sources_evaluated, written, all_errors, dry_run)
        if not dry_run:
            log.info("✓ Evaluation run logged")
    except Exception as e:
        log.warning("Could not write eval run: %s", e)

    log.info(divider)
    log.info("Done.  %d new signals ingested.", written)


# ──────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Frontier Tech ingestion pipeline")
    parser.add_argument(
        "--source",
        choices=["all", "papers", "models", "rss", "arxiv", "harmonic", "reddit",
                 "hn", "github", "leaderboards"],
        default="all",
        help="Which source group to run (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and enrich, but skip all Airtable writes",
    )
    args = parser.parse_args()
    run(sources=args.source, dry_run=args.dry_run)
