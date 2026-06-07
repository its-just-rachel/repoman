#!/usr/bin/env python3
"""
Frontier Tech — Ingestion Pipeline v1
======================================
Fetches signals from:
  - HuggingFace Daily Papers API     (academic, daily)
  - HuggingFace Hub Models API       (api, real-time polling)
  - 7 RSS feeds                      (lab blogs + Apple)

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

RSS_SOURCES = [
    {"id": "recAsTlPTetqFWV41", "name": "DeepMind Blog",
     "url": "https://deepmind.google/blog/rss.xml"},
    {"id": "rece3xIcfJI5vkydq", "name": "MIT AI News",
     "url": "https://news.mit.edu/rss/topic/artificial-intelligence2"},
    {"id": "recFTSkcynFlNmvDb", "name": "Google AI Blog",
     "url": "https://blog.google/technology/ai/rss/"},
    {"id": "recn9vYYixi6o5C6J", "name": "BAIR Blog",
     "url": "https://bair.berkeley.edu/blog/feed.xml"},
    {"id": "recV1GIbxU8XyFtEK", "name": "OpenAI News",
     "url": "https://openai.com/news/rss.xml"},
    {"id": "recPmCZCXEUdaAuaC", "name": "Apple Developer News",
     "url": "https://developer.apple.com/news/releases/rss/releases.rss"},
    {"id": "recWvEjOCh1cxDAe6", "name": "Apple Newsroom",
     "url": "https://www.apple.com/newsroom/rss-feed.rss"},
]


# ──────────────────────────────────────────────────────────────────────────────
# PIPELINE SETTINGS
# ──────────────────────────────────────────────────────────────────────────────

# Only consider items published within this window
LOOKBACK_DAYS = 2

# ArXiv categories to monitor
ARXIV_CATEGORIES  = ["cs.AI", "cs.LG", "cs.CL", "cs.CV", "cs.RO"]
ARXIV_MAX_RESULTS = 30    # per run across all categories

# Harmonic Scout reports directory (relative to this file)
HARMONIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "..", "sources", "harmonic")

# HF Models noise filters
HF_MODELS_LIMIT     = 30   # max models to inspect per run
HF_MODELS_MIN_LIKES = 10   # skip models with fewer likes than this

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

Return a JSON array — one object per signal, same order as input, no other text.
Each object: {{"insight": "..."}}

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


def _parse_iso(s: str) -> datetime | None:
    """Parse an ISO 8601 string; return None on failure."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _struct_to_dt(st) -> datetime | None:
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


def fetch_rss_feeds(existing_urls: set[str]) -> tuple[list[dict], list[str]]:
    raw, errors = [], []
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    for source in RSS_SOURCES:
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
                    insight_text = insights[k].get("insight", "").strip()
                    if insight_text:
                        updated[orig_idx] = dict(updated[orig_idx])   # copy before mutating
                        updated[orig_idx][SF["summary"]] = insight_text

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
        raw, errs = fetch_rss_feeds(existing_urls)
        all_raw.extend(raw)
        all_errors.extend(errs)
        sources_evaluated += len(RSS_SOURCES)

    if sources in ("all", "arxiv"):
        raw, errs = fetch_arxiv(existing_urls)
        all_raw.extend(raw)
        all_errors.extend(errs)
        sources_evaluated += 1

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
        choices=["all", "papers", "models", "rss", "arxiv", "harmonic"],
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
