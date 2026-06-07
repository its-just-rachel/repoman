#!/usr/bin/env python3
"""
Frontier Tech — Signal Submission App
======================================
A local web app for submitting URLs for immediate TL/DR analysis.

Usage:
  cd webapp/
  pip install -r requirements.txt
  # copy ../.env or create a .env here with AIRTABLE_TOKEN + ANTHROPIC_API_KEY
  python app.py
  # then open http://localhost:5000
"""

import json
import logging
import os
import re
import time
from datetime import datetime, timezone

import requests
from anthropic import Anthropic
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

# Load .env from this directory, or fall back to ../ingest/.env
load_dotenv()
if not os.environ.get("AIRTABLE_TOKEN"):
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", "ingest", ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger("frontier_submit")

app = Flask(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN", "")
ANTHROPIC_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
BASE_ID        = "appwe0lxRHbASBgG2"
T_SIGNALS      = "tblvgGIVerqksC5FP"

# Field IDs — keep in sync with ingest/main.py
SF = dict(
    title                = "fldQ9K0UMFs6jWpGe",
    url                  = "fldXOHzAs204eXeSs",
    author               = "fldifKZdKcXjXjV2S",
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
    source               = "fldvqlOKqvlIOM7FP",
    tl_dr                = "fldRDTrdresooZpZC",
    why_read_skip        = "fldgcrntMCFY4uXsF",
    submission_source    = "fldr1cfzD7V3bpjFg",
)

VALID_QUADRANTS = ["Tools", "Techniques", "Platforms", "Frameworks & Languages"]
VALID_VERTICALS = [
    "AI Systems", "NLP / Language", "Computer Vision", "Audio", "Multimodal",
    "Reinforcement Learning", "Infrastructure", "Security & Trust", "Edge / On-Device",
]

ANALYZE_SYSTEM = """\
You are a senior technology analyst evaluating AI and emerging technology content for \
C-suite and technical leadership.

Analyze the submitted article and return a structured classification.

Tech Lead ("TL"): Leadership SHOULD know about this. Novel capability, market-moving \
development, competitive threat, or paradigm shift — the kind of thing that would embarrass \
them not to have known about a quarter from now.

Don't Read ("DR"): Will circulate widely but safe to deprioritize — known hype cycle, \
incremental update to something already tracked, too niche, or too early-stage to act on.

Return ONLY valid JSON, no other text:
{
  "title": "short article title (≤12 words)",
  "description": "one sentence: what this is about",
  "tl_dr": "Tech Lead" or "Don't Read",
  "rationale": "2-3 sentences: Why Read or Why Skip — exec-readable, no jargon",
  "summary": "4-6 sentences for practitioners: what it is, why it matters, what to do with it",
  "quadrant": one of ["Tools", "Techniques", "Platforms", "Frameworks & Languages"],
  "verticals": array of 1-3 from ["AI Systems", "NLP / Language", "Computer Vision",
    "Audio", "Multimodal", "Reinforcement Learning", "Infrastructure",
    "Security & Trust", "Edge / On-Device"],
  "pipeline_status": "Surface" or "Research" or "Hold" or "Dismissed",
  "confidence": float 0.0-1.0,
  "fidelity_score": float 0-10,
  "availability_score": float 0-10
}"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _clamp(v, lo, hi, default):
    try:
        return max(lo, min(hi, float(v)))
    except (TypeError, ValueError):
        return default


def fetch_url_content(url: str) -> str:
    """Fetch a URL and return cleaned text content, capped at 5000 chars."""
    try:
        resp = requests.get(
            url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (compatible; FrontierTech/1.0)"},
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        text = re.sub(r"\s+", " ", text)
        return text[:5000]
    except Exception as e:
        log.warning("Could not fetch URL content: %s", e)
        return ""


def url_already_exists(url: str) -> bool:
    """Check if this URL is already in Airtable."""
    try:
        params = {"fields[]": ["URL"], "filterByFormula": f'URL = "{url}"', "maxRecords": 1}
        resp = requests.get(
            f"https://api.airtable.com/v0/{BASE_ID}/{T_SIGNALS}",
            headers={"Authorization": f"Bearer {AIRTABLE_TOKEN}"},
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        return len(resp.json().get("records", [])) > 0
    except Exception:
        return False


def count_today_tl_dr(tl_dr_value: str) -> int:
    """Count how many signals with this TL/DR value are in Airtable from today."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    formula = f'AND({{TL/DR}} = "{tl_dr_value}", IS_SAME({{Captured At}}, "{today}", "day"))'
    try:
        resp = requests.get(
            f"https://api.airtable.com/v0/{BASE_ID}/{T_SIGNALS}",
            headers={"Authorization": f"Bearer {AIRTABLE_TOKEN}"},
            params={"filterByFormula": formula, "fields[]": ["URL"]},
            timeout=15,
        )
        resp.raise_for_status()
        return len(resp.json().get("records", []))
    except Exception:
        return 0


def write_to_airtable(url: str, note: str, analysis: dict) -> str | None:
    """Write the analyzed signal to Airtable. Returns record ID or None on failure."""
    quadrant = analysis.get("quadrant", "Tools")
    if quadrant not in VALID_QUADRANTS:
        quadrant = "Tools"

    verticals = [v for v in (analysis.get("verticals") or []) if v in VALID_VERTICALS]
    if not verticals:
        verticals = ["AI Systems"]

    status = analysis.get("pipeline_status", "Surface")
    if status not in ("Surface", "Research", "Hold", "Dismissed"):
        status = "Surface"

    tl_dr = analysis.get("tl_dr")
    if tl_dr not in ("Tech Lead", "Don't Read"):
        tl_dr = None

    fields = {
        SF["title"]:             analysis.get("title", url[:80]),
        SF["url"]:               url,
        SF["captured_at"]:       _iso_now(),
        SF["pipeline_status"]:   status,
        SF["quadrant"]:          quadrant,
        SF["verticals"]:         verticals,
        SF["confidence_score"]:  _clamp(analysis.get("confidence"), 0.0, 1.0, 0.5),
        SF["fidelity_score"]:    _clamp(analysis.get("fidelity_score"), 0.0, 10.0, 5.0),
        SF["availability_score"]:_clamp(analysis.get("availability_score"), 0.0, 10.0, 5.0),
        SF["corroboration_count"]: 0,
        SF["submission_source"]: "Submitted",
        SF["summary"]:           analysis.get("summary", ""),
        SF["enrichment_rationale"]: analysis.get("description", ""),
    }

    if tl_dr:
        fields[SF["tl_dr"]] = tl_dr
    if analysis.get("rationale"):
        fields[SF["why_read_skip"]] = analysis["rationale"]
    if note:
        # Prepend the submitter's note to raw_excerpt
        fields[SF["raw_excerpt"]] = f"[Submitted with note: {note}]"

    try:
        resp = requests.post(
            f"https://api.airtable.com/v0/{BASE_ID}/{T_SIGNALS}",
            headers={
                "Authorization": f"Bearer {AIRTABLE_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"records": [{"fields": fields}]},
            timeout=30,
        )
        resp.raise_for_status()
        record_id = resp.json()["records"][0]["id"]
        log.info("Wrote signal to Airtable: %s (%s)", record_id, tl_dr or "unclassified")
        return record_id
    except Exception as e:
        log.error("Airtable write failed: %s", e)
        return None


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/analyze", methods=["POST"])
def analyze():
    data = request.get_json(force=True)
    url  = (data.get("url") or "").strip()
    note = (data.get("note") or "").strip()

    if not url:
        return jsonify({"error": "No URL provided"}), 400
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    log.info("Analyzing submitted URL: %s", url)

    # Check for duplicate
    if url_already_exists(url):
        return jsonify({"error": "This URL has already been processed and is in the signal database."}), 409

    # Fetch page content
    content = fetch_url_content(url)

    # Build prompt
    user_parts = [f"URL: {url}"]
    if note:
        user_parts.append(f"Submitted with note: \"{note}\"")
    if content:
        user_parts.append(f"\nArticle content:\n{content}")
    else:
        user_parts.append("\n(Could not fetch article content — analyze based on URL and context.)")

    # Call Sonnet
    try:
        client = Anthropic(api_key=ANTHROPIC_KEY)
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=ANALYZE_SYSTEM,
            messages=[{"role": "user", "content": "\n\n".join(user_parts)}],
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*\n?", "", raw)
            raw = re.sub(r"\n?\s*```\s*$", "", raw)
        analysis = json.loads(raw)
    except Exception as e:
        log.error("Analysis failed: %s", e)
        return jsonify({"error": f"Analysis failed: {e}"}), 500

    # Write to Airtable
    record_id = write_to_airtable(url, note, analysis)

    # Count today's signals for context
    tl_dr = analysis.get("tl_dr")
    today_count = count_today_tl_dr(tl_dr) if tl_dr else 0

    return jsonify({
        "title":        analysis.get("title", ""),
        "description":  analysis.get("description", ""),
        "tl_dr":        tl_dr,
        "rationale":    analysis.get("rationale", ""),
        "summary":      analysis.get("summary", ""),
        "quadrant":     analysis.get("quadrant", ""),
        "verticals":    analysis.get("verticals", []),
        "confidence":   analysis.get("confidence", 0),
        "saved":        record_id is not None,
        "record_id":    record_id,
        "today_count":  today_count,
    })


if __name__ == "__main__":
    if not AIRTABLE_TOKEN:
        print("WARNING: AIRTABLE_TOKEN not set — Airtable writes will fail")
    if not ANTHROPIC_KEY:
        print("ERROR: ANTHROPIC_API_KEY not set")
    app.run(debug=True, host="0.0.0.0", port=5000)
