#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Frontier Tech — Ingestion pipeline local setup
# Run once from the ingest/ directory:  bash setup.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "── Frontier Tech ingest setup ──────────────────────────────────────────"

# ── 1. Python version check ──────────────────────────────────────────────────
REQUIRED_MINOR=10
PYTHON=$(command -v python3 || command -v python || true)

if [[ -z "$PYTHON" ]]; then
  echo "✗ Python 3 not found. Install from https://python.org and re-run."
  exit 1
fi

PYTHON_VERSION=$("$PYTHON" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYTHON_MINOR=$("$PYTHON" -c "import sys; print(sys.version_info.minor)")

if [[ $("$PYTHON" -c "import sys; print(sys.version_info.major)") -lt 3 ]] || \
   [[ "$PYTHON_MINOR" -lt "$REQUIRED_MINOR" ]]; then
  echo "✗ Python 3.${REQUIRED_MINOR}+ required (found ${PYTHON_VERSION}). Please upgrade."
  exit 1
fi

echo "✓ Python ${PYTHON_VERSION}"

# ── 2. Virtual environment ────────────────────────────────────────────────────
if [[ ! -d ".venv" ]]; then
  echo "  Creating virtual environment..."
  "$PYTHON" -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate
echo "✓ Virtual environment active"

# ── 3. Dependencies ───────────────────────────────────────────────────────────
echo "  Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
echo "✓ Dependencies installed"

# ── 4. .env file ──────────────────────────────────────────────────────────────
if [[ ! -f ".env" ]]; then
  cp .env.example .env
  echo ""
  echo "⚠  Created .env from .env.example."
  echo "   Open ingest/.env and fill in:"
  echo "     AIRTABLE_TOKEN  — your Airtable personal access token"
  echo "     ANTHROPIC_API_KEY  — your Anthropic API key"
  echo ""
else
  echo "✓ .env already exists"
fi

# ── 5. Done ───────────────────────────────────────────────────────────────────
echo "────────────────────────────────────────────────────────────────────────"
echo "Setup complete. To run the pipeline:"
echo ""
echo "  source .venv/bin/activate          # if not already active"
echo "  python main.py --dry-run           # sanity check (no Airtable writes)"
echo "  python main.py                     # full run (all sources)"
echo "  python main.py --source papers     # single source: papers | models | rss"
echo ""
