#!/usr/bin/env python3
"""
Frontier Tech — KumoRFM POC
============================
Uses the kumo-rfm-mcp server via MCP stdio protocol to:
  1. Load the Signals CSV
  2. Sanitize column names for PQL compatibility
  3. Build a graph and materialize it
  4. Predict TL/DR for all unclassified signals
  5. Write high-confidence predictions (≥ CONFIDENCE_THRESHOLD) back to Airtable
     along with Kumo Score for every prediction

Usage:
  source ingest/.env
  ~/kumo-venv/bin/python kumo_poc.py --csv ~/Downloads/Signals-Grid\\ View.csv

Environment vars required:
  KUMO_API_KEY      — Kumo.ai API key
  AIRTABLE_TOKEN    — Airtable personal access token
"""

import argparse
import asyncio
import json
import os
import re
import sys
import tempfile

try:
    import pandas as pd
    import requests
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
except ImportError as e:
    print(f"ERROR: {e}")
    print("Make sure you're running with: ~/kumo-venv/bin/python")
    sys.exit(1)

# ── Config ─────────────────────────────────────────────────────────────────────
KUMO_API_KEY      = os.environ.get("KUMO_API_KEY", "")
AIRTABLE_TOKEN    = os.environ.get("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_ID  = "appwe0lxRHbASBgG2"
AIRTABLE_TABLE_ID = "tblvgGIVerqksC5FP"

# Field IDs in Airtable Signals table
F_URL        = "fldXOHzAs204eXeSs"
F_TLDR       = "fldRDTrdresooZpZC"   # singleSelect: Tech Lead / Don't Read
F_KUMO_SCORE = "fldMoETm17PK5QuTM"   # number: KumoRFM confidence score

CONFIDENCE_THRESHOLD = 0.65  # only auto-write TL/DR at or above this score

# ── Args ───────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--csv", required=True, help="Path to Signals CSV export from Airtable")
parser.add_argument("--dry-run", action="store_true", help="Skip Airtable writes; print what would change")
args = parser.parse_args()

csv_path = os.path.abspath(os.path.expanduser(args.csv))
csv_dir  = os.path.dirname(csv_path)

if not os.path.exists(csv_path):
    print(f"ERROR: CSV not found at {csv_path}")
    sys.exit(1)
if not KUMO_API_KEY:
    print("ERROR: KUMO_API_KEY not set. Run: source ingest/.env")
    sys.exit(1)
if not AIRTABLE_TOKEN and not args.dry_run:
    print("ERROR: AIRTABLE_TOKEN not set. Run: source ingest/.env")
    sys.exit(1)


# ── Helpers ────────────────────────────────────────────────────────────────────
def tool_result_text(result) -> str:
    """Extract text payload from an MCP tool result."""
    if hasattr(result, "content"):
        return "\n".join(c.text for c in result.content if hasattr(c, "text"))
    return str(result)


def fetch_airtable_records() -> dict[str, str]:
    """Return {url: record_id} for every signal in Airtable."""
    url_to_rec = {}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    params  = {"fields[]": F_URL, "pageSize": 100}
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for rec in data.get("records", []):
            sig_url = rec.get("fields", {}).get("URL", "")
            if sig_url:
                url_to_rec[sig_url] = rec["id"]
        offset = data.get("offset")
        if not offset:
            break
        params["offset"] = offset
    return url_to_rec


def write_predictions_to_airtable(
    predictions: list[dict],
    url_to_rec: dict[str, str],
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Patch Airtable records with Kumo predictions.
    - Always writes Kumo Score (the winning class's score).
    - Writes TL/DR only if score >= CONFIDENCE_THRESHOLD.
    Returns (tldr_written, score_written).
    """
    # Collapse per-class rows into {url: {class, score}} keeping the PREDICTED=true row
    best: dict[str, dict] = {}
    for row in predictions:
        entity = row["ENTITY"]
        if row.get("PREDICTED"):
            best[entity] = {"class": row["CLASS"], "score": row["SCORE"]}

    headers = {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    }
    patch_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_ID}"

    tldr_written = score_written = 0
    batch = []

    for sig_url, pred in best.items():
        rec_id = url_to_rec.get(sig_url)
        if not rec_id:
            print(f"  ⚠️  No Airtable record found for {sig_url}")
            continue

        fields: dict = {F_KUMO_SCORE: round(pred["score"], 4)}
        score_written += 1

        if pred["score"] >= CONFIDENCE_THRESHOLD:
            fields[F_TLDR] = pred["class"]
            tldr_written += 1

        batch.append({"id": rec_id, "fields": fields})

        # Airtable batch limit is 10
        if len(batch) == 10:
            _flush_batch(batch, patch_url, headers, dry_run)
            batch = []

    if batch:
        _flush_batch(batch, patch_url, headers, dry_run)

    return tldr_written, score_written


def _flush_batch(batch: list[dict], url: str, headers: dict, dry_run: bool):
    if dry_run:
        for rec in batch:
            print(f"  [dry-run] PATCH {rec['id']} → {rec['fields']}")
        return
    resp = requests.patch(url, headers=headers, json={"records": batch}, timeout=30)
    resp.raise_for_status()


# ── Main ───────────────────────────────────────────────────────────────────────
async def run_poc():
    df = pd.read_csv(csv_path)
    print(f"\n📂 Loaded: {csv_path}")
    print(f"   {len(df)} signals, {len(df.columns)} columns")

    # Sanitize column names for PQL (no spaces, slashes, special chars)
    col_map = {}
    for col in df.columns:
        safe = re.sub(r"[^A-Za-z0-9_]", "_", col).strip("_")
        safe = re.sub(r"_+", "_", safe)
        col_map[col] = safe
    df_clean = df.rename(columns=col_map)

    tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    clean_csv_path = tmp.name
    df_clean.to_csv(clean_csv_path, index=False)
    tmp.close()

    tldr_col = col_map.get("TL/DR", "TL_DR")

    if "TL/DR" in df.columns:
        dist = df["TL/DR"].value_counts(dropna=False)
        print("\nTL/DR distribution:")
        for val, count in dist.items():
            label = str(val) if str(val) != "nan" else "(unclassified)"
            print(f"  {label}: {count}")

    unclassified_urls = []
    if "TL/DR" in df.columns and "URL" in df.columns:
        unc = df[df["TL/DR"].isna()]
        unclassified_urls = list(unc["URL"].dropna())  # all unclassified
    print(f"\n   Predicting for {len(unclassified_urls)} unclassified signals")

    # ── Fetch Airtable record map ───────────────────────────────────────────────
    url_to_rec: dict[str, str] = {}
    if not args.dry_run:
        print("\n📋 Fetching Airtable record IDs...")
        url_to_rec = fetch_airtable_records()
        print(f"   Found {len(url_to_rec)} records in Airtable")
    else:
        print("\n[dry-run] Skipping Airtable record fetch")

    # ── Start KumoRFM MCP server ────────────────────────────────────────────────
    print("\n🔌 Starting KumoRFM MCP server...")
    server_params = StdioServerParameters(
        command=os.path.expanduser("~/kumo-venv/bin/python"),
        args=["-m", "kumo_rfm_mcp.server"],
        env={**os.environ, "KUMO_API_KEY": KUMO_API_KEY},
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            tools_resp = await session.list_tools()
            print(f"   {len(tools_resp.tools)} tools available\n")

            # Step 1: Register table
            print("🕸️  Step 1: Configure graph...")
            await session.call_tool(
                "update_graph_metadata",
                {
                    "update": {
                        "tables_to_add": [
                            {"name": "signals", "path": clean_csv_path, "primary_key": "URL"}
                        ]
                    }
                },
            )

            # Step 2: Materialize
            print("⚙️  Step 2: Materialize graph...")
            mat = json.loads(tool_result_text(await session.call_tool("materialize_graph", {})))
            print(f"   {mat['num_nodes']} nodes, {mat['num_edges']} edges")

            # Step 3: Predict TL/DR in batches of 100 (API limit per call)
            print(f"\n🤖 Step 3: Predict TL/DR for {len(unclassified_urls)} signals...")
            pql = f"PREDICT signals.{tldr_col} FOR EACH signals.URL"
            print(f"   PQL: {pql}")

            all_predictions = []
            batch_size = 100
            for i in range(0, len(unclassified_urls), batch_size):
                batch_urls = unclassified_urls[i : i + batch_size]
                print(f"   Batch {i // batch_size + 1}: {len(batch_urls)} signals...", end=" ", flush=True)
                result = await session.call_tool(
                    "predict",
                    {
                        "query": pql,
                        "indices": batch_urls,
                        "anchor_time": "2026-06-07T00:00:00",
                    },
                )
                payload = json.loads(tool_result_text(result))
                batch_preds = payload.get("predictions", [])
                all_predictions.extend(batch_preds)
                logs = payload.get("logs", [])
                print(f"→ {len([p for p in batch_preds if p.get('PREDICTED')])} classified  [{logs[-1] if logs else ''}]")

            # ── Summary table ───────────────────────────────────────────────────
            print(f"\n{'─'*60}")
            print(f"{'URL':<50} {'CLASS':<12} {'SCORE':>6}")
            print(f"{'─'*60}")
            high_conf = []
            for row in all_predictions:
                if row.get("PREDICTED"):
                    url_short = row["ENTITY"].split("/")[-1]
                    score = row["SCORE"]
                    cls   = row["CLASS"]
                    marker = "✓" if score >= CONFIDENCE_THRESHOLD else "~"
                    print(f"  {marker} {url_short:<48} {cls:<12} {score:.4f}")
                    if score >= CONFIDENCE_THRESHOLD:
                        high_conf.append(row)
            print(f"{'─'*60}")
            print(f"  {len(high_conf)}/{len(unclassified_urls)} above {CONFIDENCE_THRESHOLD} threshold → will write TL/DR")

            # ── Write to Airtable ───────────────────────────────────────────────
            print(f"\n{'📝' if not args.dry_run else '🔎'} Step 4: {'Writing' if not args.dry_run else 'Dry-run'} to Airtable...")
            tldr_n, score_n = write_predictions_to_airtable(
                all_predictions, url_to_rec, dry_run=args.dry_run
            )
            print(f"   TL/DR written: {tldr_n}")
            print(f"   Kumo Score written: {score_n}")

            print("\n✓ POC complete.")

    try:
        os.unlink(clean_csv_path)
    except Exception:
        pass


if __name__ == "__main__":
    asyncio.run(run_poc())
