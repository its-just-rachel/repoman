# Harmonic Scout Reports

Drop Harmonic Intelligence Briefing `.md` files here and the ingestion pipeline will automatically extract signals from them.

## Workflow

1. Run a Harmonic Scout report at [harmonic.ai](https://harmonic.ai)
2. Copy the output and save it as a `.md` file in this directory
   - Naming convention: `harmonic-intelligence-YYYY-MM-DD.md`
3. Run the pipeline: `python ingest/main.py --source harmonic`
   - Or run `python ingest/main.py` for a full run (all sources)

## What the pipeline does

- Haiku reads each `.md` report and extracts up to 15 discrete signals
- Each signal gets a synthetic URL (`harmonic://report/{filename}/{index}`) for deduplication across runs
- Signals pass through the normal Haiku → Sonnet enrichment pipeline
- The full report text is injected as context into Sonnet's system prompt so "why it matters" summaries are grounded in market data

## Notes

- Reports may contain sensitive market intelligence; review `.gitignore` before committing raw briefings
