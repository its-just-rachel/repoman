# Frontier Tech — Signal Submission App

A lightweight local web app for submitting URLs for immediate TL/DR analysis.
Paste a link, get a Tech Lead / Don't Read classification with rationale in ~10 seconds.
The signal is saved to Airtable and will appear in the next daily digest.

## Local setup

```bash
# From the repo root:
cd webapp/
pip install -r requirements.txt
python app.py
```

Then open **http://localhost:5000** in your browser.

## Environment variables

The app reads credentials from (in order):
1. `webapp/.env`
2. `ingest/.env` (fallback — uses the same credentials as the ingestion pipeline)

Required variables:
```
AIRTABLE_TOKEN=patXXXXXXXXXXXXXX.XXXX...
ANTHROPIC_API_KEY=sk-ant-XXXXXXXXXXXXXXXX
```

If you've already set up the ingestion pipeline, no extra configuration is needed —
the app will pick up credentials from `ingest/.env` automatically.

## How it works

1. Paste a URL (and an optional context note) into the form
2. The app fetches the article content, then calls Claude Sonnet to classify it
3. Result appears inline: Tech Lead or Don't Read, with a Why Read/Why Skip rationale,
   a practitioner summary, and quadrant/vertical tags
4. The signal is written to Airtable tagged as "Submitted" — it will appear in the
   next daily digest email alongside pipeline-discovered signals

## Deployment

For a shareable URL (so execs can access it without running it locally):
- **Vercel**: deploy `webapp/` as a Python Flask app (free tier)
- **AWS Lambda**: use the existing Lambda target planned for the ingestion pipeline
- Engineers: review security posture before exposing to the open internet
