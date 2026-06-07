# Frontier Tech

A signal intelligence pipeline for tracking the frontier of **emerging AI and its evolution** — built on a modified Tech Radar model.

Signals enter the outer ring as early-stage chatter, get enriched and classified, and move toward the center as they gain fidelity and momentum. The goal is to surface what's emerging before it becomes obvious, and to bring structured insight to teams who need to make technology decisions.

---

## What this system does

1. **Ingests signals** from a curated set of sources — academic papers, lab blogs, practitioner communities, GitHub, newsletters, and more
2. **Enriches each signal** with classification (quadrant + vertical), taxonomy tags with match strength, credibility scoring, and corroboration detection
3. **Surfaces conceptual neighborhoods** — clusters of signals pointing at an emerging concept that doesn't have a name yet, which drives algorithmic taxonomy expansion
4. **Generates insight** — trend detection, velocity monitoring, and a weekly brief for human review
5. **Presents signals** in a filterable, sortable microblog-style list view with human annotation support

---

## Radar structure

**Quadrants** (what kind of thing a signal is about):
- Tools
- Techniques
- Platforms
- Frameworks / Languages *(Cognitive / Foundation Model work lives here)*

**Verticals** (which domain it operates in):
- *Defined as the pipeline develops — coverage spans AI and adjacent emerging technology*

**Pipeline stages** (how mature/validated a signal is):
```
Surface → Research → Solution → Prototype → Advise
                                              ↓
                                    Dismiss / Hold (at any stage)
```

---

## Repository structure

```
frontier-tech/
├── README.md
├── ingest/                    # Polling ingestion pipeline (local / Lambda)
│   ├── main.py                # Fetch → dedup → enrich → write to Airtable
│   ├── requirements.txt       # Python dependencies
│   ├── setup.sh               # One-shot local setup script
│   └── .env.example           # Environment variable template
└── sources/
    └── harmonic/              # Harmonic Scout intelligence reports
        ├── README.md          # Workflow: drop .md reports here for ingestion
        └── *.md               # Weekly briefings (excluded from git if sensitive)
```

---

## Database

This project uses [Airtable](https://airtable.com) as its primary data store.

**Base:** Frontier Tech (`appwe0lxRHbASBgG2`) — workspace `wspvM6WKzMJ6BPugj`

**Core tables:**

| Table | Purpose |
|---|---|
| `Sources` | Every ingestion source, scored on a five-dimension rubric. Composite score auto-calculated. |
| `Taxonomy Nodes` | Classification hierarchy for signals. Human-defined and algorithmically surfaced. Supports parent-child links. |
| `Signals` | Individual signals captured from sources. Enriched with quadrant, verticals, pipeline stage, and scores. |
| `Signal-Taxonomy Matches` | Junction: signal ↔ taxonomy node, with match strength (0–1) and match type. |
| `Signal Corroborations` | Junction: signal ↔ signal, with relationship type (corroborates / contradicts / extends / references). |
| `Signal Annotations` | Human-added comments, tags, flags, promote/dismiss decisions on signals. |
| `Taxonomy Neighborhood Flags` | Candidate new taxonomy nodes surfaced algorithmically — pending human review. |
| `Evaluation Runs` | Audit log of every source evaluation or discovery run. |

**Kumo.ai integration note:** KumoRFM 2.0 is a relational foundation model (zero training, zero feature engineering) that runs predictions on multi-table data. Two integration paths:
- **Experimentation now:** `pip install kumo-rfm-mcp` — works directly from local CSV/Parquet exports. No warehouse required. Free trial at kumorfm.ai.
- **Production:** Airtable (operational store) → warehouse sync (Snowflake/BigQuery/Databricks/PostgreSQL) → Kumo for predictive modeling at scale.

---

## Source evaluation rubric

Sources are scored across five dimensions, producing a composite score that drives tier assignment:

| Dimension | Weight | What it measures |
|---|---|---|
| Relevance | 30% | Fraction of content relevant to active verticals |
| Originality | 25% | Primary research vs. aggregation/republishing |
| Authority | 20% | Reputation of authors or institution |
| Recency | 15% | Update frequency and content freshness |
| Signal-to-noise | 10% | Novelty within relevant content |

**Tiers:** Tier 1 (score 8–10) · Tier 2 (6–7.9) · Tier 3 (4–5.9) · Removed (<4)

---

## Status

🟡 In design and early setup. Database schema defined. Agent pipeline and dashboard in development.
