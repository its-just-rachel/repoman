# Source Evaluation Agent — Specification

**Purpose:** Maintain a high-quality, up-to-date source registry by (1) scoring existing sources against a five-dimension rubric and (2) discovering new candidate sources from the existing ones. The agent feeds the `sources` table in Supabase, which in turn drives which sources the ingestion pipeline pulls from and how frequently.

**Who runs it:** Automated on a schedule, or triggered manually when a new vertical is added or a major new community emerges.

**Frequency:** Monthly for full re-evaluation. Weekly for newly added, unscored sources. On-demand for discovery runs.

---

## Overview: two modes

| Mode | Trigger | What it does |
|---|---|---|
| **Evaluation** | Monthly schedule, or any time unscored sources exist | Scores sources already in the registry against the rubric |
| **Discovery** | Monthly schedule, or on-demand | Finds new candidate sources not yet in the registry |

A full monthly run executes Discovery first, then Evaluation on everything (new and existing).

---

## Tools the agent requires

| Tool | Used for |
|---|---|
| **Supabase API** (read) | Fetching sources to evaluate; checking for duplicates |
| **Supabase API** (write) | Writing scores, tier assignments, and new candidate sources |
| **Web search** | Finding candidate sources in Discovery mode |
| **Web fetch** | Retrieving and sampling content from source URLs |
| **LLM (Claude)** | Scoring content against rubric dimensions; classifying sources |

---

## Mode 1: Evaluation

### What it scores

Each source is scored on five dimensions. The composite score is calculated automatically by the database; the agent only needs to write the five individual scores.

| Dimension | Weight | What it measures |
|---|---|---|
| Relevance | 30% | What fraction of recent content covers Agentic AI or Physical AI |
| Originality | 25% | Whether this source produces original work or republishes others |
| Authority | 20% | Reputation of the authors or institution behind the source |
| Recency | 15% | How frequently and recently it publishes |
| Signal-to-noise | 10% | How much of the relevant content is genuinely novel vs. derivative |

**Tier assignment** (based on composite score — calculated automatically by the database):
- Tier 1 (8.0–10): Pull on every ingestion cycle
- Tier 2 (6.0–7.9): Pull weekly
- Tier 3 (4.0–5.9): Pull monthly or for corroboration only
- Below 4.0: Set `active = false`

---

### Step-by-step workflow

**Step 1 — Fetch sources to evaluate**

Query Supabase for sources where any of the following is true:
- `score_relevance` is null (never been scored)
- `last_evaluated_at` is older than 30 days
- `active = true` and `tier` is null

```
READ from: sources
FILTER: score_relevance IS NULL
        OR last_evaluated_at < now() - interval '30 days'
        OR (active = true AND tier IS NULL)
ORDER BY: last_evaluated_at ASC NULLS FIRST
LIMIT: 50 per run (to avoid rate limiting and cost overruns)
```

---

**Step 2 — For each source: fetch and sample content**

Fetch the source URL and retrieve a representative sample of recent content.

- For RSS feeds: retrieve the last 20 items (title + description/excerpt)
- For blogs or news sites: retrieve the landing page and extract the last 20 article titles and excerpts
- For GitHub: retrieve the README and recent commit messages or release notes
- For community sites (Reddit, HN): retrieve the last 20 post titles and top-line text
- For paid/paywalled sources: retrieve whatever is publicly visible (abstracts, titles, previews); note the limitation

If the source returns an error (404, timeout, access denied):
- Set `active = false`
- Write `notes`: "Unreachable as of [date]: [error]"
- Skip scoring and move to next source

If the source has no content newer than 90 days:
- Write `notes`: "Appears inactive — no content since [last item date]"
- Set score_recency to 2
- Continue scoring remaining dimensions on available content

---

**Step 3 — Score: Relevance**

Use the sampled content to score how much of it covers the active verticals.

*Active verticals to check against:*
- **Agentic AI:** autonomous AI agents, multi-agent systems, agent orchestration, agent memory, tool use / function calling, agent reasoning, LLM planning, agent evaluation, agentic workflows, human-in-the-loop systems
- **Physical AI:** robotics, embodied AI, sim-to-real transfer, dexterous manipulation, robot perception, motion planning, human-robot interaction, physical world models, edge inference for robotics, sensor fusion

**Prompt to send to LLM:**

```
You are evaluating a data source for a technology intelligence system focused on Agentic AI and Physical AI.

Here are the titles and excerpts from the last [N] items published by [SOURCE NAME] at [URL]:

[PASTE SAMPLE CONTENT]

Count how many of these items are substantially about:
- Agentic AI: autonomous AI agents, multi-agent systems, agent orchestration, agent memory, tool use, agent reasoning, LLM-based planning, or agentic workflows
- Physical AI: robotics, embodied AI, sim-to-real transfer, dexterous manipulation, robot perception, motion planning, or physical world models

Respond in JSON:
{
  "relevant_count": <integer>,
  "total_count": <integer>,
  "agentic_count": <integer>,
  "physical_count": <integer>,
  "score": <number 0-10, where 10 = all content is relevant, 5 = half, 0 = none>,
  "reasoning": "<one sentence>"
}
```

Write the returned `score` to `sources.score_relevance`.
Write the returned `agentic_count` and `physical_count` to `sources.vertical_coverage` as an array (e.g. `["agentic_ai", "physical_ai"]` if both are non-zero).

---

**Step 4 — Score: Originality**

Evaluate whether the source produces original work or republishes others'.

**Prompt:**

```
Based on this sample from [SOURCE NAME] at [URL], classify this source's originality:

[PASTE SAMPLE CONTENT]

Classify into one of these categories:
- "primary_research": Publishes original research, experiments, or findings first released here (score: 9–10)
- "original_analysis": Publishes original commentary, analysis, or opinion about the field (score: 6–8)
- "curated_aggregation": Curates and summarizes work from other sources with meaningful editorial (score: 4–6)
- "republisher": Republishes others' content with minimal original contribution (score: 1–3)

Respond in JSON:
{
  "classification": "<one of the four categories above>",
  "score": <number 0-10>,
  "reasoning": "<one sentence>"
}
```

Write the returned `score` to `sources.score_originality`.

---

**Step 5 — Score: Authority**

Evaluate the reputation and credibility of the source.

**Prompt:**

```
Evaluate the authority and reputation of [SOURCE NAME] at [URL].

Context: This source is being evaluated for a technology intelligence system focused on Agentic AI and Physical AI, used in a federal consulting context.

Consider:
- Is this a top-tier AI research institution or lab? (e.g. DeepMind, Anthropic, OpenAI, Google Brain, Meta AI, NVIDIA, top universities like MIT, Stanford, CMU, Oxford)
- Is this a peer-reviewed academic venue? (conference proceedings, journals)
- Is this a named practitioner with a demonstrated track record in the field?
- Is this a known, established community platform?
- Is this anonymous, unknown, or unverified?

Respond in JSON:
{
  "authority_tier": "<'top_institution' | 'peer_reviewed' | 'established_practitioner' | 'known_community' | 'unknown'>",
  "score": <number 0-10>,
  "reasoning": "<one sentence noting what establishes or limits the authority>"
}
```

Write the returned `score` to `sources.score_authority`.

---

**Step 6 — Score: Recency**

Calculate from the sampled content — no LLM needed.

Look at the publication dates of the sampled items and calculate the average time between posts:

| Average interval | Score |
|---|---|
| Daily or more frequent | 10 |
| 2–3 times per week | 9 |
| Weekly | 8 |
| Bi-weekly | 6 |
| Monthly | 4 |
| Less than monthly or irregular | 2 |
| No content in 90 days | 1 |

Write the calculated score to `sources.score_recency`.
Write the calculated frequency as text (e.g. "weekly", "bi-weekly") to `sources.update_frequency`.

---

**Step 7 — Score: Signal-to-noise**

Among the relevant content, assess how much is genuinely novel versus widely-known or derivative.

**Prompt (run only on the items flagged as relevant in Step 3):**

```
The following items are from [SOURCE NAME] and have been identified as relevant to Agentic AI or Physical AI:

[PASTE RELEVANT ITEMS ONLY]

For each item, assess whether it is:
- "novel": Covers an emerging concept, early-stage technology, or observation not yet widely discussed
- "derivative": Covers something already well-established, widely reported, or a rehash of known concepts

Respond in JSON:
{
  "novel_count": <integer>,
  "derivative_count": <integer>,
  "score": <number 0-10, where 10 = all relevant content is novel, 5 = mix, 0 = all derivative>,
  "reasoning": "<one sentence>"
}
```

Write the returned `score` to `sources.score_signal_noise`.

---

**Step 8 — Write results to Supabase**

After all five scores are collected, write back to the `sources` table:

```
UPDATE sources SET
  score_relevance    = <from step 3>,
  score_originality  = <from step 4>,
  score_authority    = <from step 5>,
  score_recency      = <from step 6>,
  score_signal_noise = <from step 7>,
  vertical_coverage  = <from step 3>,
  update_frequency   = <from step 6>,
  tier               = CASE
                         WHEN composite_score >= 8.0 THEN 1
                         WHEN composite_score >= 6.0 THEN 2
                         WHEN composite_score >= 4.0 THEN 3
                         ELSE NULL
                       END,
  active             = CASE WHEN composite_score < 4.0 THEN false ELSE active END,
  last_evaluated_at  = now()
WHERE id = <source_id>
```

Note: `composite_score` is calculated automatically by the database from the five scores — the agent does not need to calculate it manually.

---

**Step 9 — Log the evaluation run**

After completing all sources in the batch, write a summary log entry (to be stored in a `evaluation_runs` table — see Developer Notes below):

```
{
  "run_type": "evaluation",
  "sources_evaluated": <count>,
  "sources_skipped": <count>,
  "sources_deactivated": <count>,
  "new_tier_1": <count>,
  "new_tier_2": <count>,
  "new_tier_3": <count>,
  "run_at": <timestamp>
}
```

---

## Mode 2: Discovery

### What it does

Starting from active sources already in the registry, the discovery agent finds new candidate sources that are not yet tracked. Candidates are added to the `sources` table with `active = false` until they are evaluated.

---

### Step-by-step workflow

**Step 1 — Pull the seed list**

Fetch active Tier 1 and Tier 2 sources from Supabase:

```
READ from: sources
FILTER: active = true AND tier IN (1, 2)
```

These are the starting points for discovery. Also use the static seed categories below if the registry is sparse.

**Static seed categories to search against (always include these):**
- arXiv cs.AI, cs.RO, cs.LG, cs.MA
- Major lab blogs: DeepMind, Anthropic, OpenAI, Google DeepMind, Meta AI, NVIDIA AI, Microsoft Research
- Key conferences: NeurIPS, ICML, ICLR, ICRA, CoRL, RSS, CVPR
- Practitioner newsletters: The Batch (deeplearning.ai), Import AI, Last Week in AI, The Gradient
- Community platforms: Hugging Face blog, Papers With Code, Hacker News (filtered), r/MachineLearning, r/robotics
- GitHub trending (filtered to AI/robotics repos)

---

**Step 2 — Generate discovery queries**

For each seed source, generate 3–5 web search queries designed to find related sources:

**Prompt:**

```
You are building a discovery list for a technology intelligence system focused on Agentic AI and Physical AI.

Given this source: [SOURCE NAME] at [URL]

Generate 5 web search queries that would help find OTHER sources (blogs, newsletters, research groups, communities, GitHub orgs, RSS feeds) that cover similar topics. 

Focus on finding primary sources — places that publish original content — not aggregators or search engines.

Return a JSON array of 5 search query strings. Queries should be specific enough to surface niche sources, not generic enough to return Wikipedia.
```

---

**Step 3 — Run searches and collect candidates**

For each generated query, run a web search and collect the URLs from results.

Filter out:
- URLs already in the `sources` table (check Supabase)
- Generic search engines, Wikipedia, YouTube, LinkedIn profiles
- Single articles (keep the domain/publication, not the specific URL)
- Anything that is clearly not a recurring publication or community

Normalize URLs to their root domain/feed level (e.g. `https://newsletter.example.com` not `https://newsletter.example.com/issues/42`).

---

**Step 4 — Pre-screen candidates**

Before adding to the database, do a quick relevance pre-screen on each candidate to avoid polluting the registry with noise.

**Prompt:**

```
Evaluate whether this source is a plausible candidate for a technology intelligence system focused on Agentic AI and Physical AI.

Source: [URL]
[Paste title, description, or landing page excerpt if available]

Respond in JSON:
{
  "plausible": true | false,
  "reason": "<one sentence>",
  "likely_source_type": "<'rss' | 'api' | 'scrape' | 'community' | 'newsletter' | 'paid' | 'academic' | 'social'>"
}
```

Only candidates where `plausible = true` proceed to the next step.

---

**Step 5 — Add candidates to Supabase**

For each pre-screened candidate, insert a new row into `sources`:

```
INSERT into sources (
  name,
  url,
  source_type,
  active,
  notes
) VALUES (
  <inferred name from page title or domain>,
  <normalized URL>,
  <from pre-screen>,
  false,                    -- inactive until evaluated
  'Discovered by agent on [date] via [seed source name]'
)
```

If a URL already exists in the table (duplicate check), skip it.

---

**Step 6 — Trigger evaluation on new candidates**

After discovery completes, pass all newly inserted sources (where `active = false` and `score_relevance IS NULL`) to the Evaluation mode workflow.

Sources that score below 4.0 composite remain `active = false` and are effectively archived. Sources that score 4.0 or above are set to `active = true` and assigned a tier.

---

## Edge cases and error handling

| Situation | How to handle |
|---|---|
| Source URL returns 404 or timeout | Set `active = false`, write error to `notes`, skip scoring |
| Source is behind a paywall | Score on available metadata only; write "paywall — partial evaluation" to `notes` |
| Source has no content newer than 90 days | Score recency as 2, continue with other dimensions, write "possibly inactive" to `notes` |
| LLM returns malformed JSON | Retry once; if still malformed, skip that dimension and write null; flag source for manual review |
| Duplicate URL found during discovery | Skip without inserting |
| Rate limit hit on web fetch | Pause 10 seconds and retry up to 3 times; if still failing, defer to next run |
| Score changes tier significantly (e.g. Tier 1 → Tier 3) | Write to evaluation log; flag for human review before changing ingestion schedule |

---

## Scheduling

| Run type | Schedule | Notes |
|---|---|---|
| Full run (discovery + evaluation) | Monthly, first Monday | Evaluates all sources; runs discovery from Tier 1+2 seed list |
| Evaluation only (unscored sources) | Weekly | Processes any sources added since the last full run |
| On-demand discovery | Manual trigger | Use when adding a new vertical or after a significant field development |
| Re-evaluation (single source) | Manual trigger | For when a source's quality changes noticeably |

---

## Database operations summary

| Operation | Table | When |
|---|---|---|
| READ | `sources` | Start of each evaluation batch; duplicate check during discovery |
| UPDATE | `sources` | After scoring: writes all five dimension scores, tier, active status, timestamps |
| INSERT | `sources` | New candidates found during discovery |
| INSERT | `evaluation_runs` *(see below)* | End of each run for audit logging |

---

## Developer notes

**`evaluation_runs` table:** This table is not yet in the schema. Add it to track audit history:

```sql
create table evaluation_runs (
  id                    uuid primary key default gen_random_uuid(),
  run_type              text not null check (run_type in ('evaluation', 'discovery', 'full')),
  sources_evaluated     integer,
  sources_discovered    integer,
  sources_deactivated   integer,
  tier_changes          jsonb,
  errors                jsonb,
  run_at                timestamptz not null default now()
);
```

**LLM model:** Use Claude Haiku for the scoring prompts (fast, low cost, sufficient for structured JSON extraction). Use Claude Sonnet for the discovery query generation and pre-screening steps where judgment quality matters more.

**Parallelism:** Sources can be evaluated in parallel (up to 10 at a time) to keep monthly runs fast. Discovery queries should be rate-limited to avoid search API throttling.

**Prompt versioning:** Store the prompt text and model version used in the `evaluation_runs` log. If prompts change, re-evaluation of affected sources should be triggered so scores are comparable.

**Cost estimate:** At roughly 1,000 tokens per source evaluation (5 prompts × ~200 tokens each), 100 sources costs approximately $0.02–0.05 with Haiku. Discovery adds web search API costs depending on provider.

**Transparency requirement:** Every score written to the database should be accompanied by the LLM's `reasoning` field stored somewhere auditable. Consider adding a `score_notes` jsonb column to the `sources` table to store the per-dimension reasoning strings.
