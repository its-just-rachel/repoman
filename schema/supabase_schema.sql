-- =============================================================
-- Secret Agent — Supabase Database Schema
-- Run this entire file in the Supabase SQL Editor to set up
-- all tables, indexes, and triggers.
-- =============================================================

-- Enable UUID generation
create extension if not exists "uuid-ossp";


-- =============================================================
-- SOURCES
-- Every data source the pipeline ingests from.
-- Scored against a five-dimension rubric; composite score
-- is calculated automatically and drives tier assignment.
-- =============================================================

create table sources (
  id                  uuid primary key default gen_random_uuid(),
  name                text not null,
  url                 text not null,

  -- What kind of source this is
  source_type         text not null check (source_type in (
                        'rss', 'api', 'scrape', 'community',
                        'newsletter', 'paid', 'academic', 'social'
                      )),

  -- Which verticals and quadrants this source tends to cover
  vertical_coverage   text[] not null default '{}',
  quadrant_affinity   text[] default '{}',

  -- Five-dimension rubric scores (each 0–10)
  score_relevance     numeric(4,2) check (score_relevance between 0 and 10),
  score_originality   numeric(4,2) check (score_originality between 0 and 10),
  score_authority     numeric(4,2) check (score_authority between 0 and 10),
  score_recency       numeric(4,2) check (score_recency between 0 and 10),
  score_signal_noise  numeric(4,2) check (score_signal_noise between 0 and 10),

<<<<<<< HEAD
  -- Reasoning behind each score, stored as JSON for auditability
  -- e.g. {"relevance": "14 of 20 items covered agentic AI", "authority": "Top-tier lab (DeepMind)"}
  score_notes         jsonb,

=======
>>>>>>> b5175dc9ad96d92ada5719af788225e85dfd22e0
  -- Composite score: auto-calculated from the five dimensions
  -- Weights: relevance 30% | originality 25% | authority 20% | recency 15% | signal/noise 10%
  composite_score     numeric(4,2) generated always as (
                        coalesce(score_relevance,    0) * 0.30 +
                        coalesce(score_originality,  0) * 0.25 +
                        coalesce(score_authority,    0) * 0.20 +
                        coalesce(score_recency,      0) * 0.15 +
                        coalesce(score_signal_noise, 0) * 0.10
                      ) stored,

  -- Tier assigned based on composite score
  -- 1 = pull every cycle (8–10), 2 = weekly (6–7.9), 3 = monthly (4–5.9)
  tier                integer check (tier in (1, 2, 3)),

  -- Access and cost
  access_method       text check (access_method in ('free', 'rate_limited', 'freemium', 'paid')),
  cost_notes          text,
  update_frequency    text,

  -- Tracking
  last_evaluated_at   timestamptz,
  last_pulled_at      timestamptz,
  active              boolean not null default true,
  notes               text,

  created_at          timestamptz not null default now(),
  updated_at          timestamptz not null default now()
);


-- =============================================================
-- TAXONOMY NODES
-- The classification system for signals. Nodes can be
-- human-defined or algorithmically surfaced. Organized in a
-- hierarchy (parent_id references another node).
-- =============================================================

create table taxonomy_nodes (
  id                  uuid primary key default gen_random_uuid(),
  name                text not null,
  slug                text not null unique,   -- URL-safe identifier e.g. "agent-memory"
  description         text,

  -- Hierarchy: a node can have a parent (e.g. "agent memory" under "memory systems")
  parent_id           uuid references taxonomy_nodes(id),

  -- Which verticals and quadrants this node is associated with
  vertical_tags       text[] default '{}',
  quadrant_affinity   text[] default '{}',

  -- How this node was created
  origin              text not null default 'human' check (origin in ('human', 'algorithmic')),

  -- Lifecycle
  status              text not null default 'active' check (status in ('active', 'candidate', 'archived')),

  -- Count of signals currently matched to this node (updated by pipeline)
  signal_count        integer not null default 0,

  created_at          timestamptz not null default now(),
  updated_at          timestamptz not null default now()
);


-- =============================================================
-- TAXONOMY NEIGHBORHOOD FLAGS
-- When the system detects a cluster of signals that share
-- semantic space but don't fit any existing node cleanly,
-- it raises a flag here for human review. Accepted flags
-- become new taxonomy nodes.
-- =============================================================

create table taxonomy_neighborhood_flags (
  id                  uuid primary key default gen_random_uuid(),
  candidate_name      text not null,          -- Suggested name for the new node
  description         text,                   -- What the system thinks this cluster is about
  signal_count        integer not null default 1,
  example_signal_ids  uuid[] default '{}',    -- A few signals that triggered the flag

  status              text not null default 'pending_review' check (
                        status in ('pending_review', 'accepted', 'rejected')
                      ),

  -- If accepted, points to the newly created taxonomy node
  accepted_node_id    uuid references taxonomy_nodes(id),

  created_at          timestamptz not null default now(),
  reviewed_at         timestamptz,
  reviewed_by         text
);


-- =============================================================
-- SIGNALS
-- Individual signals captured from sources. Each signal is
-- enriched by the pipeline with classification, taxonomy
-- matches, confidence scores, and pipeline stage placement.
-- =============================================================

create table signals (
  id                    uuid primary key default gen_random_uuid(),
  source_id             uuid references sources(id),

  -- Raw content from the source
  title                 text not null,
  url                   text,
  author                text,
  published_at          timestamptz,
  captured_at           timestamptz not null default now(),
  raw_excerpt           text,

  -- Enriched content (LLM-generated)
  summary               text,

  -- Classification
  -- Quadrant: what kind of thing this signal is about
  quadrant              text check (quadrant in (
                          'tools', 'techniques', 'platforms', 'frameworks_languages'
                        )),
  -- Verticals: which domains this signal touches (can be more than one)
  verticals             text[] default '{}',

  -- Where this signal sits in the pipeline
  pipeline_status       text not null default 'outer_ring' check (
                          pipeline_status in (
                            'outer_ring', 'research', 'solution',
                            'prototype', 'advise', 'hold', 'dismissed'
                          )
                        ),

  -- Confidence in the enrichment (0–1)
  confidence_score      numeric(4,3) check (confidence_score between 0 and 1),

  -- Fidelity: how validated/rigorous is the underlying claim (0–10)
  fidelity_score        numeric(4,2) check (fidelity_score between 0 and 10),

  -- Availability: how accessible/deployable is the thing this signal describes (0–10)
  availability_score    numeric(4,2) check (availability_score between 0 and 10),

  -- How many other signals corroborate this one
  corroboration_count   integer not null default 0,

  -- Enrichment audit trail
  enrichment_rationale  text,    -- Why the agent classified it this way
  pipeline_suggestion   text,    -- Agent's recommendation for pipeline stage

  created_at            timestamptz not null default now(),
  updated_at            timestamptz not null default now()
);


-- =============================================================
-- SIGNAL TAXONOMY MATCHES
-- Junction table connecting signals to taxonomy nodes.
-- Each match has a strength score (0–1) and a type indicating
-- how the match was made.
-- =============================================================

create table signal_taxonomy_matches (
  id                  uuid primary key default gen_random_uuid(),
  signal_id           uuid not null references signals(id) on delete cascade,
  taxonomy_node_id    uuid not null references taxonomy_nodes(id),

  -- How strong is the match (cosine similarity or equivalent)
  match_strength      numeric(4,3) not null check (match_strength between 0 and 1),

  -- How the match was determined
  match_type          text not null check (match_type in ('semantic', 'lexical', 'hierarchical')),

  -- Is this one of the top matches shown on the card face?
  is_primary          boolean not null default false,

  created_at          timestamptz not null default now(),

  unique (signal_id, taxonomy_node_id)
);


-- =============================================================
-- SIGNAL CORROBORATIONS
-- Tracks when two signals are related — one corroborates,
-- extends, contradicts, or references another. Used for
-- corroboration counts and cross-source validation.
-- =============================================================

create table signal_corroborations (
  id                        uuid primary key default gen_random_uuid(),
  signal_id                 uuid not null references signals(id) on delete cascade,
  corroborating_signal_id   uuid not null references signals(id) on delete cascade,

  relationship_type         text not null check (relationship_type in (
                              'corroborates', 'contradicts', 'extends', 'references'
                            )),

  created_at                timestamptz not null default now(),

  -- Prevent duplicate pairs
  unique (signal_id, corroborating_signal_id)
);


-- =============================================================
-- SIGNAL ANNOTATIONS
-- Human-added context: comments, tags, flags, and decisions
-- (promote to next stage or dismiss).
-- =============================================================

create table signal_annotations (
  id                uuid primary key default gen_random_uuid(),
  signal_id         uuid not null references signals(id) on delete cascade,

  annotation_type   text not null check (annotation_type in (
                      'comment', 'tag', 'flag', 'promote', 'dismiss'
                    )),

  content           text,       -- The comment text, tag value, or note
  created_by        text,       -- Who added this annotation

  created_at        timestamptz not null default now()
);


-- =============================================================
-- INDEXES
-- Optimized for the most common query patterns:
-- filtering signals by pipeline stage, source, or date;
-- looking up taxonomy matches by signal or node;
-- ranking sources by score/tier.
-- =============================================================

create index idx_signals_pipeline_status      on signals (pipeline_status);
create index idx_signals_source_id            on signals (source_id);
create index idx_signals_published_at         on signals (published_at desc);
create index idx_signals_captured_at          on signals (captured_at desc);
create index idx_signals_verticals            on signals using gin (verticals);

create index idx_stm_signal_id               on signal_taxonomy_matches (signal_id);
create index idx_stm_taxonomy_node_id        on signal_taxonomy_matches (taxonomy_node_id);
create index idx_stm_match_strength          on signal_taxonomy_matches (match_strength desc);
create index idx_stm_is_primary              on signal_taxonomy_matches (is_primary) where is_primary = true;

create index idx_sources_composite_score     on sources (composite_score desc);
create index idx_sources_tier                on sources (tier);
create index idx_sources_active              on sources (active) where active = true;

create index idx_taxonomy_parent_id          on taxonomy_nodes (parent_id);
create index idx_taxonomy_status             on taxonomy_nodes (status);
create index idx_taxonomy_slug               on taxonomy_nodes (slug);

create index idx_neighborhood_status         on taxonomy_neighborhood_flags (status);


-- =============================================================
-- UPDATED_AT TRIGGER
-- Automatically updates the updated_at field whenever a
-- row is modified.
-- =============================================================

create or replace function update_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

create trigger sources_updated_at
  before update on sources
  for each row execute function update_updated_at();

create trigger signals_updated_at
  before update on signals
  for each row execute function update_updated_at();

create trigger taxonomy_nodes_updated_at
  before update on taxonomy_nodes
  for each row execute function update_updated_at();


-- =============================================================
-- ROW LEVEL SECURITY
-- Supabase enables RLS by default. The lines below allow full
-- access for authenticated users during development. Tighten
-- these policies before any public-facing deployment.
-- =============================================================

<<<<<<< HEAD
-- =============================================================
-- EVALUATION RUNS
-- Audit log of every source evaluation or discovery run.
-- Tracks what the agent did, how many sources it touched,
-- any tier changes, and any errors encountered.
-- =============================================================

create table evaluation_runs (
  id                    uuid primary key default gen_random_uuid(),

  -- Type of run
  run_type              text not null check (run_type in ('evaluation', 'discovery', 'full')),

  -- Summary counts
  sources_evaluated     integer,
  sources_discovered    integer,
  sources_deactivated   integer,

  -- Detailed records of tier changes and errors, stored as JSON arrays
  -- tier_changes example: [{"source_id": "...", "from": 1, "to": 3, "reason": "..."}]
  -- errors example: [{"source_id": "...", "url": "...", "error": "404 not found"}]
  tier_changes          jsonb,
  errors                jsonb,

  -- Model and prompt version used (for reproducibility)
  model_used            text,
  prompt_version        text,

  run_at                timestamptz not null default now()
);


-- =============================================================
-- ROW LEVEL SECURITY
-- =============================================================

alter table sources                     enable row level security;
alter table signals                     enable row level security;
alter table taxonomy_nodes              enable row level security;
alter table taxonomy_neighborhood_flags enable row level security;
alter table signal_taxonomy_matches     enable row level security;
alter table signal_corroborations       enable row level security;
alter table signal_annotations          enable row level security;
alter table evaluation_runs             enable row level security;

-- Allow all operations for authenticated users (development policy)
create policy "authenticated full access" on sources                     for all to authenticated using (true) with check (true);
create policy "authenticated full access" on signals                     for all to authenticated using (true) with check (true);
create policy "authenticated full access" on taxonomy_nodes              for all to authenticated using (true) with check (true);
create policy "authenticated full access" on taxonomy_neighborhood_flags for all to authenticated using (true) with check (true);
create policy "authenticated full access" on signal_taxonomy_matches     for all to authenticated using (true) with check (true);
create policy "authenticated full access" on signal_corroborations       for all to authenticated using (true) with check (true);
create policy "authenticated full access" on signal_annotations          for all to authenticated using (true) with check (true);
create policy "authenticated full access" on evaluation_runs             for all to authenticated using (true) with check (true);
=======
alter table sources                    enable row level security;
alter table signals                    enable row level security;
alter table taxonomy_nodes             enable row level security;
alter table taxonomy_neighborhood_flags enable row level security;
alter table signal_taxonomy_matches    enable row level security;
alter table signal_corroborations      enable row level security;
alter table signal_annotations         enable row level security;

-- Allow all operations for authenticated users (development policy)
create policy "authenticated full access" on sources                    for all to authenticated using (true) with check (true);
create policy "authenticated full access" on signals                    for all to authenticated using (true) with check (true);
create policy "authenticated full access" on taxonomy_nodes             for all to authenticated using (true) with check (true);
create policy "authenticated full access" on taxonomy_neighborhood_flags for all to authenticated using (true) with check (true);
create policy "authenticated full access" on signal_taxonomy_matches    for all to authenticated using (true) with check (true);
create policy "authenticated full access" on signal_corroborations      for all to authenticated using (true) with check (true);
create policy "authenticated full access" on signal_annotations         for all to authenticated using (true) with check (true);
>>>>>>> b5175dc9ad96d92ada5719af788225e85dfd22e0
