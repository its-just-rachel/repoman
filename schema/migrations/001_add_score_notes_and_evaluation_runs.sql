-- =============================================================
-- Migration 001: Add score_notes and evaluation_runs
-- =============================================================
-- Run this in the Supabase SQL Editor if you already ran the
-- original schema (supabase_schema.sql) and need to add these
-- two updates without dropping and recreating everything.
--
-- Safe to run more than once — each statement checks whether
-- the change is needed before applying it.
-- =============================================================


-- 1. Add score_notes column to sources
--    Stores the LLM's reasoning behind each dimension score
--    as a JSON object, e.g.:
--    {"relevance": "14 of 20 items covered agentic AI topics",
--     "authority": "Top-tier research institution (DeepMind)"}

alter table sources
  add column if not exists score_notes jsonb;


-- 2. Create the evaluation_runs table
--    Audit log of every agent run — what it did, what changed,
--    any errors encountered.

create table if not exists evaluation_runs (
  id                    uuid primary key default gen_random_uuid(),
  run_type              text not null check (run_type in ('evaluation', 'discovery', 'full')),
  sources_evaluated     integer,
  sources_discovered    integer,
  sources_deactivated   integer,
  tier_changes          jsonb,
  errors                jsonb,
  model_used            text,
  prompt_version        text,
  run_at                timestamptz not null default now()
);


-- 3. Enable row level security on evaluation_runs

alter table evaluation_runs enable row level security;

create policy "authenticated full access" on evaluation_runs
  for all to authenticated using (true) with check (true);


-- Done. Verify by running:
-- select column_name from information_schema.columns where table_name = 'sources' and column_name = 'score_notes';
-- select count(*) from evaluation_runs;
