# ML rigour: evaluation evidence and measured ranking improvements — design

Date: 2026-11-20 · Status: draft for approval (candidate decisions recorded in §5.1 and §5.7) · Approach A (chosen by the candidate)

## 1. Intent

The candidate will defend the recommendation side in an interview with ML engineers, and wants the recommendations
themselves to get better. Today's evidence (`DESIGN.md` §6, `eval/`) rests on an industry proxy and 30 AI-picked
Malay↔English pairs, with one confidence interval and no paired tests; ranking is plain cosine with no threshold and
no handling of identical-description neighbours.

This work produces (1) evidence that survives hostile questions and (2) ranking changes that ship **only** where
that evidence says they help.

## 2. Goals, non-goals, anti-goals

**Goals**
- A graded relevance set that measures "good recommendation" better than industry: fine-grained business activity.
- Every reported number has a 95% confidence interval; every model/ranking comparison has a paired test.
- Breakdowns: by query language (Malay/English), by industry, and for fallback-embedded companies; a worst-failures
  table with an explanation per row.
- A model sweep over fastembed's multilingual models, with quality, embedding latency and disk size.
- Ranking changes (identical-description handling, minimum score) tuned on one half of the data and reported on the
  other; shipped to the API only when they win.
- `DESIGN.md` §6 rewritten; a one-page interview brief.

**Non-goals**
- Fine-tuning, training a re-ranker, or any learned component (rejected: approach C — trains on our own labels).
- Click/online evaluation (no traffic exists); described as next step only.
- LLM-generated explanations.

**Anti-goals (failure even if it "works")**
- Labels changed after seeing model results (circular evaluation).
- Tuning and reporting on the same queries.
- Shipping a change whose gain's CI includes zero, or claiming a difference the CIs do not support.
- Breaking the API contract, the order-independence guarantee, or the deterministic tie order.

## 3. Constraints

- CPU only, no paid APIs; fastembed models only (confirmed list includes
  `paraphrase-multilingual-MiniLM-L12-v2` (384, current), `paraphrase-multilingual-mpnet-base-v2` (768, 1.0 GB),
  `intfloat/multilingual-e5-large` (1024, 2.24 GB), `google/embeddinggemma-300m` (768, 1.24 GB)).
- ~4.5 GB of model downloads for the sweep, cached in `~/.cache/fastembed`.
- Schema is `vector(384)`; adopting a different dimension needs an Alembic revision plus `just reembed`.
- The eval reads the dev `companies` table read-only (as today). Tests never download models.
- The data is synthetic and templated: 178 distinct descriptions (180 embedded texts incl. 10 industry fallbacks).

## 4. Approaches (decided)

- **A — evidence first, then improve only what the evidence supports (chosen).**
- B — evidence only, no product change: rejected, the candidate wants better recommendations too.
- C — fine-tune / learned re-ranker: rejected, with 178 templates and self-made labels it learns the labels.

## 5. Design

### 5.1 Relevance labels (`eval/labels/activities.csv`)
- One row per distinct embedded text (180): `text, activity, language, annotator, note`.
  - `activity`: a fine-grained business activity from a fixed vocabulary of ~30–50 snake_case slugs
    (e.g. `cafe_coffee_shop`, `parcel_delivery`, `oil_palm_plantation`), defined in
    `eval/labels/activities.md` with a one-line definition each. Translations of the same business get the same
    activity; a town is not part of the activity.
  - `language`: `ms` | `en` | `n/a` (fallback texts).
  - Fallback texts (`"{industry} business"`) get activity `industry_only` and **never score 2**: a fallback
    neighbour (or a fallback query) is grade 1 if the industries match, else 0. *(Designer's call.)*
- Graded relevance of neighbour *n* for query *q*: **2** same activity, **1** same industry (from the companies
  table), **0** otherwise. A neighbour or query without an industry can still score 2 via activity.
- **Freeze protocol:** labels are drafted from the texts alone (no model output consulted), committed in their own
  git commit on `feat/similar-companies` *before* any new metric is run, and `just eval` prints the labels file's
  sha256 into every results file, and changed afterwards only via a reviewed commit whose message says why.
  `annotator` = `claude-draft` until the candidate reviews a row (then their initials). The write-up states the
  review status honestly.
- **Human gate (decided by the candidate):** after the draft labels are committed, work stops until the candidate has
  reviewed and corrected `activities.csv`; their version is committed, and only then do metrics, the sweep or any
  tuning run.
- The existing `eval/pairs.csv` stays (cross-lingual retrieval is still a distinct question).

### 5.2 Query set and cross-fitting *(designer's call: replaces a single tune/report split)*
- Queries: every active company with an embedding (≈1,988), as the panel would be requested; candidates: all other
  active companies with an embedding — exactly the API's candidate set.
- **5-fold cross-fitting by activity.** Activities are assigned to 5 folds by a seeded greedy balance on query count
  (largest activity first, into the currently smallest fold; seed and assignment printed). Anything tuned (τ, m) is
  chosen on 4 folds and applied to the held-out fold; reported numbers pool the held-out predictions over all 5 folds.
  Every query is then reported out-of-fold, so the report uses all ~1,988 queries instead of half, and a threshold
  tuned on one café is never scored on its copy. `industry_only` fallback texts form one cluster per industry.
- Model comparison (nothing tuned) is reported on all queries.

### 5.3 Metrics (`eval/metrics.py`, pure, unit-tested)
- **nDCG@4** with the graded relevance above (primary).
- **Activity P@4**: share of the 4 neighbours with relevance 2.
- **Coverage**: share of queries returning 4 results (matters once a minimum score exists).
- **Redundancy@4**: share of the 4 neighbours whose text is identical to another neighbour's or the query's.
- Existing metrics kept: industry P@4 (all / other texts), pair recall@1/5 and MRR.
- **τ precision** is a pooled ratio (relevant returned / returned, relevant = grade ≥ 1) computed inside each bootstrap
  resample, so queries that return nothing need no per-query value; coverage is reported beside it.
- **Ranking under evaluation reproduces the API exactly**: cosine distance, then same industry, same state, reg no
  (a shared function used by both the eval and a parity test against `store.SIMILAR_SQL`).

### 5.4 Statistics
- **Confidence intervals:** bootstrap over **activity clusters** (resample activities, take all their queries), 2,000
  resamples, percentile 95% CI, fixed seed. Clustered because copies of one description are not independent queries.
- **Paired comparisons** (model vs model, ranking variant vs baseline): the same cluster bootstrap on the per-query
  *difference*; report mean difference, its 95% CI, and whether it excludes zero. Pair-recall comparisons: exact
  McNemar test on the 30 pairs.
- Breakdowns (language, industry, fallback) report n and CI per row; rows with n < 20 are flagged as indicative.

### 5.5 Error analysis
- For the shipped configuration: the 15 queries with the lowest nDCG@4 on the report half, with query text, its 4
  neighbours (text, activity, score) and a one-line diagnosis each (e.g. "town dominates", "loose translation",
  "fallback text"). Printed by `just eval` and summarised in DESIGN.

### 5.6 Model sweep
- Models: current MiniLM (384), multilingual mpnet (768), multilingual-e5-large (1024), embeddinggemma-300m (768).
  Per-model text prefixes where the model card requires them (e5: `"query: "` on both sides for symmetric
  similarity; embeddinggemma: its documented similarity prompt) — a small table in `eval/models.py`, each prefix
  cited to the model card.
- Report per model: all metrics with CIs, paired difference vs current on nDCG@4, embed time per 100 texts (best of
  3), model size on disk, peak RSS while embedding.
- **Adoption rule:** switch the default only if nDCG@4 gain vs current has a CI excluding zero **and** a mean gain
  ≥ 0.02, **and** embedding 180 texts stays < 60 s on this laptop. If adopted at a new dimension: Alembic revision
  `0003_embedding_dim_<n>` (alter both vector columns; downgrade back to 384), `Settings` default, `just reembed`,
  docs. If not adopted, say why with the numbers.

### 5.7 Ranking variants (evaluated with the chosen model)
- **Identical-description cap:** at most *m* neighbours sharing one text (m ∈ {1, 2, ∞}), **counting the query's own
  text as one of the groups** (with m = 1, at most one neighbour has the query's exact description); fill from the
  next candidates. *(Designer's call.)* Measured on nDCG@4 *and* redundancy@4 — this is a relevance/variety trade-off, and the report says so.
- **Minimum score:** drop neighbours with cosine similarity below τ. τ chosen on the tune half as the smallest value
  giving activity-or-industry precision ≥ 0.9 among returned neighbours; report precision and coverage on the report
  half. τ is per model (stored with the model name).
- **Ship rule (decided by the candidate: evidence decides; "measured and rejected" is an acceptable outcome):** a
  variant ships only if, on the report half, nDCG@4 does not drop (CI of the difference has lower
  bound ≥ −0.01) and its target metric improves with a CI excluding zero (redundancy down for the cap; precision up
  for τ). Otherwise it is documented as evaluated-and-rejected.
- **If shipped, API behaviour** (`docs/superpowers/specs/2026-11-20-company-recommendations-design.md` §7 stays
  authoritative otherwise; the only contract change is the extra `reason` value below):
  - New settings `SIMILAR_MAX_PER_TEXT` (int, default from the decision) and `SIMILAR_MIN_SCORE` (float, default from
    the decision for the default model; unset = no threshold).
  - `/similar` may return fewer than `limit` results; if a threshold leaves none, `similar: []` with
    `reason: "no_close_matches"`. Ordering and tie-breaks unchanged. Implemented in `app/store.py` (SQL over-fetch then
    a small Python filter, or SQL window function — builder's call), with API tests and UI/README updates.

### 5.8 Outputs
- `just eval` prints: model table, ranking-variant table, breakdowns, worst failures; writes the same to
  `eval/results/<date>.md` (committed) so DESIGN cites a file, not a terminal.
- `DESIGN.md` §6 rewritten: question, labels and their status, method, results with CIs, what was shipped and
  rejected and why, limitations.
- `docs/interview-ml.md`: one page — the 10–12 hardest likely questions (who labelled, why trust a synthetic set,
  why not fine-tune, why this threshold, are differences significant, what would you do with real traffic …) each
  answered with a number and a file reference.

## 6. Testing
- Unit tests for every new metric and the bootstrap (hand-built cases with known answers; fixed seed determinism).
- Parity test: the eval's ranking function returns the same order as `SIMILAR_SQL` on a seeded test database.
- If ranking variants ship: API tests for the cap, the threshold, fewer-than-limit results and `no_close_matches`;
  the existing order-independence tests still pass.
- No test downloads a model.

## 7. Left to the builder
- Exact activity vocabulary (within ~30–50 slugs) and file layout under `eval/`.
- Implementation of the cap/threshold (SQL vs Python post-filter), provided ordering parity holds.
- Output formatting of `just eval`.
