# Pabustats Implementation Plan

## Context

The historical `pabustats` project is a separate Django application:

- Repository: `https://github.com/Grzesiek3713/pabustats`
- Former public entrypoint noted in its README: `https://pabulib.org/pabustats`
- Main app code: `pabustats/analysis/views.py`, `forms.py`, `models.py`, and templates under `pabustats/analysis/templates/`

It should not be imported or deployed as-is in this repository. The old app bundles hundreds of `.pb` files inside the source tree and maintains its own database models for elections, locations, voting rules, voting-rule modes, and cached simulations. A native implementation in `pabulib-front` should use our current `PBFile` database records and existing file paths instead.

## What Old Pabustats Does

Old Pabustats is broader than a single rule-comparison widget. It acts as a small participatory-budgeting analysis lab.

Core capabilities:

- Select an existing Pabulib election from the app's stored dataset.
- Upload a standalone `.pb` file for ad-hoc analysis.
- Run multiple participatory budgeting rules over the selected election.
- Compare variants of each rule:
  - districtwise vs citywide execution
  - score utilities vs cost utilities
- Choose a base rule and compare other rule variants against it.
- Render a project-level table showing:
  - subunit
  - project name
  - project cost
  - project score/support
  - whether each rule selected the project
- Compute rule-outcome metrics:
  - total selected cost
  - total utility
  - voter point utility
  - voter cost utility
  - voter strength
  - subunit strength
- Render aggregate summaries across stored elections and locations.
- Cache expensive simulations in a database.

## What We Already Have

This repo already has related but narrower functionality:

- Current-file metadata in `PBFile`.
- Current file resolution via database paths instead of bundled source files.
- Existing preview and visualization pages for single files.
- An in-progress `rule_comparison_service` that compares recorded winners with `equalshares` for approval ballots.
- A cache model for per-file rule-comparison data.

That existing rule-comparison work is only one slice of Pabustats. It should not be treated as the full Pabustats replacement.

## Native Product Shape

A proper native Pabustats implementation should be a dedicated analysis tool, not just a duplicate of the preview rule-comparison tab.

Suggested scope:

- `GET /pabustats`
  - landing page explaining the tool
  - searchable picker for current `PBFile` records
  - no upload flow initially
- `GET /pabustats/<file_id>` or `GET /pabustats/file/<filename>`
  - analysis page for one current file
  - show file metadata and supported analysis modes
  - allow the user to select rule variants
- Optional API endpoints:
  - `GET /api/pabustats/files`
  - `POST /api/pabustats/analyze`
  - `GET /api/pabustats/jobs/<job_id>` if computations become long-running

Initial input source:

- Only `PBFile.is_current == True`
- No uploaded files in the first version
- Uploaded custom `.pb` files can be added later as a separate path

## Implementation Requirements

### 1. File Selection

Use current DB records instead of vendored files.

The selector should include:

- filename
- country
- unit/city
- instance/year
- subunit
- vote type
- recorded rule
- number of projects
- number of votes
- whether selected winners are present

The first version can show all current files but should make unsupported files clear before computation.

### 2. Pabulib Parsing Layer

Use the existing parser utilities where possible:

- `app/utils/load_pb_file.py`
- existing preview/visualization parsing patterns

The analysis layer should normalize parsed files into an internal election object with:

- budget
- projects
- costs
- vote records
- voter utilities
- selected winners
- subunit/project assignment

This should be separated from the UI route so it can be tested independently.

### 3. Rule Engine

The old app included copied `pabutools` logic. We should avoid copying a stale rule engine into this repo.

Preferred approach:

- Use `pabutools` as a dependency if it provides the rule implementations we need.
- Add a thin adapter from our parsed `.pb` data to the `pabutools` election model.
- Keep our own service layer for orchestration, caching, and UI payloads.

Rules to support eventually:

- recorded/original winners read from the file
- utilitarian greedy
- Method of Equal Shares
- Method of Equal Shares with completion variants if needed
- Phragmen/sequential Phragmen if available and relevant

The MVP should explicitly list supported rules and refuse unsupported combinations gracefully.

### 4. Rule Variants

Old Pabustats compared rule modes, not only rule names.

Native implementation should model:

- `execution_scope`
  - `districtwise`
  - `citywide`
- `utility_mode`
  - `score`
  - `cost`
- `completion`
  - none
  - add1 or other supported completion modes, if implemented

These variants should be part of the cache key.

### 5. Metrics

The old app computed more than winner overlap.

MVP metrics:

- selected project count
- selected total cost
- budget used and budget left
- approval/support score where applicable
- overlap with recorded winners
- projects added/removed by each rule

Next metrics:

- voter point utility
- voter cost utility
- unique voters reached
- voter strength
- subunit strength
- comparison with a selected base rule
- mean and standard deviation for per-voter metrics

### 6. Caching

Rule computation can be expensive. Results should be cached per file version and analysis configuration.

A future cache model should include:

- `file_id`
- `file_mtime`
- rule name
- rule parameters
- execution scope
- utility mode
- completion mode
- serialized result payload
- computed timestamp

The existing rule-comparison cache may be generalized, or a new `PabustatsAnalysisCache` model can be introduced if the payload becomes broader than pairwise comparisons.

### 7. UI

The UI should not duplicate the preview rule-comparison tab.

Recommended split:

- Preview page:
  - lightweight, single-file convenience comparison
  - good for quick "recorded vs MES" checks
- Pabustats page:
  - deeper analysis workspace
  - multiple rule modes
  - base-rule comparison
  - richer metrics and tables
  - later: cross-file summaries

The first native Pabustats UI should include:

- file picker
- selected file metadata
- rule/mode selector
- base-rule selector
- summary metric cards
- project-level table
- metric tables
- clear unsupported-state messages

### 8. Tests

Add tests around the service layer before expanding UI.

Suggested coverage:

- parser adapter handles representative `.pb` files
- unsupported vote types return safe messages
- missing selected winners return safe messages
- each supported rule returns a valid budget-feasible outcome
- cache invalidates when `file_mtime` changes
- districtwise/citywide modes produce expected output shapes
- utility-mode conversions are deterministic

## Suggested Phases

### Phase 1: Clarify Scope

- Keep the existing preview rule-comparison feature as the lightweight per-file comparison.
- Do not add `/pabustats` until it does more than the preview tab.
- Define the exact rules and metrics needed for MVP.

### Phase 2: Build Analysis Service

- Add `app/services/pabustats_service.py`.
- Add a normalized election adapter.
- Support current DB files only.
- Support one or two rules plus recorded winners.
- Return structured JSON payloads independent of templates.

### Phase 3: Add Native Pabustats UI

- Add `/pabustats` only after the service supports multiple rule/mode selections.
- Use current `PBFile` records for selection.
- Present unsupported files clearly.

### Phase 4: Add Broader Metrics

- Add voter utility metrics.
- Add voter/subunit strength metrics.
- Add base-rule comparison.
- Add cross-election summaries if still needed.

### Phase 5: Optional Upload Flow

- Add uploaded-file analysis only after current-file analysis is stable.
- Treat uploaded files as temporary inputs, not persisted `PBFile` records.
- Apply the same safety and validation standards as the existing upload/checker flow.

## Open Questions

- Which rules are required for the first native version?
- Should Pabustats be researcher-oriented, public-user-oriented, or both?
- Should cross-election summaries be included, or is single-file analysis enough?
- Should the existing preview rule-comparison service be generalized or kept separate?
- Do we want to depend directly on `pabutools`, or keep a small internal implementation for selected rules?
