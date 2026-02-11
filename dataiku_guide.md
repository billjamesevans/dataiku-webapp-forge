# Dataiku App Guide

This guide explains practical rules, requirements, and strategy for building reliable Dataiku web applications, with a focus on **Dataiku Standard WebApps** and the workflow used by this repository.

## 1) Core Rules (Non-Negotiable)

1. Use **Dataiku Standard WebApp** conventions.
2. `index.html` must be **body-only** (no full HTML document wrapper in Dataiku).
3. Keep backend code in `backend.py` and expose JSON endpoints (for example `/rows`).
4. Do not rely on `app.run()` in Dataiku runtime backend code.
5. Prefer simple dependency footprints (usually `pandas` only on backend).
6. Keep generated code deterministic so diffs are clean and easy to review.

## 2) Required Files for a Standard WebApp

Minimum files for Dataiku import:

- `index.html` (body-only)
- `style.css`
- `script.js`
- `backend.py`

Recommended companion files:

- `config.json` (single source of app config)
- `SETUP.md`
- `requirements.txt`
- `expected_schema.json` and `EXPECTED_SCHEMA.md` (for data contracts)

## 3) Backend Requirements and Guardrails

### 3.1 Data Access

- Use `dataiku.Dataset(...).get_dataframe(...)`.
- Read only required columns whenever possible.
- Cap row counts for interactive apps.
- Avoid full-table loads per request.

### 3.2 Request Pattern

- Keep one endpoint like `/rows` returning JSON:
  - `rows`
  - `total`
  - `offset`
  - `limit`
  - `meta`
- Apply transforms server-side:
  - joins
  - filters
  - computed columns
  - sorting
  - pagination

### 3.3 Performance

- Use pagination by default for larger datasets.
- Cache expensive base dataframe builds briefly (TTL cache).
- Avoid heavy recomputation on every UI interaction.
- Restrict chart payload sizes (top N, max points).

### 3.4 Stability

- Avoid unresolved type hints that can fail at runtime.
- Normalize null/blank handling consistently.
- Return explicit backend error messages to the frontend toast/log.

## 4) Frontend Requirements and Strategy

### 4.1 UX

- Keep UI simple and task-first:
  - clear source setup
  - clear transform setup
  - clear export path
- Prefer a light, readable visual style.
- Keep interactions obvious for non-technical users.

### 4.2 Data Rendering

- Render tables from backend JSON, not embedded static HTML.
- Keep row details optional.
- Keep search lightweight and understandable.

### 4.3 Charts (Simple by Design)

Because dependency constraints are tight, use simple SVG-based charts:

- bar (top N categories)
- histogram (numeric bins)
- line (grouped aggregation)
- scatter (bounded points)

Do not require heavy frontend chart libraries for baseline apps.

## 5) Data Modeling Strategy

### 5.1 Start with 1 Dataset

Default strategy:

1. Build the first working app from Dataset A only.
2. Add filters and selected columns.
3. Add pagination and sort.
4. Validate usability.

Only add joins when there is clear business value.

### 5.2 Join Strategy

- Support single join first (A -> B).
- Add chained join (A -> B -> C) only in advanced mode.
- Prefix joined columns to avoid collisions:
  - `b__<col>`
  - `c__<col>`
- Always validate join keys before export.
- Track join quality (match rate, blank keys, duplicate keys).

### 5.3 Filter Strategy

- Use OR-of-AND groups for real-world logic.
- Support exact, contains, regex, numeric, and date operators.
- Keep filter definitions explicit in config.

### 5.4 Computed Columns

Use only practical, stable transforms by default:

- `concat`
- `coalesce`
- `date_format`
- `bucket`

Avoid overly complex expression engines for v1 apps.

## 6) Configuration Strategy

Use `config.json` as primary configuration contract for generated apps.

Config should include:

- app metadata
- dataset names
- transform config (joins/filters/computed/sort/limit)
- UI config (template/chart/pagination)
- selected columns

Backend strategy:

1. Load `config.json` first.
2. Fall back to `app_config.json` when needed.
3. Fall back to embedded config only as final fallback.

This reduces backend churn and simplifies manual edits.

## 7) Security and Governance

- Never commit sensitive CSV data to repo.
- Keep `instance/` local and gitignored.
- Provide a “clean uploads” action to remove stored CSVs and samples.
- Keep example/sample files clearly non-production.

## 8) Testing and Validation Strategy

Before exporting/publishing:

1. Run schema validation.
2. Run analyze preview (sample output).
3. Review join health metrics.
4. Verify selected columns and labels.
5. Verify sort/limit/pagination behavior.
6. Verify chart config for selected template.

Before Dataiku deployment:

1. Confirm dataset names in config.
2. Confirm required columns exist in datasets.
3. Confirm code env has `pandas`.

## 9) Local Preview Strategy (Smoke Testing)

Use a local preview pack for quick checks before Dataiku:

- `preview.html`
- `preview_server.py`
- sample CSVs (`sample_a.csv`, optional `sample_b.csv`, optional `sample_c.csv`)

Goal: catch obvious UI/backend contract issues early, without blocking on Dataiku runtime.

## 10) Recommended Build Workflow for Teams

1. Prepare dataset(s) in Dataiku Flow.
2. Export schema sample CSV(s).
3. Configure app in Forge (Sources -> Transform -> UI).
4. Run Analyze and fix issues.
5. Export bundle.
6. Create Dataiku Standard WebApp and paste files.
7. Validate against real datasets.
8. Promote and document with `EXPECTED_SCHEMA.md`.

## 11) Anti-Patterns to Avoid

- Building everything as a complex multi-join app from day one.
- Full dataset scans on every request.
- Unbounded chart payloads.
- Hidden transform logic not represented in config.
- Mixing unrelated business logic directly in frontend scripts.

## 12) Definition of Done (Simple Dataiku App)

A simple app is done when:

- it loads quickly,
- row data is correct,
- filters and sort behave correctly,
- pagination works,
- chart (if enabled) is understandable,
- config is documented,
- and the app is reproducible from exported files alone.

---

If unsure between a simpler and more flexible option, choose the simpler one first and add complexity only when the use case proves it.
