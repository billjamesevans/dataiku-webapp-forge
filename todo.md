# WebApp Forge TODO (10x Improvements)

## Core UX
- [x] Add an in-app “Sample Output” view that shows the first N rows after join/filter/column selection.
- [x] Add schema validation with actionable errors.
- [x] Add a join “health report”: match rate, blank-key rate, duplicate-key rate.
- [x] Support multi-column joins (composite keys).
- [x] Support multiple joins (A -> B -> C) as an advanced mode.

## Transform Power
- [x] Expand filters: OR/AND groups, date comparisons, in/notin, regex contains, case-sensitivity variants.
- [x] Add “computed columns” (simple expressions): concat, coalesce, date formatting, numeric bucketing.
- [x] Add column reorder (drag), quick include/exclude all, column search, and auto-label.
- [x] Add a pagination-ready mode: backend supports offset/limit and returns totals.

## Templates & Reuse
- [x] Add webapp templates: Simple Table, Table+Sidebar Filters, Master-Detail, Two tables, Chart+Table.
- [x] Add presets: save/apply transform presets across projects.
- [x] Make export diff-friendly by default (deterministic JSON output).

## Project Management
- [x] Add project rename, tags, search, pin favorites, sort by last updated, and “clean uploads” action.
- [x] Add duplicate at the “template/preset” level (not just whole-project).

## Dataiku Experience
- [x] Add a “Dataiku runtime preview pack”: small local HTML runner that mimics Dataiku environment to smoke test frontend+backend.
- [x] Generate a concise config block (datasets/join/filters/columns) and make `backend.py` load from it to reduce regeneration churn.
- [x] Add “expected dataset schema” export for Dataiku docs (columns, types guessed, join keys, filter columns).
