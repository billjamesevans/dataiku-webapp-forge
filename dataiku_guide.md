# Dataiku Guide for AI Agents

This guide is a **general operating playbook** for AI agents building Dataiku applications.
It is not tied to any one repository, template, or generator.

## 1) Purpose and Scope

Use this guide when an AI agent is asked to build or modify:

- Data pipelines in a Dataiku project (datasets + recipes + flow)
- Dataiku Standard WebApps
- Dataiku dashboards and insights
- scenario-driven automation in Dataiku
- deployable API/services backed by Dataiku datasets

Primary goals:

1. Build correct solutions quickly.
2. Keep solutions maintainable by humans.
3. Avoid brittle architectures and avoidable runtime failures.
4. Keep security, governance, and reproducibility first-class.

## 2) Agent Operating Principles

1. Start simple and evolve only when needed.
2. Prefer Dataiku-native capabilities before custom code.
3. Treat dataset schemas and contracts as explicit design inputs.
4. Make every major behavior config-driven and reviewable.
5. Optimize for explainability: someone else should understand the app fast.
6. Avoid hidden state and environment-specific assumptions.

## 3) Choose the Right Dataiku Artifact

Before writing code, choose the right implementation type.

### 3.1 When to use Flow recipes

Use Flow recipes when logic is batch-style transformation:

- joins
- aggregations
- cleansing
- enrichment
- row-level calculations

Rule: if a transformation can be materialized upstream once, do it in Flow instead of per request in app code.

### 3.2 When to use a Standard WebApp

Use Standard WebApp when you need interactive UI behavior:

- user-driven filtering/slicing
- drill-down exploration
- lightweight operational tooling
- guided decisions/forms

### 3.3 When to use dashboards/insights

Use dashboards if users only need static/refreshable visual monitoring and no custom workflow logic.

### 3.4 When to use a plugin/app package

Use plugin packaging when logic must be reused across multiple projects/teams with shared governance.

## 4) Requirements Discovery (Do This First)

An AI agent should lock these down before implementation:

- Business objective (what decision/action changes?)
- User persona(s)
- Data sources and refresh cadence
- SLA targets (latency, freshness, uptime)
- Security constraints (PII/PHI, row-level access, audit)
- Deployment target (design node / automation / API node)

If these are unknown, state assumptions explicitly in the deliverable.

## 5) Data Contract Strategy

Define a data contract for each input dataset:

- dataset name
- required columns
- expected types
- key semantics (primary keys, join keys)
- nullability expectations
- known quality checks

For webapps and APIs, include:

- output schema
- pagination model
- sort/filter behavior
- error response shape

Rule: if schema assumptions are implicit, runtime defects are likely.

## 6) Architecture Strategy

### 6.1 Pushdown principle

Do heavy compute upstream in Flow whenever possible.

Keep app/backend request-time logic limited to:

- filtering
- sorting
- pagination
- display shaping
- lightweight derived fields

### 6.2 Single-responsibility layers

- Flow layer: durable transformations
- backend layer: query/read + controlled business logic
- frontend layer: presentation and user interaction

Do not mix all logic in frontend JavaScript.

### 6.3 Config-first behavior

Store critical behavior in explicit config:

- dataset references
- join setup
- filters
- selected columns/labels
- chart config
- limits/timeouts

Code should load config and behave deterministically.

## 7) Standard WebApp Rules

For Dataiku Standard WebApps specifically:

1. `index.html` should be body content (Dataiku embeds shell/head).
2. Backend routes should return JSON contracts consistently.
3. Avoid assumptions about local Flask server behavior in Dataiku runtime.
4. Keep dependencies minimal in code env.
5. Implement graceful error messaging to UI.

Recommended response shape for row endpoints:

- `status`
- `rows`
- `total`
- `offset`
- `limit`
- `meta`

## 8) Performance Rules

1. Never full-scan large datasets on every request unless unavoidable.
2. Read only needed columns.
3. Apply sensible limits and pagination.
4. Bound chart payload sizes (top N / max points).
5. Cache expensive read/merge steps when safe.
6. Separate user-facing latency from batch prep work.

If timeouts occur, first reduce request-time workload before adding complexity.

## 9) Join and Filter Strategy

### 9.1 Joins

- Prefer one join layer first.
- For multi-join chains, make step order explicit.
- Validate join keys (existence + quality).
- Track match rate, blank-key rate, duplicate-key rate when feasible.
- Use deterministic column naming to avoid collisions.

### 9.2 Filters

- Support composable logic (AND/OR groups) where required.
- Validate operators and types early.
- Keep filter semantics explicit and documented.

## 10) Visualization Strategy (Low-Dependency)

Default chart strategy for agent-built apps:

- simple bar charts
- histograms
- line charts
- scatter plots (bounded points)

Prefer built-in/simple rendering paths unless advanced visualization is a hard requirement.

Rule: avoid heavy visualization libraries unless justified by clear user need.

## 11) Security and Governance Rules

1. Never commit raw sensitive data to source control.
2. Keep local/project state out of git unless explicitly required.
3. Minimize PII exposure in logs, previews, and error traces.
4. Follow least privilege for external connections and secrets.
5. Add clear ownership and maintenance notes.

## 12) Validation and Testing Checklist

Before handoff, AI agent should verify:

- schema validation passes
- core user flows work end-to-end
- backend error paths are understandable
- pagination/sort/filter logic is correct
- joins return expected row behavior
- UI remains usable on realistic data volume
- exported/deployed files are complete and reproducible

If any validation is skipped, document exactly what was not tested.

## 13) Deployment and Promotion Strategy

1. Develop and validate in a controlled project branch/environment.
2. Promote with explicit change summary.
3. Validate against production-like data shape.
4. Monitor early usage and error rates.
5. Keep rollback path simple.

## 14) Documentation Requirements for Agent Deliverables

Every delivered Dataiku app should include:

- purpose and scope
- required datasets + columns
- configuration keys
- runtime dependencies
- operational limits (row limits, timeout assumptions)
- troubleshooting notes

If the app is interactive, document endpoint contracts and UI state assumptions.

## 15) Anti-Patterns (Avoid)

- Building request-time mega-transformations instead of Flow prep
- Unbounded queries in interactive endpoints
- Hidden business logic only in frontend code
- Undocumented schema dependencies
- Tight coupling to one local environment
- "Magic" behavior with no config trail

## 16) Definition of Done

A Dataiku app is done when:

- requirements are met,
- behavior is documented,
- data contracts are explicit,
- runtime is performant enough for target users,
- deployment is reproducible,
- and support handoff is realistic.

## 17) Quick Decision Heuristic for AI Agents

If uncertain:

1. Choose the simplest artifact that can solve the problem.
2. Move heavy transforms upstream to Flow.
3. Keep runtime endpoints thin and bounded.
4. Ship with clear contracts and docs.
5. Add complexity only when evidence requires it.

---

This guide is intended to be reused across repositories and teams as a general baseline for AI-driven Dataiku app delivery.
