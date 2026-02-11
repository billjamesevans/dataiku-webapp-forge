import math
from datetime import datetime
from typing import Any, Dict, List, Optional

from .csv_inspect import iter_csv_rows


def _is_blank(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s.lower() in {"nan", "none", "null"}


def _to_float(v: Any) -> Optional[float]:
    if _is_blank(v):
        return None
    try:
        return float(str(v).strip())
    except Exception:
        return None


def _to_dt(v: Any) -> Optional[datetime]:
    if _is_blank(v):
        return None
    s = str(v).strip()
    # Common formats from Dataiku exports: YYYY-MM-DD, ISO, MM/DD/YY, MM/DD/YYYY
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def validate_config(
    columns_a: List[str],
    transform: Dict[str, Any],
    ui: Dict[str, Any],
) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []

    cols = [c for c in (transform.get("columns") or []) if isinstance(c, dict) and c.get("name")]
    all_cols = [str(c.get("name") or "") for c in cols if c.get("name")]
    selected = [c for c in cols if c.get("include")]

    if not columns_a:
        errors.append("Dataset A columns are missing. Upload CSV A in Sources.")

    # Ensure transform columns align with source A columns.
    source_only = [c for c in all_cols if c not in set(columns_a)]
    if source_only:
        warnings.append("Some configured columns are no longer in Dataset A: " + ", ".join(source_only[:10]))

    if not selected:
        warnings.append("No columns selected; the generated webapp will default to all available columns.")

    # Filters
    groups = transform.get("filter_groups")
    if not isinstance(groups, list):
        groups = [{"filters": transform.get("filters") if isinstance(transform.get("filters"), list) else []}]

    for gi, g in enumerate(groups):
        flts = (g or {}).get("filters") if isinstance(g, dict) else []
        if not isinstance(flts, list):
            continue
        for fi, f in enumerate(flts):
            if not isinstance(f, dict):
                continue
            col = str(f.get("column") or "").strip()
            op = str(f.get("op") or "").strip()
            if not col or not op:
                errors.append(f"Filter group {gi+1}, row {fi+1}: missing column or operator.")
            elif col not in all_cols:
                errors.append(f"Filter group {gi+1}, row {fi+1}: column not found: {col}")

    # Computed columns
    computed = transform.get("computed_columns") if isinstance(transform.get("computed_columns"), list) else []
    for i, cc in enumerate(computed):
        if not isinstance(cc, dict):
            continue
        ctype = str(cc.get("type") or "")
        name = str(cc.get("name") or "")
        if not ctype or not name:
            errors.append(f"Computed column {i+1}: missing type or name.")
            continue
        if name in all_cols:
            warnings.append(f"Computed column {i+1}: name collides with an existing column: {name}")
        if ctype in {"concat", "coalesce"}:
            in_cols = cc.get("columns") if isinstance(cc.get("columns"), list) else []
            missing = [c for c in in_cols if c not in all_cols]
            if missing:
                errors.append(f"Computed column {name}: missing input columns: " + ", ".join(missing))
        elif ctype in {"date_format", "bucket"}:
            col = str(cc.get("column") or "")
            if col and col not in all_cols:
                errors.append(f"Computed column {name}: missing input column: " + col)
        else:
            errors.append(f"Computed column {i+1}: unsupported type: {ctype}")

    # Sort
    sort_cfg = transform.get("sort") if isinstance(transform.get("sort"), dict) else {}
    sort_col = str(sort_cfg.get("column") or "").strip()
    sort_dir = str(sort_cfg.get("direction") or "asc").strip().lower()
    if sort_col and sort_col not in all_cols:
        errors.append("Sort column not found: " + sort_col)
    if sort_dir not in {"asc", "desc"}:
        errors.append("Sort direction must be asc or desc.")

    # UI template checks
    tpl = str((ui or {}).get("template") or "table")
    if tpl not in {"table", "sidebar_filters", "master_detail", "chart_table"}:
        warnings.append("Unknown template: " + tpl + " (defaulting to table).")

    return {"errors": errors, "warnings": warnings}


def join_health(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    # Single-dataset mode: no join health metrics.
    return {"steps": []}


def _apply_filters_to_row(row: Dict[str, Any], flt: Dict[str, Any]) -> bool:
    col = str(flt.get("column") or "").strip()
    op = str(flt.get("op") or "").strip().lower()
    val = str(flt.get("value") or "")
    v = row.get(col)

    if op == "blank":
        return _is_blank(v)
    if op == "notblank":
        return not _is_blank(v)

    if op in {"contains", "contains_cs", "startswith", "startswith_cs", "endswith", "endswith_cs"}:
        s = "" if _is_blank(v) else str(v)
        q = str(val)
        if op == "contains_cs":
            return q in s
        if op == "contains":
            return q.lower() in s.lower()
        if op == "startswith_cs":
            return s.startswith(q)
        if op == "startswith":
            return s.lower().startswith(q.lower())
        if op == "endswith_cs":
            return s.endswith(q)
        return s.lower().endswith(q.lower())

    if op == "regex":
        try:
            import re

            s = "" if _is_blank(v) else str(v)
            return re.search(str(val or ""), s) is not None
        except Exception:
            return False

    if op in {"eq", "neq"}:
        s = "" if _is_blank(v) else str(v)
        ok = s == str(val)
        return ok if op == "eq" else (not ok)

    if op in {"in", "notin"}:
        items = [x.strip() for x in str(val).split(",") if x.strip()]
        s = "" if _is_blank(v) else str(v)
        ok = s in items
        return ok if op == "in" else (not ok)

    if op in {"gt", "gte", "lt", "lte"}:
        f = _to_float(v)
        q = _to_float(val)
        if f is None or q is None:
            return False
        if op == "gt":
            return f > q
        if op == "gte":
            return f >= q
        if op == "lt":
            return f < q
        return f <= q

    if op in {"date_gt", "date_gte", "date_lt", "date_lte"}:
        d = _to_dt(v)
        qd = _to_dt(val)
        if d is None or qd is None:
            return False
        if op == "date_gt":
            return d > qd
        if op == "date_gte":
            return d >= qd
        if op == "date_lt":
            return d < qd
        return d <= qd

    return True


def sample_output(
    path_a: str,
    transform: Dict[str, Any],
    *,
    max_rows_a: int = 2000,
    out_rows: int = 30,
) -> Dict[str, Any]:
    rows = list(iter_csv_rows(path_a, max_rows=max_rows_a)) if path_a else []

    # Apply OR-of-AND filter groups.
    groups = transform.get("filter_groups")
    if not isinstance(groups, list):
        legacy = transform.get("filters") if isinstance(transform.get("filters"), list) else []
        groups = [{"filters": legacy}]

    if groups and any((g or {}).get("filters") for g in groups if isinstance(g, dict)):
        kept: List[Dict[str, Any]] = []
        for r in rows:
            ok_any = False
            for g in groups:
                flts = (g or {}).get("filters") if isinstance(g, dict) else []
                if not isinstance(flts, list) or not flts:
                    continue
                ok_all = True
                for f in flts:
                    if isinstance(f, dict) and not _apply_filters_to_row(r, f):
                        ok_all = False
                        break
                if ok_all:
                    ok_any = True
                    break
            if ok_any:
                kept.append(r)
        rows = kept

    # Apply computed columns
    computed = transform.get("computed_columns") if isinstance(transform.get("computed_columns"), list) else []
    for r in rows:
        for cc in computed:
            if not isinstance(cc, dict):
                continue
            name = str(cc.get("name") or "").strip()
            ctype = str(cc.get("type") or "").strip()
            if not name or not ctype:
                continue
            if ctype == "concat":
                in_cols = cc.get("columns") if isinstance(cc.get("columns"), list) else []
                sep = str(cc.get("sep") or "")
                r[name] = sep.join([str(r.get(c, "") or "") for c in in_cols])
            elif ctype == "coalesce":
                in_cols = cc.get("columns") if isinstance(cc.get("columns"), list) else []
                out = ""
                for c in in_cols:
                    v = r.get(c)
                    if not _is_blank(v):
                        out = str(v)
                        break
                r[name] = out
            elif ctype == "date_format":
                col = str(cc.get("column") or "").strip()
                fmt = str(cc.get("format") or "%Y-%m-%d")
                dt = _to_dt(r.get(col))
                r[name] = dt.strftime(fmt) if dt else ""
            elif ctype == "bucket":
                col = str(cc.get("column") or "").strip()
                size = int(cc.get("size") or 10)
                f = _to_float(r.get(col))
                if f is None or size <= 0:
                    r[name] = ""
                else:
                    base = math.floor(f / float(size)) * size
                    r[name] = str(int(base))

    # Sort preview.
    sort_cfg = transform.get("sort") if isinstance(transform.get("sort"), dict) else {}
    sort_col = str(sort_cfg.get("column") or "").strip()
    sort_dir = str(sort_cfg.get("direction") or "asc").strip().lower()
    if sort_col:
        rows = sorted(rows, key=lambda r: str(r.get(sort_col, "") or ""), reverse=(sort_dir == "desc"))

    # Respect row limit in preview too.
    limit = int(transform.get("limit") or max_rows_a)
    limit = max(1, min(limit, 200000))
    rows = rows[:limit]

    # Select columns based on include and output_order.
    cols = [c for c in (transform.get("columns") or []) if isinstance(c, dict) and c.get("name")]
    for cc in computed:
        if isinstance(cc, dict) and cc.get("name"):
            cols.append({"name": str(cc.get("name")), "label": str(cc.get("label") or cc.get("name")), "include": bool(cc.get("include"))})

    col_by_name = {str(c["name"]): c for c in cols if c.get("name")}
    order = transform.get("output_order") if isinstance(transform.get("output_order"), list) else []
    selected = [str(c["name"]) for c in cols if c.get("include")]

    if order:
        selected = [n for n in order if n in col_by_name and col_by_name[n].get("include")] + [n for n in selected if n not in set(order)]

    if selected:
        out_rows_list: List[Dict[str, Any]] = []
        for r in rows[:out_rows]:
            out_rows_list.append({n: r.get(n, "") for n in selected})
        return {
            "rows": out_rows_list,
            "columns": [{"name": n, "label": (col_by_name[n].get("label") or n)} for n in selected],
        }

    # Fall back to whatever keys exist.
    keys = list(rows[0].keys()) if rows else []
    return {"rows": rows[:out_rows], "columns": [{"name": n, "label": n} for n in keys]}
