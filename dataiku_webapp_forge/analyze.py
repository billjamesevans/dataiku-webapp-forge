import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

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


def _key_tuple(row: Dict[str, str], keys: List[Tuple[str, str]], *, side: str) -> Tuple[str, ...]:
    out: List[str] = []
    for a, b in keys:
        col = a if side == "a" else b
        out.append(str(row.get(col, "") or "").strip())
    return tuple(out)


def _normalize_joins(transform: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Returns canonical join steps: [{"right":"b"|"c","enabled":bool,"how":"left|inner","keys":[{"left":..,"right":..}]}]
    If transform.joins is missing, falls back to legacy transform.join_enabled/join.
    """
    joins = transform.get("joins") if isinstance(transform.get("joins"), list) else []
    out: List[Dict[str, Any]] = []
    if joins:
        for s in joins:
            if not isinstance(s, dict):
                continue
            right = str(s.get("right") or "").strip().lower()
            if right not in {"b", "c"}:
                continue
            keys_raw = s.get("keys") if isinstance(s.get("keys"), list) else []
            keys: List[Dict[str, str]] = []
            for k in keys_raw:
                if not isinstance(k, dict):
                    continue
                left = str(k.get("left") or "").strip()
                rk = str(k.get("right") or "").strip()
                if left or rk:
                    keys.append({"left": left, "right": rk})
            if not keys:
                keys = [{"left": "", "right": ""}]
            out.append(
                {
                    "right": right,
                    "enabled": bool(s.get("enabled")),
                    "how": str(s.get("how") or "left").strip().lower(),
                    "keys": keys,
                }
            )
    if out:
        # Ensure b then c order if both exist.
        by = {s["right"]: s for s in out if isinstance(s, dict) and s.get("right")}
        res = []
        if "b" in by:
            res.append(by["b"])
        if "c" in by:
            res.append(by["c"])
        return res

    # Legacy fallback: join_enabled + join keys (a/b).
    join_enabled = bool(transform.get("join_enabled"))
    join = transform.get("join") or {}
    how = str(join.get("how") or "left").strip().lower()
    keys = join.get("keys") if isinstance(join.get("keys"), list) else []
    kp: List[Dict[str, str]] = []
    for k in keys:
        if not isinstance(k, dict):
            continue
        a = str(k.get("a") or "").strip()
        b = str(k.get("b") or "").strip()
        if a or b:
            kp.append({"left": a, "right": b})
    if not kp:
        kp = [{"left": "", "right": ""}]
    return [{"right": "b", "enabled": join_enabled, "how": how, "keys": kp}]


def validate_config(
    columns_a: List[str],
    columns_b: List[str],
    columns_c: List[str],
    transform: Dict[str, Any],
    ui: Dict[str, Any],
) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []

    joins = _normalize_joins(transform)
    # Validate joins in order so later left-key validation can include earlier prefixes.
    left_cols = list(columns_a)
    for step in joins:
        if not isinstance(step, dict):
            continue
        right = str(step.get("right") or "")
        enabled = bool(step.get("enabled"))
        how = str(step.get("how") or "left").lower()
        keys = step.get("keys") if isinstance(step.get("keys"), list) else []
        pairs = [(str(k.get("left") or "").strip(), str(k.get("right") or "").strip()) for k in keys if isinstance(k, dict)]
        pairs = [(a, b) for a, b in pairs if a or b]

        right_cols = columns_b if right == "b" else columns_c
        if enabled:
            if right == "b" and not columns_b:
                errors.append("Join to Dataset B is enabled but Dataset B is missing. Upload CSV B in Sources.")
            if right == "c" and not columns_c:
                errors.append("Join to Dataset C is enabled but Dataset C is missing. Upload CSV C in Sources.")
            if how not in {"left", "inner"}:
                errors.append(f"Join to Dataset {right.upper()} type must be left or inner.")
            if not pairs:
                errors.append(f"Join to Dataset {right.upper()} is enabled but no join keys are configured.")
            for a, b in pairs:
                if not a or a not in left_cols:
                    errors.append(f"Join key missing in left dataframe for Dataset {right.upper()}: " + (a or "(blank)"))
                if not b or b not in right_cols:
                    errors.append(f"Join key missing in Dataset {right.upper()}: " + (b or "(blank)"))

        # If this step is enabled and right dataset exists, subsequent joins can reference prefixed columns.
        if enabled and right == "b" and columns_b:
            left_cols = left_cols + ["b__" + str(c) for c in columns_b]
        if enabled and right == "c" and columns_c:
            left_cols = left_cols + ["c__" + str(c) for c in columns_c]

    # Output columns
    cols = [c for c in (transform.get("columns") or []) if isinstance(c, dict) and c.get("name")]
    selected = [c for c in cols if c.get("include")]
    if not selected:
        warnings.append("No columns selected; the generated webapp will default to all available columns.")
    join_b_enabled = any(s.get("right") == "b" and s.get("enabled") for s in joins)
    join_c_enabled = any(s.get("right") == "c" and s.get("enabled") for s in joins)
    if not join_b_enabled:
        bad = [c.get("name") for c in selected if str(c.get("name") or "").startswith("b__")]
        if bad:
            warnings.append("Dataset B columns are selected but join to Dataset B is disabled; they will be ignored.")
    if not join_c_enabled:
        bad = [c.get("name") for c in selected if str(c.get("name") or "").startswith("c__")]
        if bad:
            warnings.append("Dataset C columns are selected but join to Dataset C is disabled; they will be ignored.")

    # Filters
    groups = transform.get("filter_groups")
    if not isinstance(groups, list):
        groups = [{"filters": transform.get("filters") if isinstance(transform.get("filters"), list) else []}]
    all_cols = [c.get("name") for c in cols if c.get("name")]
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
            cols = cc.get("columns") if isinstance(cc.get("columns"), list) else []
            missing = [c for c in cols if c not in all_cols]
            if missing:
                errors.append(f"Computed column {name}: missing input columns: " + ", ".join(missing))
        elif ctype in {"date_format", "bucket"}:
            col = str(cc.get("column") or "")
            if col and col not in all_cols:
                errors.append(f"Computed column {name}: missing input column: " + col)
        else:
            errors.append(f"Computed column {i+1}: unsupported type: {ctype}")

    # UI template checks
    tpl = str((ui or {}).get("template") or "table")
    if tpl not in {"table", "sidebar_filters", "master_detail", "chart_table", "two_tables"}:
        warnings.append("Unknown template: " + tpl + " (defaulting to table).")

    return {"errors": errors, "warnings": warnings}


def _key_tuple_lr(row: Dict[str, str], keys: List[Tuple[str, str]], *, side: str) -> Tuple[str, ...]:
    out: List[str] = []
    for left, right in keys:
        col = left if side == "left" else right
        out.append(str(row.get(col, "") or "").strip())
    return tuple(out)


def join_health(
    path_a: str,
    path_b: Optional[str],
    path_c: Optional[str],
    *,
    joins: List[Dict[str, Any]],
    max_rows: int = 5000,
) -> Dict[str, Any]:
    """
    Join health report for sequential joins (A->B->C). Returns {"steps":[...]}.
    """
    if not path_a or not joins:
        return {"steps": []}

    def key_blank(t: Tuple[str, ...]) -> bool:
        return all(_is_blank(x) for x in t)

    def dup_rate(keys_list: List[Tuple[str, ...]]) -> float:
        if not keys_list:
            return 0.0
        return 1.0 - (len(set(keys_list)) / float(len(keys_list)))

    rows_left = list(iter_csv_rows(path_a, max_rows=max_rows))
    steps: List[Dict[str, Any]] = []

    for step in joins:
        if not isinstance(step, dict):
            continue
        right = str(step.get("right") or "").strip().lower()
        enabled = bool(step.get("enabled"))
        if not enabled:
            continue

        right_path = path_b if right == "b" else path_c
        if not right_path:
            continue

        how = str(step.get("how") or "left").strip().lower()
        keys_raw = step.get("keys") if isinstance(step.get("keys"), list) else []
        pairs = [(str(k.get("left") or "").strip(), str(k.get("right") or "").strip()) for k in keys_raw if isinstance(k, dict)]
        pairs = [(a, b) for a, b in pairs if a and b]
        if not pairs:
            continue

        rows_right = list(iter_csv_rows(right_path, max_rows=max_rows))
        left_keys = [_key_tuple_lr(r, pairs, side="left") for r in rows_left]
        right_keys = [_key_tuple_lr(r, pairs, side="right") for r in rows_right]
        left_nonblank = [k for k in left_keys if not key_blank(k)]
        right_nonblank = [k for k in right_keys if not key_blank(k)]

        right_set = set(right_nonblank)
        matched = sum(1 for k in left_nonblank if k in right_set)
        total = len(left_nonblank)
        match_rate = (matched / total) if total else None

        steps.append(
            {
                "right": right,
                "left_rows_sampled": len(rows_left),
                "right_rows_sampled": len(rows_right),
                "left_key_blank_rate": (1.0 - (len(left_nonblank) / float(len(left_keys)))) if left_keys else None,
                "right_key_blank_rate": (1.0 - (len(right_nonblank) / float(len(right_keys)))) if right_keys else None,
                "left_key_dup_rate": dup_rate(left_nonblank),
                "right_key_dup_rate": dup_rate(right_nonblank),
                "match_rate": match_rate,
                "matched_left_keys": matched,
                "left_keys_with_value": total,
            }
        )

        # Carry forward a lightweight merge for subsequent steps (first-match semantics).
        prefix = right + "__"
        index: Dict[Tuple[str, ...], Dict[str, str]] = {}
        for rr in rows_right:
            kt = _key_tuple_lr(rr, pairs, side="right")
            if kt not in index:
                index[kt] = rr

        merged: List[Dict[str, Any]] = []
        for rl in rows_left:
            out = dict(rl)
            kt = _key_tuple_lr(rl, pairs, side="left")
            rr = index.get(kt)
            if rr:
                for k, v in rr.items():
                    out[prefix + k] = v
            elif how == "inner":
                continue
            merged.append(out)
        rows_left = merged

    return {"steps": steps}


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
        # Minimal regex support for preview only.
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
    path_b: Optional[str],
    path_c: Optional[str] = None,
    *,
    transform: Dict[str, Any],
    max_rows_a: int = 2000,
    max_rows_b: int = 2000,
    max_rows_c: int = 2000,
    out_rows: int = 30,
) -> Dict[str, Any]:
    """
    Lightweight preview based on CSV data (not Dataiku datasets).
    We only need a "shape" preview for users to confirm mapping and transforms.
    """
    rows_a = list(iter_csv_rows(path_a, max_rows=max_rows_a)) if path_a else []
    rows_b = list(iter_csv_rows(path_b, max_rows=max_rows_b)) if path_b else []
    rows_c = list(iter_csv_rows(path_c, max_rows=max_rows_c)) if path_c else []

    joins = _normalize_joins(transform)

    # Start with A rows then sequentially join to B and C (first-match semantics).
    merged: List[Dict[str, Any]] = [dict(r) for r in rows_a]

    for step in joins:
        if not isinstance(step, dict) or not step.get("enabled"):
            continue
        right = str(step.get("right") or "").strip().lower()
        how = str(step.get("how") or "left").strip().lower()
        keys_raw = step.get("keys") if isinstance(step.get("keys"), list) else []
        pairs = [(str(k.get("left") or "").strip(), str(k.get("right") or "").strip()) for k in keys_raw if isinstance(k, dict)]
        pairs = [(a, b) for a, b in pairs if a and b]
        if not pairs:
            continue

        right_rows = rows_b if right == "b" else rows_c
        if not right_rows:
            continue

        idx: Dict[Tuple[str, ...], Dict[str, str]] = {}
        for rr in right_rows:
            kt = _key_tuple_lr(rr, pairs, side="right")
            if kt not in idx:
                idx[kt] = rr

        prefix = right + "__"
        new_merged: List[Dict[str, Any]] = []
        for rl in merged:
            out = dict(rl)
            kt = _key_tuple_lr(rl, pairs, side="left")
            rr = idx.get(kt)
            if rr:
                for k, v in rr.items():
                    out[prefix + k] = v
            elif how == "inner":
                continue
            new_merged.append(out)
        merged = new_merged

    # Apply OR-of-AND filter groups.
    groups = transform.get("filter_groups")
    if not isinstance(groups, list):
        legacy = transform.get("filters") if isinstance(transform.get("filters"), list) else []
        groups = [{"filters": legacy}]

    if groups and any((g or {}).get("filters") for g in groups if isinstance(g, dict)):
        kept: List[Dict[str, Any]] = []
        for r in merged:
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
        merged = kept

    # Apply computed columns
    computed = transform.get("computed_columns") if isinstance(transform.get("computed_columns"), list) else []
    for r in merged:
        for cc in computed:
            if not isinstance(cc, dict):
                continue
            name = str(cc.get("name") or "").strip()
            ctype = str(cc.get("type") or "").strip()
            if not name or not ctype:
                continue
            if ctype == "concat":
                cols = cc.get("columns") if isinstance(cc.get("columns"), list) else []
                sep = str(cc.get("sep") or "")
                r[name] = sep.join([str(r.get(c, "") or "") for c in cols])
            elif ctype == "coalesce":
                cols = cc.get("columns") if isinstance(cc.get("columns"), list) else []
                out = ""
                for c in cols:
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

    # Select columns based on include and output_order.
    cols = [c for c in (transform.get("columns") or []) if isinstance(c, dict) and c.get("name")]
    # Make computed columns selectable in preview output.
    for cc in computed:
        if isinstance(cc, dict) and cc.get("name"):
            cols.append({"name": str(cc.get("name")), "label": str(cc.get("label") or cc.get("name")), "include": bool(cc.get("include"))})
    col_by_name = {c["name"]: c for c in cols}
    order = transform.get("output_order") if isinstance(transform.get("output_order"), list) else []
    selected = [c["name"] for c in cols if c.get("include")]
    if order:
        selected = [n for n in order if n in col_by_name and col_by_name[n].get("include")] + [
            n for n in selected if n not in set(order)
        ]

    if selected:
        out_rows_list: List[Dict[str, Any]] = []
        for r in merged[:out_rows]:
            out_rows_list.append({n: r.get(n, "") for n in selected})
        return {"rows": out_rows_list, "columns": [{"name": n, "label": (col_by_name[n].get("label") or n)} for n in selected]}

    # Fall back to whatever keys exist.
    keys = list(merged[0].keys()) if merged else []
    return {"rows": merged[:out_rows], "columns": [{"name": n, "label": n} for n in keys]}
