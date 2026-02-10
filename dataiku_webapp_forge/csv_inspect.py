import csv
import io
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Iterable


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").strip().lower())


def suggest_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    lookup = {_normalize_key(c): c for c in columns}
    for cand in candidates:
        key = _normalize_key(cand)
        if key in lookup:
            return lookup[key]
    return None


def sniff_dialect(path: str) -> csv.Dialect:
    # Dataiku CSV exports are usually comma-delimited, but sniff in case.
    with open(path, "rb") as f:
        raw = f.read(64 * 1024)
    text = raw.decode("utf-8", errors="replace")
    try:
        return csv.Sniffer().sniff(text)
    except Exception:
        return csv.get_dialect("excel")


@dataclass(frozen=True)
class CsvInfo:
    path: str
    filename: str
    columns: List[str]
    sample_rows: List[Dict[str, str]]


def inspect_csv(path: str, *, max_rows: int = 25) -> CsvInfo:
    dialect = sniff_dialect(path)
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, dialect=dialect)
        columns = list(reader.fieldnames or [])
        sample_rows: List[Dict[str, str]] = []
        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            # Normalize None -> "" for display
            sample_rows.append({k: (v if v is not None else "") for k, v in (row or {}).items()})
    return CsvInfo(
        path=os.path.abspath(path),
        filename=os.path.basename(path),
        columns=columns,
        sample_rows=sample_rows,
    )


def iter_csv_rows(path: str, *, max_rows: int = 5000) -> Iterable[Dict[str, str]]:
    """Stream rows as dicts. Values are strings ("" for null)."""
    dialect = sniff_dialect(path)
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f, dialect=dialect)
        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            yield {k: (v if v is not None else "") for k, v in (row or {}).items()}


def sample_value(info: CsvInfo, column: str) -> str:
    if not info.sample_rows:
        return ""
    return str(info.sample_rows[0].get(column, "") or "")

def suggest_join_keys(columns_a: List[str], columns_b: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Best-effort guess for a join key between two CSV schemas.
    Returns (left_col_name, right_col_name) or (None, None) if no obvious match.
    """
    if not columns_a or not columns_b:
        return None, None

    a_norm = {_normalize_key(c): c for c in columns_a}
    b_norm = {_normalize_key(c): c for c in columns_b}

    preferred = [
        "id",
        "itemid",
        "item_id",
        "inv_item_id",
        "ib_item_id",
        "key",
        "sku",
        "code",
    ]
    for k in preferred:
        nk = _normalize_key(k)
        if nk in a_norm and nk in b_norm:
            return a_norm[nk], b_norm[nk]

    # Fallback: any common normalized column name, keeping A's order.
    for c in columns_a:
        nk = _normalize_key(c)
        if nk and nk in b_norm:
            return c, b_norm[nk]

    return None, None
