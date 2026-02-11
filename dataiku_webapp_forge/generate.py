import io
import json
import os
import re
import zipfile
import pprint
from datetime import datetime
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from jinja2 import Environment, FileSystemLoader, select_autoescape


def _slug(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return value or "webapp"


@dataclass(frozen=True)
class GeneratedApp:
    slug: str
    title: str
    files: Dict[str, str]  # path -> content


def _env(templates_dir: str) -> Environment:
    env = Environment(
        loader=FileSystemLoader(templates_dir),
        autoescape=select_autoescape(enabled_extensions=("html", "xml")),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    # Keep templates simple: allow emitting JSON and Python literals explicitly.
    def _tojson(obj: Any, indent: Optional[int] = None) -> str:
        # Deterministic JSON output helps diffs and reduces noisy regen churn.
        return json.dumps(obj, indent=indent, ensure_ascii=True, sort_keys=True)

    def _py(obj: Any) -> str:
        return pprint.pformat(obj, width=120, sort_dicts=True)

    env.filters["tojson"] = _tojson
    env.filters["py"] = _py
    return env


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
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _guess_type(values: List[Any]) -> str:
    vals = [v for v in values if not _is_blank(v)]
    if not vals:
        return "unknown"
    # Sample a bounded number
    vals = vals[:50]
    total = len(vals)
    bool_set = {"true", "false", "yes", "no", "y", "n", "0", "1"}
    bool_ok = sum(1 for v in vals if str(v).strip().lower() in bool_set)
    dt_ok = sum(1 for v in vals if _to_dt(v) is not None)
    num_ok = sum(1 for v in vals if _to_float(v) is not None)
    if dt_ok / total >= 0.80:
        return "datetime"
    if num_ok / total >= 0.90:
        # If it's purely 0/1, consider boolean.
        if bool_ok / total >= 0.95:
            return "boolean"
        return "number"
    if bool_ok / total >= 0.95:
        return "boolean"
    return "string"


def _expected_schema(project: Dict[str, Any], columns_selected: List[Dict[str, Any]]) -> Dict[str, Any]:
    csv = project.get("csv") if isinstance(project.get("csv"), dict) else {}
    dku = project.get("dataiku") if isinstance(project.get("dataiku"), dict) else {}
    transform = project.get("transform") if isinstance(project.get("transform"), dict) else {}
    ui = project.get("ui") if isinstance(project.get("ui"), dict) else {}

    def dataset_schema(key: str) -> Dict[str, Any]:
        info = csv.get(key) if isinstance(csv.get(key), dict) else {}
        cols = info.get("columns") if isinstance(info.get("columns"), list) else []
        sample_rows = info.get("sample_rows") if isinstance(info.get("sample_rows"), list) else []
        col_types = []
        for c in cols:
            vals = []
            for r in sample_rows[:25]:
                if isinstance(r, dict):
                    vals.append(r.get(c))
            col_types.append({"name": str(c), "type_guess": _guess_type(vals)})
        return {
            "dataset_name": str(dku.get(f"dataset_{key}") or ""),
            "columns": col_types,
        }

    out: Dict[str, Any] = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "datasets": {
            "a": dataset_schema("a"),
        },
        "required": {
            "output_columns": [str(c.get("name")) for c in columns_selected if isinstance(c, dict) and c.get("name")],
            "filter_columns": [],
            "computed_inputs": [],
        },
        "notes": [
            "Types are best-effort guesses from CSV sample rows captured in the Forge (may be empty if uploads were cleaned).",
            "Single-dataset mode: this generated backend reads only Dataset A.",
        ],
    }

    # Filters
    filter_cols: List[str] = []
    for g in transform.get("filter_groups") or []:
        if not isinstance(g, dict):
            continue
        for f in g.get("filters") or []:
            if isinstance(f, dict) and f.get("column"):
                filter_cols.append(str(f.get("column")))
    out["required"]["filter_columns"] = sorted(set(filter_cols))

    # Computed columns inputs
    comp_inputs: List[str] = []
    computed = transform.get("computed_columns") if isinstance(transform.get("computed_columns"), list) else []
    for cc in computed:
        if not isinstance(cc, dict):
            continue
        ctype = str(cc.get("type") or "")
        if ctype in {"concat", "coalesce"}:
            for c in cc.get("columns") or []:
                if c:
                    comp_inputs.append(str(c))
        elif ctype in {"date_format", "bucket"}:
            if cc.get("column"):
                comp_inputs.append(str(cc.get("column")))
    out["required"]["computed_inputs"] = sorted(set(comp_inputs))

    # UI extras (chart/filter)
    out["ui"] = {
        "template": str(ui.get("template") or "table"),
        "frontend_filters": ui.get("frontend_filters") if isinstance(ui.get("frontend_filters"), list) else [],
        "chart": ui.get("chart") if isinstance(ui.get("chart"), dict) else {},
    }

    return out


def build_apps(project: Dict[str, Any], *, templates_dir: str) -> List[GeneratedApp]:
    env = _env(templates_dir)

    base_name = (project.get("app") or {}).get("name") or "Simple Dataiku WebApp"
    subtitle = (project.get("app") or {}).get("subtitle") or ""
    dataiku_cfg = project.get("dataiku") or {}
    transform = project.get("transform") or {}
    ui_cfg = project.get("ui") or {}

    cols_all = [c for c in (transform.get("columns") or []) if isinstance(c, dict) and c.get("name")]
    computed = transform.get("computed_columns") if isinstance(transform.get("computed_columns"), list) else []
    for cc in computed:
        if not isinstance(cc, dict):
            continue
        name = cc.get("name")
        if not name:
            continue
        # Computed columns become selectable output columns.
        cols_all.append(
            {
                "name": str(name),
                "label": str(cc.get("label") or name),
                "include": bool(cc.get("include")),
                "source": "computed",
            }
        )

    # Apply explicit output ordering if provided.
    order = transform.get("output_order") if isinstance(transform.get("output_order"), list) else []
    by_name = {c.get("name"): c for c in cols_all if c.get("name")}
    cols_selected = [c for c in cols_all if c.get("include")]
    if order:
        ordered = [by_name[n] for n in order if n in by_name and by_name[n].get("include")]
        rest = [c for c in cols_selected if c.get("name") not in set(order)]
        cols_selected = ordered + rest
    if not cols_selected:
        cols_selected = cols_all

    common_ctx = {
        "app_name": base_name,
        "subtitle": subtitle,
        "dataiku": dataiku_cfg,
        "project": project,
        "transform": transform,
        "ui": ui_cfg,
        "columns_selected": cols_selected,
        "expected_schema": _expected_schema(project, cols_selected),
    }

    apps: List[GeneratedApp] = []

    slug = _slug(base_name) or "webapp"
    files: Dict[str, str] = {}
    for tpl_name, out_name in [
        ("webapp/index.html.j2", "index.html"),
        ("webapp/style.css.j2", "style.css"),
        ("webapp/script.js.j2", "script.js"),
        ("webapp/backend.py.j2", "backend.py"),
        ("webapp/requirements.txt.j2", "requirements.txt"),
        ("webapp/SETUP.md.j2", "SETUP.md"),
        ("webapp/config.json.j2", "config.json"),
        ("webapp/app_config.json.j2", "app_config.json"),
        ("webapp/expected_schema.json.j2", "expected_schema.json"),
        ("webapp/EXPECTED_SCHEMA.md.j2", "EXPECTED_SCHEMA.md"),
        ("webapp/preview.html.j2", "preview.html"),
        ("webapp/preview_server.py.j2", "preview_server.py"),
        ("webapp/preview_requirements.txt.j2", "preview_requirements.txt"),
        ("webapp/PREVIEW.md.j2", "PREVIEW.md"),
    ]:
        tpl = env.get_template(tpl_name)
        files[out_name] = tpl.render(**common_ctx)
    apps.append(GeneratedApp(slug=slug, title=base_name, files=files))

    return apps


def build_zip_bytes(project: Dict[str, Any], *, templates_dir: str) -> bytes:
    apps = build_apps(project, templates_dir=templates_dir)
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Top-level README in the export bundle.
        readme = (
            "# Dataiku WebApp Export\\n\\n"
            "This zip was generated by `dataiku_webapp_forge`.\\n\\n"
            "Each subfolder is a Dataiku Standard WebApp. In Dataiku, create a new Standard WebApp and add the files\\n"
            "from the chosen folder: `index.html` (body-only), `style.css`, `script.js`, `backend.py`.\\n\\n"
            "See each folder's `SETUP.md` for the exact dataset/schema expectations.\\n"
        )
        zf.writestr("README.md", readme)

        for app in apps:
            base = app.slug + "/"
            for path, content in app.files.items():
                zf.writestr(base + path, content)
    return mem.getvalue()
