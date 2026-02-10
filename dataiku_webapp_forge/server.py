import io
import os
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, abort, redirect, render_template, request, send_file, url_for
from flask import jsonify
from werkzeug.utils import secure_filename

from .csv_inspect import inspect_csv, suggest_join_keys
from .analyze import join_health, sample_output, validate_config
from .generate import build_apps, build_zip_bytes
from .presets import list_presets, load_preset, save_preset
from .projects import (
    Project,
    create_project,
    delete_project,
    duplicate_project,
    ensure_dir,
    list_projects,
    load_project,
    project_uploads_dir,
    save_project,
)


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _get_project(app: Flask, project_id: str) -> Project:
    try:
        return load_project(app.instance_path, project_id)
    except Exception:
        abort(404)


def _sync_transform_columns(project: Project) -> None:
    """
    Ensure project.transform.columns is aligned with the uploaded CSV schema(s).

    - Dataset A columns are included by default.
    - Dataset B columns are prefixed as b__<col> and excluded by default.
    - Preserves include/label choices when possible.
    """
    csv_a = (project.data.get("csv") or {}).get("a") or {}
    csv_b = (project.data.get("csv") or {}).get("b") or {}
    csv_c = (project.data.get("csv") or {}).get("c") or {}
    cols_a = csv_a.get("columns") or []
    cols_b = csv_b.get("columns") or []
    cols_c = csv_c.get("columns") or []

    existing = project.data.setdefault("transform", {}).setdefault("columns", [])
    by_name: Dict[str, Dict[str, Any]] = {}
    for entry in existing:
        if isinstance(entry, dict) and entry.get("name"):
            by_name[str(entry["name"])] = entry

    new_entries: List[Dict[str, Any]] = []

    for c in cols_a:
        name = str(c)
        prev = by_name.get(name) or {}
        new_entries.append(
            {
                "name": name,
                "label": str(prev.get("label") or name),
                "include": bool(prev.get("include", True)),
                "source": "a",
            }
        )

    for c in cols_b:
        name = "b__" + str(c)
        prev = by_name.get(name) or {}
        new_entries.append(
            {
                "name": name,
                "label": str(prev.get("label") or ("B." + str(c))),
                "include": bool(prev.get("include", False)),
                "source": "b",
            }
        )

    for c in cols_c:
        name = "c__" + str(c)
        prev = by_name.get(name) or {}
        new_entries.append(
            {
                "name": name,
                "label": str(prev.get("label") or ("C." + str(c))),
                "include": bool(prev.get("include", False)),
                "source": "c",
            }
        )

    project.data.setdefault("transform", {})["columns"] = new_entries


def _maybe_seed_join_keys(project: Project) -> None:
    transform = project.data.setdefault("transform", {})
    joins = transform.get("joins") if isinstance(transform.get("joins"), list) else []
    # Ensure b/c steps exist.
    by_right: Dict[str, Dict[str, Any]] = {}
    for s in joins:
        if isinstance(s, dict) and s.get("right") in {"b", "c"}:
            by_right[str(s.get("right"))] = s
    jb = by_right.get("b") or {"right": "b", "enabled": False, "how": "left", "keys": [{"left": "", "right": ""}]}
    jc = by_right.get("c") or {"right": "c", "enabled": False, "how": "left", "keys": [{"left": "", "right": ""}]}

    csv_a = (project.data.get("csv") or {}).get("a") or {}
    csv_b = (project.data.get("csv") or {}).get("b") or {}
    csv_c = (project.data.get("csv") or {}).get("c") or {}
    cols_a = csv_a.get("columns") or []
    cols_b = csv_b.get("columns") or []
    cols_c = csv_c.get("columns") or []
    if not cols_a or not cols_b:
        # We can still seed join C based on A<->C if available.
        cols_b = []

    def has_keys(step: Dict[str, Any]) -> bool:
        keys = step.get("keys") if isinstance(step.get("keys"), list) else []
        return bool(keys and isinstance(keys[0], dict) and (keys[0].get("left") and keys[0].get("right")))

    if cols_a and cols_b and not has_keys(jb):
        left, right = suggest_join_keys(cols_a, cols_b)
        if left and right:
            jb["keys"] = [{"left": left, "right": right}]

    # Seed join to C using A columns by default.
    if cols_a and cols_c and not has_keys(jc):
        left, right = suggest_join_keys(cols_a, cols_c)
        if left and right:
            jc["keys"] = [{"left": left, "right": right}]

    transform["joins"] = [
        {"right": "b", "enabled": bool(jb.get("enabled")), "how": str(jb.get("how") or "left"), "keys": jb.get("keys") or [{"left": "", "right": ""}]},
        {"right": "c", "enabled": bool(jc.get("enabled")), "how": str(jc.get("how") or "left"), "keys": jc.get("keys") or [{"left": "", "right": ""}]},
    ]
    # Keep legacy join fields in sync with join B.
    transform["join_enabled"] = bool(transform["joins"][0].get("enabled"))
    transform.setdefault("join", {})
    transform["join"]["how"] = str(transform["joins"][0].get("how") or "left")
    transform["join"]["keys"] = [
        {"a": str(k.get("left") or ""), "b": str(k.get("right") or "")}
        for k in (transform["joins"][0].get("keys") or [])
        if isinstance(k, dict)
    ] or [{"a": "", "b": ""}]


def create_app() -> Flask:
    app = Flask(__name__, instance_relative_config=True)
    app.config["SECRET_KEY"] = os.environ.get("WEBAPP_FORGE_SECRET", "dev-secret-change-me")
    ensure_dir(app.instance_path)
    app.config["INSTANCE_PATH"] = app.instance_path
    ensure_dir(os.path.join(app.instance_path, "projects"))

    templates_dir = os.path.join(os.path.dirname(__file__), "webapp_templates")
    app.config["FORGE_TEMPLATES_DIR"] = templates_dir

    @app.get("/")
    def index():
        projects = list_projects(app.instance_path)
        return render_template("index.html", projects=projects)

    @app.post("/projects/new")
    def projects_new():
        proj = create_project(app.instance_path)
        return redirect(url_for("project_sources", project_id=proj.id))

    @app.post("/projects/<project_id>/delete")
    def project_delete(project_id: str):
        delete_project(app.instance_path, project_id)
        return redirect(url_for("index"))

    @app.post("/projects/<project_id>/duplicate")
    def project_duplicate(project_id: str):
        new = duplicate_project(app.instance_path, project_id)
        return redirect(url_for("project_sources", project_id=new.id))

    @app.get("/projects/<project_id>")
    def project_root(project_id: str):
        return redirect(url_for("project_sources", project_id=project_id))

    @app.route("/projects/<project_id>/sources", methods=["GET", "POST"])
    def project_sources(project_id: str):
        proj = _get_project(app, project_id)
        error = ""

        if request.method == "POST":
            app_name = (request.form.get("app_name") or "").strip()
            subtitle = (request.form.get("subtitle") or "").strip()
            ds_a = (request.form.get("dataset_a") or "").strip()
            ds_b = (request.form.get("dataset_b") or "").strip()
            ds_c = (request.form.get("dataset_c") or "").strip()

            if app_name:
                proj.data.setdefault("app", {})["name"] = app_name
            proj.data.setdefault("app", {})["subtitle"] = subtitle
            if ds_a:
                proj.data.setdefault("dataiku", {})["dataset_a"] = ds_a
            proj.data.setdefault("dataiku", {})["dataset_b"] = ds_b
            proj.data.setdefault("dataiku", {})["dataset_c"] = ds_c

            uploads = project_uploads_dir(app.instance_path, project_id)
            ensure_dir(uploads)

            file_a = request.files.get("csv_a")
            file_b = request.files.get("csv_b")
            file_c = request.files.get("csv_c")

            if file_a and file_a.filename:
                name = secure_filename(file_a.filename)
                path = os.path.join(uploads, name)
                file_a.save(path)
                info = inspect_csv(path)
                proj.data.setdefault("csv", {})["a"] = {
                    "filename": info.filename,
                    "path": info.path,
                    "columns": info.columns,
                    "sample_rows": info.sample_rows,
                }

            if file_b and file_b.filename:
                name = secure_filename(file_b.filename)
                path = os.path.join(uploads, name)
                file_b.save(path)
                info = inspect_csv(path)
                proj.data.setdefault("csv", {})["b"] = {
                    "filename": info.filename,
                    "path": info.path,
                    "columns": info.columns,
                    "sample_rows": info.sample_rows,
                }

            if file_c and file_c.filename:
                name = secure_filename(file_c.filename)
                path = os.path.join(uploads, name)
                file_c.save(path)
                info = inspect_csv(path)
                proj.data.setdefault("csv", {})["c"] = {
                    "filename": info.filename,
                    "path": info.path,
                    "columns": info.columns,
                    "sample_rows": info.sample_rows,
                }

            if not (proj.data.get("csv") or {}).get("a"):
                error = "Please upload at least CSV A (your primary dataset export)."
            else:
                _sync_transform_columns(proj)
                _maybe_seed_join_keys(proj)
                # If dataset_b is empty and there is no CSV B, force join off.
                if not (proj.data.get("csv") or {}).get("b"):
                    tr = proj.data.setdefault("transform", {})
                    tr["join_enabled"] = False
                    if isinstance(tr.get("joins"), list) and tr["joins"]:
                        tr["joins"][0]["enabled"] = False
                # If dataset_c is empty and there is no CSV C, force join-to-C off.
                if not (proj.data.get("csv") or {}).get("c"):
                    tr = proj.data.setdefault("transform", {})
                    if isinstance(tr.get("joins"), list) and len(tr["joins"]) > 1:
                        tr["joins"][1]["enabled"] = False
                save_project(app.instance_path, proj)
                return redirect(url_for("project_transform", project_id=project_id))

        csv_a = (proj.data.get("csv") or {}).get("a")
        csv_b = (proj.data.get("csv") or {}).get("b")
        csv_c = (proj.data.get("csv") or {}).get("c")
        return render_template(
            "sources.html",
            project=proj,
            csv_a=csv_a,
            csv_b=csv_b,
            csv_c=csv_c,
            error=error,
            active_tab="sources",
        )

    @app.route("/projects/<project_id>/transform", methods=["GET", "POST"])
    def project_transform(project_id: str):
        proj = _get_project(app, project_id)
        error = ""

        csv_a = (proj.data.get("csv") or {}).get("a") or {}
        csv_b = (proj.data.get("csv") or {}).get("b") or {}
        csv_c = (proj.data.get("csv") or {}).get("c") or {}
        cols_a = csv_a.get("columns") or []
        cols_b = csv_b.get("columns") or []
        cols_c = csv_c.get("columns") or []

        # Ensure columns are present if user navigates here directly after upload.
        _sync_transform_columns(proj)

        transform = proj.data.setdefault("transform", {})
        join = transform.setdefault("join", {"how": "left", "keys": [{"a": "", "b": ""}]})
        # Canonical join steps
        joins = transform.get("joins") if isinstance(transform.get("joins"), list) else []
        if not joins:
            # Best-effort: build from legacy join.
            keys = join.get("keys") if isinstance(join.get("keys"), list) else []
            pairs = []
            for k in keys:
                if not isinstance(k, dict):
                    continue
                a = str(k.get("a") or "").strip()
                b = str(k.get("b") or "").strip()
                if a or b:
                    pairs.append({"left": a, "right": b})
            if not pairs:
                pairs = [{"left": "", "right": ""}]
            joins = [
                {"right": "b", "enabled": bool(transform.get("join_enabled")), "how": str(join.get("how") or "left"), "keys": pairs},
                {"right": "c", "enabled": False, "how": "left", "keys": [{"left": "", "right": ""}]},
            ]
            transform["joins"] = joins

        def _step(right: str) -> Dict[str, Any]:
            for s in joins:
                if isinstance(s, dict) and str(s.get("right") or "").lower() == right:
                    return s
            s = {"right": right, "enabled": False, "how": "left", "keys": [{"left": "", "right": ""}]}
            joins.append(s)
            return s

        join_b = _step("b")
        join_c = _step("c")

        # Ensure legacy flat filters are wrapped.
        if "filter_groups" not in transform:
            legacy = transform.get("filters") if isinstance(transform.get("filters"), list) else []
            transform["filter_groups"] = [{"filters": legacy}]
        transform.setdefault("filter_groups", [{"filters": []}])
        transform.setdefault("computed_columns", [])
        transform.setdefault("output_order", [])

        if request.method == "POST":
            action = (request.form.get("action") or "").strip()
            if action == "apply_preset":
                preset_id = (request.form.get("preset_id") or "").strip()
                if not preset_id:
                    error = "Select a preset to apply."
                else:
                    try:
                        preset = load_preset(app.instance_path, preset_id)
                        preset_transform = preset.get("transform") if isinstance(preset.get("transform"), dict) else {}
                        preset_ui = preset.get("ui") if isinstance(preset.get("ui"), dict) else {}
                        # Apply transform + UI without touching datasets/CSVs.
                        proj.data["transform"] = {**transform, **preset_transform}
                        proj.data["ui"] = {**(proj.data.get("ui") or {}), **preset_ui}
                        # Resync columns for current CSV schemas, preserving include/labels when possible.
                        _sync_transform_columns(proj)
                        _maybe_seed_join_keys(proj)
                        save_project(app.instance_path, proj)
                        return redirect(url_for("project_transform", project_id=project_id))
                    except Exception as exc:
                        error = "Failed to apply preset: " + str(exc)

            if action == "save_preset":
                preset_name = (request.form.get("preset_name") or "").strip()
                if not preset_name:
                    error = "Enter a preset name."
                else:
                    try:
                        save_preset(app.instance_path, preset_name, transform=transform, ui=(proj.data.get("ui") or {}))
                        return redirect(url_for("project_transform", project_id=project_id))
                    except Exception as exc:
                        error = "Failed to save preset: " + str(exc)

            # Join B
            join_b_enabled = request.form.get("join_b_enabled") == "on"
            if not cols_b:
                join_b_enabled = False
            join_b["enabled"] = bool(join_b_enabled)
            join_b["how"] = (request.form.get("join_b_how") or "left").strip().lower()
            key_pairs_b: List[Dict[str, str]] = []
            for i in range(0, 10):
                l_key = (request.form.get(f"join_b_left_{i}") or "").strip()
                r_key = (request.form.get(f"join_b_right_{i}") or "").strip()
                if not l_key and not r_key:
                    continue
                key_pairs_b.append({"left": l_key, "right": r_key})
            if not key_pairs_b:
                key_pairs_b = [{"left": "", "right": ""}]
            join_b["keys"] = key_pairs_b

            # Join C (advanced)
            join_c_enabled = request.form.get("join_c_enabled") == "on"
            if not cols_c:
                join_c_enabled = False
            join_c["enabled"] = bool(join_c_enabled)
            join_c["how"] = (request.form.get("join_c_how") or "left").strip().lower()
            left_cols_for_c: List[str] = list(cols_a)
            if join_b_enabled:
                left_cols_for_c += ["b__" + str(c) for c in cols_b]
            key_pairs_c: List[Dict[str, str]] = []
            for i in range(0, 10):
                l_key = (request.form.get(f"join_c_left_{i}") or "").strip()
                r_key = (request.form.get(f"join_c_right_{i}") or "").strip()
                if not l_key and not r_key:
                    continue
                key_pairs_c.append({"left": l_key, "right": r_key})
            if not key_pairs_c:
                key_pairs_c = [{"left": "", "right": ""}]
            join_c["keys"] = key_pairs_c

            # Persist canonical joins list in order.
            transform["joins"] = [
                {"right": "b", "enabled": bool(join_b.get("enabled")), "how": str(join_b.get("how") or "left"), "keys": join_b.get("keys") or [{"left": "", "right": ""}]},
                {"right": "c", "enabled": bool(join_c.get("enabled")), "how": str(join_c.get("how") or "left"), "keys": join_c.get("keys") or [{"left": "", "right": ""}]},
            ]

            # Backward compat fields (join_enabled/join) track join B.
            join_enabled = bool(join_b_enabled)
            transform["join_enabled"] = join_enabled
            join["how"] = str(join_b.get("how") or "left")
            join["keys"] = [{"a": str(k.get("left") or ""), "b": str(k.get("right") or "")} for k in (join_b.get("keys") or []) if isinstance(k, dict)] or [{"a": "", "b": ""}]

            # Columns selection + labels
            new_cols: List[Dict[str, Any]] = []
            for idx, entry in enumerate(transform.get("columns") or []):
                if not isinstance(entry, dict) or not entry.get("name"):
                    continue
                name = str(entry["name"])
                include = request.form.get(f"col_include_{idx}") == "on"
                label = (request.form.get(f"col_label_{idx}") or "").strip()
                source = entry.get("source") or ("c" if name.startswith("c__") else ("b" if name.startswith("b__") else "a"))
                if source == "b" and not join_b_enabled:
                    include = False
                if source == "c" and not join_c_enabled:
                    include = False
                new_cols.append(
                    {
                        "name": name,
                        "label": label or name,
                        "include": include,
                        "source": source,
                    }
                )
            transform["columns"] = new_cols

            # Output ordering (drag list)
            order_raw = (request.form.get("output_order") or "").strip()
            if order_raw:
                transform["output_order"] = [x for x in order_raw.split(",") if x]

            # Filter groups (OR-of-AND)
            groups: List[Dict[str, Any]] = []
            for g in range(0, 10):
                group_filters: List[Dict[str, str]] = []
                for i in range(0, 50):
                    col = (request.form.get(f"fg_{g}_col_{i}") or "").strip()
                    op = (request.form.get(f"fg_{g}_op_{i}") or "").strip()
                    val = (request.form.get(f"fg_{g}_val_{i}") or "").strip()
                    if not col and not op and not val:
                        continue
                    if not col or not op:
                        error = "Each filter needs a column and an operator."
                        break
                    group_filters.append({"column": col, "op": op, "value": val})
                if error:
                    break
                if group_filters:
                    groups.append({"filters": group_filters})
            if not error:
                transform["filter_groups"] = groups if groups else [{"filters": []}]

            # Backward compat
            transform["filters"] = []

            # Computed columns
            computed: List[Dict[str, Any]] = []
            for i in range(0, 30):
                ctype = (request.form.get(f"cc_type_{i}") or "").strip()
                name = (request.form.get(f"cc_name_{i}") or "").strip()
                if not ctype and not name:
                    continue
                if not ctype or not name:
                    error = "Computed columns need a type and a name."
                    break
                entry: Dict[str, Any] = {"type": ctype, "name": name}
                entry["include"] = request.form.get(f"cc_include_{i}") == "on"
                extra = (request.form.get(f"cc_extra_{i}") or "").strip()
                if ctype == "concat":
                    entry["columns"] = [x.strip() for x in (request.form.get(f"cc_cols_{i}") or "").split(",") if x.strip()]
                    entry["sep"] = extra
                elif ctype == "coalesce":
                    entry["columns"] = [x.strip() for x in (request.form.get(f"cc_cols_{i}") or "").split(",") if x.strip()]
                elif ctype == "date_format":
                    # Accept "col | fmt" in extra, or "col" in cols input.
                    if "|" in extra:
                        col, fmt = [p.strip() for p in extra.split("|", 1)]
                        entry["column"] = col
                        entry["format"] = fmt or "%Y-%m-%d"
                    else:
                        entry["column"] = extra or (request.form.get(f"cc_cols_{i}") or "").strip()
                        entry["format"] = "%Y-%m-%d"
                elif ctype == "bucket":
                    if "|" in extra:
                        col, size = [p.strip() for p in extra.split("|", 1)]
                        entry["column"] = col
                        entry["size"] = _safe_int(size, 10)
                    else:
                        entry["column"] = extra or (request.form.get(f"cc_cols_{i}") or "").strip()
                        entry["size"] = 10
                else:
                    error = "Unsupported computed column type: " + ctype
                    break
                computed.append(entry)
            if not error:
                transform["computed_columns"] = computed

            # Sort + limit
            transform.setdefault("sort", {})
            transform["sort"]["column"] = (request.form.get("sort_column") or "").strip()
            transform["sort"]["direction"] = (request.form.get("sort_direction") or "asc").strip().lower()
            transform["limit"] = max(1, min(200000, _safe_int(request.form.get("limit"), 2000)))

            if join_b_enabled:
                pairs = [(p.get("left") or "", p.get("right") or "") for p in (join_b.get("keys") or []) if isinstance(p, dict)]
                pairs = [(a.strip(), b.strip()) for a, b in pairs if a.strip() or b.strip()]
                if not pairs:
                    error = "Join to Dataset B is enabled: add at least one join key pair."
                else:
                    for a, b in pairs:
                        if not a or a not in cols_a:
                            error = "Join to Dataset B is enabled: invalid left join key: " + (a or "(blank)")
                            break
                        if not b or b not in cols_b:
                            error = "Join to Dataset B is enabled: invalid Dataset B join key: " + (b or "(blank)")
                            break
                if not error and str(join_b.get("how") or "") not in {"left", "inner"}:
                    error = "Join to Dataset B type must be left or inner."

            if not error and join_c_enabled:
                pairs = [(p.get("left") or "", p.get("right") or "") for p in (join_c.get("keys") or []) if isinstance(p, dict)]
                pairs = [(a.strip(), b.strip()) for a, b in pairs if a.strip() or b.strip()]
                if not pairs:
                    error = "Join to Dataset C is enabled: add at least one join key pair."
                else:
                    for a, b in pairs:
                        if not a or a not in left_cols_for_c:
                            error = "Join to Dataset C is enabled: invalid left join key: " + (a or "(blank)")
                            break
                        if not b or b not in cols_c:
                            error = "Join to Dataset C is enabled: invalid Dataset C join key: " + (b or "(blank)")
                            break
                if not error and str(join_c.get("how") or "") not in {"left", "inner"}:
                    error = "Join to Dataset C type must be left or inner."

            if not error:
                save_project(app.instance_path, proj)
                next_tab = request.form.get("next") or ""
                if next_tab == "ui":
                    return redirect(url_for("project_ui", project_id=project_id))
                return redirect(url_for("project_export_page", project_id=project_id))

        # Column options for filters/sort should include all available columns, not only displayed ones.
        all_cols = [c for c in (transform.get("columns") or []) if isinstance(c, dict) and c.get("name")]
        column_names = [c.get("name") for c in all_cols if c.get("name")]

        cols_with_index = list(enumerate(transform.get("columns") or []))
        cols_a_entries = [(i, c) for i, c in cols_with_index if isinstance(c, dict) and c.get("source") == "a"]
        cols_b_entries = [(i, c) for i, c in cols_with_index if isinstance(c, dict) and c.get("source") == "b"]
        cols_c_entries = [(i, c) for i, c in cols_with_index if isinstance(c, dict) and c.get("source") == "c"]
        presets = list_presets(app.instance_path)

        return render_template(
            "transform.html",
            project=proj,
            cols_a=cols_a,
            cols_b=cols_b,
            cols_c=cols_c,
            transform=transform,
            join_b=join_b,
            join_c=join_c,
            left_cols_for_c=(cols_a + (["b__" + str(c) for c in cols_b] if bool(join_b.get("enabled")) else [])),
            column_names=column_names,
            cols_a_entries=cols_a_entries,
            cols_b_entries=cols_b_entries,
            cols_c_entries=cols_c_entries,
            presets=presets,
            error=error,
            active_tab="transform",
        )

    @app.route("/projects/<project_id>/ui", methods=["GET", "POST"])
    def project_ui(project_id: str):
        proj = _get_project(app, project_id)
        ui = proj.data.setdefault("ui", {})
        if request.method == "POST":
            ui["row_details"] = request.form.get("row_details") == "on"
            ui["template"] = (request.form.get("template") or "table").strip()
            ui["pagination"] = request.form.get("pagination") == "on"
            ui["page_size"] = max(10, min(5000, _safe_int(request.form.get("page_size"), 200)))
            ff = (request.form.get("frontend_filters") or "").strip()
            ui["frontend_filters"] = [x.strip() for x in ff.split(",") if x.strip()]
            ui.setdefault("chart", {})
            ui["chart"]["enabled"] = request.form.get("chart_enabled") == "on"
            ui["chart"]["type"] = (request.form.get("chart_type") or "bar").strip()
            ui["chart"]["column"] = (request.form.get("chart_column") or "").strip()
            ui["chart"]["x_column"] = (request.form.get("chart_x_column") or "").strip()
            ui["chart"]["y_column"] = (request.form.get("chart_y_column") or "").strip()
            ui["chart"]["agg"] = (request.form.get("chart_agg") or "count").strip()
            ui["chart"]["top_n"] = max(1, min(50, _safe_int(request.form.get("chart_top_n"), 12)))
            ui["chart"]["bins"] = max(3, min(80, _safe_int(request.form.get("chart_bins"), 16)))
            ui["chart"]["max_points"] = max(50, min(5000, _safe_int(request.form.get("chart_max_points"), 600)))
            # Allow updating app name/subtitle from here too.
            app_name = (request.form.get("app_name") or "").strip()
            subtitle = (request.form.get("subtitle") or "").strip()
            if app_name:
                proj.data.setdefault("app", {})["name"] = app_name
            proj.data.setdefault("app", {})["subtitle"] = subtitle
            save_project(app.instance_path, proj)
            return redirect(url_for("project_export_page", project_id=project_id))

        return render_template("ui.html", project=proj, ui=ui, active_tab="ui")

    @app.get("/projects/<project_id>/export")
    def project_export_page(project_id: str):
        proj = _get_project(app, project_id)
        apps = build_apps(proj.data, templates_dir=app.config["FORGE_TEMPLATES_DIR"])
        return render_template("export.html", project=proj, apps=apps, active_tab="export")

    @app.get("/projects/<project_id>/export.zip")
    def project_export_zip(project_id: str):
        proj = _get_project(app, project_id)
        zbytes = build_zip_bytes(proj.data, templates_dir=app.config["FORGE_TEMPLATES_DIR"])
        fname = "dataiku-webapp-" + project_id[:8] + ".zip"
        return send_file(
            io.BytesIO(zbytes),
            mimetype="application/zip",
            as_attachment=True,
            download_name=fname,
        )

    @app.get("/projects/<project_id>/analyze.json")
    def project_analyze(project_id: str):
        proj = _get_project(app, project_id)
        csv_a = (proj.data.get("csv") or {}).get("a") or {}
        csv_b = (proj.data.get("csv") or {}).get("b") or {}
        csv_c = (proj.data.get("csv") or {}).get("c") or {}
        path_a = csv_a.get("path") or ""
        path_b = csv_b.get("path") or ""
        path_c = csv_c.get("path") or ""
        cols_a = csv_a.get("columns") or []
        cols_b = csv_b.get("columns") or []
        cols_c = csv_c.get("columns") or []
        transform = proj.data.get("transform") or {}
        ui = proj.data.get("ui") or {}

        validation = validate_config(cols_a, cols_b, cols_c, transform, ui)

        # Join health on samples only (fast). Only if joins are enabled.
        health: Dict[str, Any] = {}
        try:
            joins = transform.get("joins") if isinstance(transform.get("joins"), list) else []
            if path_a and (path_b or path_c) and joins:
                health = join_health(path_a, path_b or None, path_c or None, joins=joins, max_rows=5000)
        except Exception as exc:
            validation.setdefault("warnings", []).append("Join health report failed: " + str(exc))

        preview = {}
        if path_a:
            try:
                preview = sample_output(
                    path_a,
                    path_b if path_b else None,
                    path_c if path_c else None,
                    transform=transform,
                )
            except Exception as exc:
                validation.setdefault("warnings", []).append("Sample output failed: " + str(exc))

        return jsonify({"status": "ok", "validation": validation, "join_health": health, "preview": preview})

    return app


def main() -> None:
    app = create_app()
    port = int(os.environ.get("WEBAPP_FORGE_PORT", "5010"))
    host = os.environ.get("WEBAPP_FORGE_HOST", "127.0.0.1")
    print(f"Dataiku WebApp Forge running on http://{host}:{port}")
    app.run(host=host, port=port, debug=True)
