import io
import os
import time
from typing import Any, Dict, List

from flask import Flask, abort, redirect, render_template, request, send_file, url_for
from flask import jsonify
from werkzeug.utils import secure_filename

from .csv_inspect import inspect_csv
from .analyze import sample_output, validate_config
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

def _project_display_name(p: Project) -> str:
    return str(((p.data.get("app") or {}).get("name")) or "Untitled")


def _project_tags(p: Project) -> List[str]:
    meta = p.data.get("meta") or {}
    tags = meta.get("tags") if isinstance(meta, dict) else []
    if not isinstance(tags, list):
        return []
    return [str(t).strip() for t in tags if str(t).strip()]


def _project_pinned(p: Project) -> bool:
    meta = p.data.get("meta") or {}
    return bool(isinstance(meta, dict) and meta.get("pinned"))


def _clean_uploads(proj: Project) -> None:
    """
    Remove uploaded CSV files and strip sensitive samples/paths, but keep schema column names.
    This keeps the project usable for export, but Analyze preview will require re-uploading.
    """
    uploads_dir = os.path.join(proj.root_dir, "uploads")
    if os.path.isdir(uploads_dir):
        for name in os.listdir(uploads_dir):
            path = os.path.join(uploads_dir, name)
            try:
                if os.path.isfile(path):
                    os.remove(path)
            except Exception:
                pass

    csv = proj.data.get("csv") if isinstance(proj.data.get("csv"), dict) else {}
    for key in ("a", "b", "c"):
        info = csv.get(key)
        if not isinstance(info, dict):
            continue
        info["path"] = ""
        # Samples can contain sensitive data. Keep only schema (columns).
        info["sample_rows"] = []
        csv[key] = info
    proj.data["csv"] = csv


def _sync_transform_columns(project: Project) -> None:
    """
    Ensure project.transform.columns is aligned with Dataset A schema.
    Preserves include/label choices when possible.
    """
    csv_a = (project.data.get("csv") or {}).get("a") or {}
    cols_a = csv_a.get("columns") or []

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

    project.data.setdefault("transform", {})["columns"] = new_entries


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
        q = (request.args.get("q") or "").strip().lower()
        tag = (request.args.get("tag") or "").strip().lower()

        projects = list_projects(app.instance_path)
        for p in projects:
            try:
                ts = int(p.data.get("updated_at_unix") or 0)
                p.data["_updated_at_human"] = time.strftime("%Y-%m-%d %H:%M", time.localtime(ts)) if ts else ""
            except Exception:
                p.data["_updated_at_human"] = ""
        # Sort: pinned first, then most recently updated.
        projects.sort(
            key=lambda p: (
                0 if _project_pinned(p) else 1,
                -int(p.data.get("updated_at_unix") or 0),
                _project_display_name(p).lower(),
            )
        )

        if q or tag:
            filtered: List[Project] = []
            for p in projects:
                name = _project_display_name(p).lower()
                pid = str(p.id or "").lower()
                tags = [t.lower() for t in _project_tags(p)]
                ok = True
                if tag and tag not in tags:
                    ok = False
                if q and ok:
                    ok = (q in name) or (q in pid) or any(q in t for t in tags)
                if ok:
                    filtered.append(p)
            projects = filtered

        presets = list_presets(app.instance_path)
        return render_template("index.html", projects=projects, presets=presets, q=q, tag=tag)

    @app.post("/projects/new")
    def projects_new():
        proj = create_project(app.instance_path)
        return redirect(url_for("project_sources", project_id=proj.id))

    @app.post("/projects/new_from_preset")
    def projects_new_from_preset():
        preset_id = (request.form.get("preset_id") or "").strip()
        if not preset_id:
            return redirect(url_for("index"))
        proj = create_project(app.instance_path)
        try:
            preset = load_preset(app.instance_path, preset_id)
            preset_transform = preset.get("transform") if isinstance(preset.get("transform"), dict) else {}
            preset_ui = preset.get("ui") if isinstance(preset.get("ui"), dict) else {}
            proj.data["transform"] = {**(proj.data.get("transform") or {}), **preset_transform}
            proj.data["ui"] = {**(proj.data.get("ui") or {}), **preset_ui}
            # Ensure schema-dependent bits are aligned.
            _sync_transform_columns(proj)
            # Mark the project name so it's obvious it came from a preset.
            pname = str(preset.get("name") or "Preset").strip()
            proj.data.setdefault("app", {})["name"] = f"{pname} WebApp"
        except Exception:
            pass
        save_project(app.instance_path, proj)
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

    @app.route("/projects/<project_id>/settings", methods=["GET", "POST"])
    def project_settings(project_id: str):
        proj = _get_project(app, project_id)
        error = ""
        if request.method == "POST":
            action = (request.form.get("action") or "").strip()

            if action == "clean_uploads":
                _clean_uploads(proj)
                save_project(app.instance_path, proj)
                return redirect(url_for("project_settings", project_id=project_id))

            # Update fields
            app_name = (request.form.get("app_name") or "").strip()
            subtitle = (request.form.get("subtitle") or "").strip()
            tags_raw = (request.form.get("tags") or "").strip()
            pinned = request.form.get("pinned") == "on"

            if not app_name:
                error = "Project name is required."
            else:
                proj.data.setdefault("app", {})["name"] = app_name
                proj.data.setdefault("app", {})["subtitle"] = subtitle
                meta = proj.data.get("meta") if isinstance(proj.data.get("meta"), dict) else {}
                tags = [t.strip() for t in tags_raw.split(",") if t.strip()]
                meta["tags"] = tags
                meta["pinned"] = bool(pinned)
                proj.data["meta"] = meta
                save_project(app.instance_path, proj)
                return redirect(url_for("project_settings", project_id=project_id))

        return render_template("settings.html", project=proj, error=error, active_tab="settings")

    @app.route("/projects/<project_id>/sources", methods=["GET", "POST"])
    def project_sources(project_id: str):
        proj = _get_project(app, project_id)
        error = ""

        if request.method == "POST":
            app_name = (request.form.get("app_name") or "").strip()
            subtitle = (request.form.get("subtitle") or "").strip()
            ds_a = (request.form.get("dataset_a") or "").strip()

            if app_name:
                proj.data.setdefault("app", {})["name"] = app_name
            proj.data.setdefault("app", {})["subtitle"] = subtitle
            if ds_a:
                proj.data.setdefault("dataiku", {})["dataset_a"] = ds_a

            uploads = project_uploads_dir(app.instance_path, project_id)
            ensure_dir(uploads)

            file_a = request.files.get("csv_a")

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

            if not (proj.data.get("csv") or {}).get("a"):
                error = "Please upload at least CSV A (your primary dataset export)."
            else:
                _sync_transform_columns(proj)
                save_project(app.instance_path, proj)
                return redirect(url_for("project_transform", project_id=project_id))

        csv_a = (proj.data.get("csv") or {}).get("a")
        return render_template(
            "sources.html",
            project=proj,
            csv_a=csv_a,
            error=error,
            active_tab="sources",
        )

    @app.route("/projects/<project_id>/transform", methods=["GET", "POST"])
    def project_transform(project_id: str):
        proj = _get_project(app, project_id)
        error = ""

        csv_a = (proj.data.get("csv") or {}).get("a") or {}
        cols_a = csv_a.get("columns") or []

        # Ensure columns are present if user navigates here directly after upload.
        _sync_transform_columns(proj)

        transform = proj.data.setdefault("transform", {})
        # Single-dataset mode: keep legacy join keys disabled for compatibility.
        transform["join_enabled"] = False
        transform["joins"] = []
        transform["join"] = {"how": "left", "keys": [{"a": "", "b": ""}]}

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
                        proj.data.setdefault("transform", {})["join_enabled"] = False
                        proj.data.setdefault("transform", {})["joins"] = []
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

            # Columns selection + labels
            new_cols: List[Dict[str, Any]] = []
            for idx, entry in enumerate(transform.get("columns") or []):
                if not isinstance(entry, dict) or not entry.get("name"):
                    continue
                name = str(entry["name"])
                include = request.form.get(f"col_include_{idx}") == "on"
                label = (request.form.get(f"col_label_{idx}") or "").strip()
                source = "a"
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
        presets = list_presets(app.instance_path)

        return render_template(
            "transform.html",
            project=proj,
            cols_a=cols_a,
            transform=transform,
            column_names=column_names,
            cols_a_entries=cols_a_entries,
            presets=presets,
            error=error,
            active_tab="transform",
        )

    @app.route("/projects/<project_id>/ui", methods=["GET", "POST"])
    def project_ui(project_id: str):
        proj = _get_project(app, project_id)
        ui = proj.data.setdefault("ui", {})
        allowed_templates = {"table", "sidebar_filters", "master_detail", "chart_table"}
        if str(ui.get("template") or "table") not in allowed_templates:
            ui["template"] = "table"
        if request.method == "POST":
            ui["row_details"] = request.form.get("row_details") == "on"
            tpl = (request.form.get("template") or "table").strip()
            ui["template"] = tpl if tpl in allowed_templates else "table"
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
        path_a = csv_a.get("path") or ""
        cols_a = csv_a.get("columns") or []
        transform = proj.data.get("transform") or {}
        ui = proj.data.get("ui") or {}

        validation = validate_config(cols_a, transform, ui)

        preview = {}
        if path_a:
            try:
                preview = sample_output(
                    path_a,
                    transform=transform,
                )
            except Exception as exc:
                validation.setdefault("warnings", []).append("Sample output failed: " + str(exc))

        return jsonify({"status": "ok", "validation": validation, "join_health": {"steps": []}, "preview": preview})

    return app


def main() -> None:
    app = create_app()
    port = int(os.environ.get("WEBAPP_FORGE_PORT", "5010"))
    host = os.environ.get("WEBAPP_FORGE_HOST", "127.0.0.1")
    print(f"Dataiku WebApp Forge running on http://{host}:{port}")
    app.run(host=host, port=port, debug=True)
