import json
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class Project:
    id: str
    root_dir: str
    data: Dict[str, Any]


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def projects_root(instance_dir: str) -> str:
    return os.path.join(instance_dir, "projects")


def project_dir(instance_dir: str, project_id: str) -> str:
    return os.path.join(projects_root(instance_dir), project_id)


def project_json_path(instance_dir: str, project_id: str) -> str:
    return os.path.join(project_dir(instance_dir, project_id), "project.json")


def project_uploads_dir(instance_dir: str, project_id: str) -> str:
    return os.path.join(project_dir(instance_dir, project_id), "uploads")


def create_project(instance_dir: str) -> Project:
    pid = uuid.uuid4().hex
    root = project_dir(instance_dir, pid)
    ensure_dir(root)
    ensure_dir(project_uploads_dir(instance_dir, pid))
    now = int(time.time())
    data: Dict[str, Any] = {
        "id": pid,
        "created_at_unix": now,
        "updated_at_unix": now,
        "meta": {
            "tags": [],
            "pinned": False,
        },
        "app": {
            "name": "Simple Dataiku WebApp",
            "subtitle": "",
        },
        "dataiku": {
            "dataset_a": "dataset_a",
        },
        "csv": {
            "a": None,
        },
        "transform": {
            # OR-of-AND filter groups: any group may match; within a group all filters must match.
            "filter_groups": [{"filters": []}],
            # Backward compat: earlier versions stored a flat list in transform.filters.
            "filters": [],
            "computed_columns": [],
            "columns": [],
            # Explicit output ordering (names). If empty, order falls back to transform.columns list.
            "output_order": [],
            "sort": {"column": "", "direction": "asc"},
            "limit": 2000,
        },
        "ui": {
            "row_details": True,
            "template": "table",  # table|sidebar_filters|master_detail|chart_table
            "frontend_filters": [],
            "pagination": False,
            "page_size": 200,
            "chart": {
                "enabled": False,
                # bar|hist|line|scatter
                "type": "bar",
                # Backward compat: "column" is used for bar/hist when x_column is not set.
                "column": "",
                # General inputs
                "x_column": "",
                "y_column": "",
                # Aggregation for line charts: count|sum|mean
                "agg": "count",
                # Parameters
                "top_n": 12,
                "bins": 16,
                "max_points": 600,
            },
        },
    }
    proj = Project(id=pid, root_dir=root, data=data)
    save_project(instance_dir, proj)
    return proj


def load_project(instance_dir: str, project_id: str) -> Project:
    path = project_json_path(instance_dir, project_id)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    data = normalize_project_data(data)
    return Project(id=project_id, root_dir=project_dir(instance_dir, project_id), data=data)


def save_project(instance_dir: str, project: Project) -> None:
    project.data["updated_at_unix"] = int(time.time())
    path = project_json_path(instance_dir, project.id)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(project.data, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def normalize_project_data(data: Dict[str, Any]) -> Dict[str, Any]:
    # Best-effort migration from the earlier role/audit-based schema.
    if not isinstance(data, dict):
        return {}

    now = int(time.time())
    data.setdefault("id", uuid.uuid4().hex)
    data.setdefault("created_at_unix", now)
    data.setdefault("updated_at_unix", now)

    meta = data.setdefault("meta", {})
    if not isinstance(meta, dict):
        meta = {}
    # Tags are freeform, used for filtering/search.
    tags = meta.get("tags")
    if not isinstance(tags, list):
        tags = []
    meta["tags"] = [str(t).strip() for t in tags if str(t).strip()]
    meta.setdefault("pinned", False)
    meta["pinned"] = bool(meta.get("pinned"))
    data["meta"] = meta

    app = data.setdefault("app", {})
    app.setdefault("name", "Simple Dataiku WebApp")
    app.setdefault("subtitle", "")

    dku = data.setdefault("dataiku", {})
    # Older projects used source_dataset/audit_dataset; map source_dataset to dataset_a.
    dku.setdefault("dataset_a", dku.get("source_dataset") or "dataset_a")

    csv = data.setdefault("csv", {})
    # Older projects stored csv.source/csv.audit; map source -> a.
    if "a" not in csv:
        csv["a"] = csv.get("source") or None

    transform = data.setdefault("transform", {})
    # Keep these fields present for backward compatibility, but force single-dataset mode.
    transform["join_enabled"] = False
    transform["join"] = {"how": "left", "keys": [{"a": "", "b": ""}]}
    transform["joins"] = []

    transform.setdefault("filters", [])
    # If we have a legacy flat filter list and no groups, wrap it into one group.
    if "filter_groups" not in transform:
        legacy = transform.get("filters") if isinstance(transform.get("filters"), list) else []
        transform["filter_groups"] = [{"filters": legacy}]
    transform.setdefault("filter_groups", [{"filters": []}])
    transform.setdefault("computed_columns", [])
    transform.setdefault("columns", [])
    transform.setdefault("output_order", [])
    transform.setdefault("sort", {"column": "", "direction": "asc"})
    transform.setdefault("limit", 2000)

    ui = data.setdefault("ui", {})
    ui.setdefault("row_details", True)
    ui.setdefault("template", "table")
    ui.setdefault("frontend_filters", [])
    ui.setdefault("pagination", False)
    ui.setdefault("page_size", 200)
    ui.setdefault(
        "chart",
        {
            "enabled": False,
            "type": "bar",
            "column": "",
            "x_column": "",
            "y_column": "",
            "agg": "count",
            "top_n": 12,
            "bins": 16,
            "max_points": 600,
        },
    )
    # Normalize chart config keys for older projects.
    chart = ui.get("chart") if isinstance(ui.get("chart"), dict) else {}
    if isinstance(chart, dict):
        chart.setdefault("enabled", False)
        chart.setdefault("type", "bar")
        chart.setdefault("column", "")
        chart.setdefault("x_column", "")
        chart.setdefault("y_column", "")
        chart.setdefault("agg", "count")
        chart.setdefault("top_n", 12)
        chart.setdefault("bins", 16)
        chart.setdefault("max_points", 600)
        ui["chart"] = chart

    return data


def delete_project(instance_dir: str, project_id: str) -> None:
    pdir = project_dir(instance_dir, project_id)
    if os.path.isdir(pdir):
        shutil.rmtree(pdir)


def duplicate_project(instance_dir: str, project_id: str) -> Project:
    src_dir = project_dir(instance_dir, project_id)
    if not os.path.isdir(src_dir):
        raise FileNotFoundError(project_id)

    old = load_project(instance_dir, project_id)
    new = create_project(instance_dir)

    # Copy uploads (if present)
    src_uploads = project_uploads_dir(instance_dir, project_id)
    dst_uploads = project_uploads_dir(instance_dir, new.id)
    if os.path.isdir(src_uploads):
        ensure_dir(dst_uploads)
        for name in os.listdir(src_uploads):
            sp = os.path.join(src_uploads, name)
            dp = os.path.join(dst_uploads, name)
            if os.path.isfile(sp):
                shutil.copy2(sp, dp)

    new.data = normalize_project_data(dict(old.data))
    new.data["id"] = new.id
    new.data["created_at_unix"] = int(time.time())
    new.data["updated_at_unix"] = int(time.time())
    new.data.setdefault("app", {})
    new.data["app"]["name"] = (new.data["app"].get("name") or "Simple Dataiku WebApp") + " (Copy)"
    save_project(instance_dir, new)
    return new


def list_projects(instance_dir: str) -> List[Project]:
    root = projects_root(instance_dir)
    if not os.path.isdir(root):
        return []
    out: List[Project] = []
    for name in sorted(os.listdir(root)):
        pdir = os.path.join(root, name)
        if not os.path.isdir(pdir):
            continue
        pjson = os.path.join(pdir, "project.json")
        if not os.path.isfile(pjson):
            continue
        try:
            out.append(load_project(instance_dir, name))
        except Exception:
            continue
    return out
