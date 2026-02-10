import json
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


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
        "app": {
            "name": "Simple Dataiku WebApp",
            "subtitle": "",
        },
        "dataiku": {
            "dataset_a": "dataset_a",
            "dataset_b": "",
            "dataset_c": "",
        },
        "csv": {
            "a": None,
            "b": None,
            "c": None,
        },
        "transform": {
            # Backward-compat fields (kept in sync with joins[0] when possible).
            "join_enabled": False,
            "join": {"how": "left", "keys": [{"a": "", "b": ""}]},
            # Canonical multi-join support: sequentially join the current dataframe to Dataset B and then Dataset C.
            # Each key pair maps a left-column name (current dataframe) to a right-column name (right dataset).
            "joins": [
                {"right": "b", "enabled": False, "how": "left", "keys": [{"left": "", "right": ""}]},
                {"right": "c", "enabled": False, "how": "left", "keys": [{"left": "", "right": ""}]},
            ],
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
            "template": "table",  # table|sidebar_filters|master_detail|chart_table|two_tables
            "frontend_filters": [],
            "pagination": False,
            "page_size": 200,
            "chart": {"enabled": False, "column": "", "top_n": 12},
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

    app = data.setdefault("app", {})
    app.setdefault("name", "Simple Dataiku WebApp")
    app.setdefault("subtitle", "")

    dku = data.setdefault("dataiku", {})
    # Older projects used source_dataset/audit_dataset; map source_dataset to dataset_a.
    dku.setdefault("dataset_a", dku.get("source_dataset") or "dataset_a")
    dku.setdefault("dataset_b", "")
    dku.setdefault("dataset_c", "")

    csv = data.setdefault("csv", {})
    # Older projects stored csv.source/csv.audit; map source -> a.
    if "a" not in csv:
        csv["a"] = csv.get("source") or None
    csv.setdefault("b", None)
    csv.setdefault("c", None)

    transform = data.setdefault("transform", {})
    transform.setdefault("join_enabled", False)
    join = transform.setdefault("join", {"how": "left", "keys": [{"a": "", "b": ""}]})
    # Migrate old single-key join fields if present.
    if isinstance(join, dict) and ("left_on" in join or "right_on" in join):
        left_on = str(join.get("left_on") or "").strip()
        right_on = str(join.get("right_on") or "").strip()
        join.pop("left_on", None)
        join.pop("right_on", None)
        join.setdefault("keys", [{"a": left_on, "b": right_on}])
    join.setdefault("how", "left")
    join.setdefault("keys", [{"a": "", "b": ""}])

    # Canonical multi-join config: transform.joins
    joins = transform.get("joins")
    if not isinstance(joins, list):
        joins = []

    # Build joins from legacy join fields if missing/empty.
    if not joins:
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
            {
                "right": "b",
                "enabled": bool(transform.get("join_enabled")),
                "how": str(join.get("how") or "left"),
                "keys": pairs,
            },
            {"right": "c", "enabled": False, "how": "left", "keys": [{"left": "", "right": ""}]},
        ]

    # Normalize join steps, ensure we have b and c entries.
    def _norm_step(step: Dict[str, Any], *, right: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        out["right"] = right
        out["enabled"] = bool(step.get("enabled"))
        out["how"] = str(step.get("how") or "left").strip().lower()
        keys = step.get("keys") if isinstance(step.get("keys"), list) else []
        kp: List[Dict[str, str]] = []
        for k in keys:
            if not isinstance(k, dict):
                continue
            left = str(k.get("left") or "").strip()
            rightk = str(k.get("right") or "").strip()
            if left or rightk:
                kp.append({"left": left, "right": rightk})
        if not kp:
            kp = [{"left": "", "right": ""}]
        out["keys"] = kp
        return out

    by_right: Dict[str, Dict[str, Any]] = {}
    for s in joins:
        if not isinstance(s, dict):
            continue
        r = str(s.get("right") or "").strip().lower()
        if r in {"b", "c"} and r not in by_right:
            by_right[r] = s
    joins_norm = [
        _norm_step(by_right.get("b") or {}, right="b"),
        _norm_step(by_right.get("c") or {}, right="c"),
    ]
    transform["joins"] = joins_norm

    # Keep backward compat fields in sync with join step B.
    jb = joins_norm[0]
    transform["join_enabled"] = bool(jb.get("enabled"))
    transform.setdefault("join", {})
    transform["join"]["how"] = str(jb.get("how") or "left")
    transform["join"]["keys"] = [
        {"a": str(k.get("left") or ""), "b": str(k.get("right") or "")}
        for k in (jb.get("keys") or [])
        if isinstance(k, dict)
    ] or [{"a": "", "b": ""}]

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
    ui.setdefault("chart", {"enabled": False, "column": "", "top_n": 12})

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
