import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


def _slug(value: str) -> str:
    v = (value or "").strip().lower()
    v = re.sub(r"[^a-z0-9]+", "-", v).strip("-")
    return v or "preset"


def presets_dir(instance_dir: str) -> str:
    return os.path.join(instance_dir, "presets")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def list_presets(instance_dir: str) -> List[Dict[str, Any]]:
    root = presets_dir(instance_dir)
    if not os.path.isdir(root):
        return []
    out: List[Dict[str, Any]] = []
    for name in sorted(os.listdir(root)):
        if not name.endswith(".json"):
            continue
        path = os.path.join(root, name)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            out.append(
                {
                    "id": name[:-5],
                    "name": data.get("name") or name[:-5],
                    "updated_at_unix": data.get("updated_at_unix") or 0,
                }
            )
        except Exception:
            continue
    out.sort(key=lambda x: (-(x.get("updated_at_unix") or 0), x.get("name") or ""))
    return out


def load_preset(instance_dir: str, preset_id: str) -> Dict[str, Any]:
    pid = _slug(preset_id)
    path = os.path.join(presets_dir(instance_dir), pid + ".json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_preset(instance_dir: str, name: str, *, transform: Dict[str, Any], ui: Dict[str, Any]) -> str:
    ensure_dir(presets_dir(instance_dir))
    pid = _slug(name)
    path = os.path.join(presets_dir(instance_dir), pid + ".json")
    data = {
        "id": pid,
        "name": (name or pid).strip() or pid,
        "updated_at_unix": int(time.time()),
        "transform": transform or {},
        "ui": ui or {},
    }
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp, path)
    return pid

