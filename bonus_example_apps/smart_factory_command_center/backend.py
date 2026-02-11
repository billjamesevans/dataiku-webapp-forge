import json
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
from flask import Flask, jsonify, request

try:
    import dataiku  # type: ignore

    HAS_DATAIKU = True
except Exception:
    HAS_DATAIKU = False

app = Flask(__name__)

BASE_DIR = os.path.dirname(__file__) if "__file__" in globals() else os.getcwd()

DEFAULT_CONFIG = {
    "app": {
        "name": "Smart Factory Command Center",
        "subtitle": "Throughput, quality, and machine health in one cockpit",
    },
    "dataiku": {
        "production_dataset": "mf_production_events",
        "quality_dataset": "mf_quality_events",
        "telemetry_dataset": "mf_machine_telemetry",
    },
    "local_sources": {
        "production_csv": "sample_data/production_log.csv",
        "quality_csv": "sample_data/quality_events.csv",
        "telemetry_csv": "sample_data/machine_telemetry.csv",
    },
}


def _load_config() -> dict:
    for fname in ("config.json", "app_config.json"):
        path = os.path.join(BASE_DIR, fname)
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as stream:
                data = json.load(stream)
            if isinstance(data, dict):
                return data
    return DEFAULT_CONFIG


CONFIG = _load_config()


def _source_path(key: str) -> str:
    rel = (CONFIG.get("local_sources") or {}).get(key)
    if not rel:
        raise ValueError(f"Missing local source for {key}")
    return os.path.join(BASE_DIR, rel)


def _load_df(dataset_name: str, csv_key: str) -> pd.DataFrame:
    if HAS_DATAIKU:
        try:
            return dataiku.Dataset(dataset_name).get_dataframe(parse_dates=False)
        except Exception:
            pass

    path = _source_path(csv_key)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Source CSV not found: {path}")
    return pd.read_csv(path)


def _safe_pct(num: float, den: float) -> float:
    if not den:
        return 0.0
    return float(num) / float(den) * 100.0


def _num(value) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _load_window(days: int):
    days = max(3, min(days, 90))

    datasets = CONFIG.get("dataiku") or {}
    production = _load_df(str(datasets.get("production_dataset") or "mf_production_events"), "production_csv")
    quality = _load_df(str(datasets.get("quality_dataset") or "mf_quality_events"), "quality_csv")
    telemetry = _load_df(str(datasets.get("telemetry_dataset") or "mf_machine_telemetry"), "telemetry_csv")

    production["timestamp"] = pd.to_datetime(production["timestamp"], errors="coerce", utc=True)
    quality["timestamp"] = pd.to_datetime(quality["timestamp"], errors="coerce", utc=True)
    telemetry["timestamp"] = pd.to_datetime(telemetry["timestamp"], errors="coerce", utc=True)

    max_ts = production["timestamp"].max()
    if pd.isna(max_ts):
        max_ts = datetime.now(timezone.utc)
    cutoff = max_ts - timedelta(days=days)

    production = production[production["timestamp"] >= cutoff].copy()
    quality = quality[quality["timestamp"] >= cutoff].copy()
    telemetry = telemetry[telemetry["timestamp"] >= cutoff].copy()

    return production, quality, telemetry


def _build_summary(production: pd.DataFrame) -> dict:
    produced = production["units_produced"].sum()
    good = production["good_units"].sum()
    scrap = production["scrap_units"].sum()
    downtime_min = production["downtime_minutes"].sum()
    planned_runtime = production["planned_runtime_minutes"].sum()
    planned_units = production["planned_units"].sum()
    total_energy = production["energy_kwh"].sum()
    on_time = production["orders_on_time"].sum()
    orders_total = production["orders_total"].sum()

    availability = max(0.0, 1.0 - (downtime_min / planned_runtime if planned_runtime else 0.0))
    performance = produced / planned_units if planned_units else 0.0
    quality = good / produced if produced else 0.0

    oee = max(0.0, min(1.0, availability * performance * quality))

    return {
        "oee_pct": oee * 100.0,
        "total_good_units": int(good),
        "total_scrap_units": int(scrap),
        "scrap_rate_pct": _safe_pct(scrap, produced),
        "total_downtime_minutes": int(downtime_min),
        "downtime_hours": downtime_min / 60.0,
        "energy_kwh_per_good_unit": (total_energy / good) if good else 0.0,
        "otif_pct": _safe_pct(on_time, orders_total),
    }


def _trend(production: pd.DataFrame) -> list:
    if production.empty:
        return []

    production["day"] = production["timestamp"].dt.strftime("%Y-%m-%d")
    grouped = production.groupby("day", as_index=False).agg(
        good_units=("good_units", "sum"),
        units_produced=("units_produced", "sum"),
        scrap_units=("scrap_units", "sum"),
        downtime_minutes=("downtime_minutes", "sum"),
        planned_runtime_minutes=("planned_runtime_minutes", "sum"),
        planned_units=("planned_units", "sum"),
    )

    rows = []
    for _, row in grouped.sort_values("day").iterrows():
        availability = max(0.0, 1.0 - _num(row["downtime_minutes"]) / max(1.0, _num(row["planned_runtime_minutes"])))
        performance = _num(row["units_produced"]) / max(1.0, _num(row["planned_units"]))
        quality = _num(row["good_units"]) / max(1.0, _num(row["units_produced"]))
        rows.append(
            {
                "day": str(row["day"]),
                "good_units": int(_num(row["good_units"])),
                "downtime_minutes": int(_num(row["downtime_minutes"])),
                "scrap_rate_pct": _safe_pct(_num(row["scrap_units"]), _num(row["units_produced"])),
                "oee_pct": max(0.0, min(100.0, availability * performance * quality * 100.0)),
            }
        )
    return rows


def _line_breakdown(production: pd.DataFrame) -> list:
    if production.empty:
        return []

    grouped = production.groupby("line", as_index=False).agg(
        good_units=("good_units", "sum"),
        units_produced=("units_produced", "sum"),
        scrap_units=("scrap_units", "sum"),
        downtime_minutes=("downtime_minutes", "sum"),
        planned_runtime_minutes=("planned_runtime_minutes", "sum"),
        planned_units=("planned_units", "sum"),
    )

    out = []
    for _, row in grouped.sort_values("good_units", ascending=False).iterrows():
        availability = max(0.0, 1.0 - _num(row["downtime_minutes"]) / max(1.0, _num(row["planned_runtime_minutes"])))
        performance = _num(row["units_produced"]) / max(1.0, _num(row["planned_units"]))
        quality = _num(row["good_units"]) / max(1.0, _num(row["units_produced"]))
        out.append(
            {
                "line": str(row["line"]),
                "good_units": int(_num(row["good_units"])),
                "downtime_minutes": int(_num(row["downtime_minutes"])),
                "scrap_rate_pct": _safe_pct(_num(row["scrap_units"]), _num(row["units_produced"])),
                "oee_pct": max(0.0, min(100.0, availability * performance * quality * 100.0)),
            }
        )
    return out


def _defect_pareto(quality: pd.DataFrame) -> list:
    if quality.empty:
        return []

    grouped = (
        quality.groupby("defect_type", as_index=False)["units_impacted"]
        .sum()
        .sort_values("units_impacted", ascending=False)
        .head(7)
    )

    return [
        {"defect_type": str(row["defect_type"]), "units_impacted": int(_num(row["units_impacted"]))}
        for _, row in grouped.iterrows()
    ]


def _alerts(telemetry: pd.DataFrame) -> list:
    if telemetry.empty:
        return []

    latest = telemetry.sort_values("timestamp", ascending=False).head(120)
    alert_rows = latest[
        (latest["status"].isin(["DOWN", "ALARM"]))
        | (pd.to_numeric(latest["temperature_c"], errors="coerce") > 84)
        | (pd.to_numeric(latest["vibration_mm_s"], errors="coerce") > 7.2)
    ]

    alerts = []
    for _, row in alert_rows.head(10).iterrows():
        temp = _num(row.get("temperature_c"))
        vib = _num(row.get("vibration_mm_s"))
        status = str(row.get("status") or "")

        reasons = []
        if status in {"DOWN", "ALARM"}:
            reasons.append(status)
        if temp > 84:
            reasons.append("high temperature")
        if vib > 7.2:
            reasons.append("high vibration")

        severity = "high" if status in {"DOWN", "ALARM"} or temp > 88 or vib > 8.0 else "medium"

        alerts.append(
            {
                "timestamp": pd.to_datetime(row["timestamp"], utc=True).strftime("%Y-%m-%d %H:%M"),
                "machine_id": str(row.get("machine_id") or ""),
                "line": str(row.get("line") or ""),
                "temperature_c": temp,
                "vibration_mm_s": vib,
                "reason": ", ".join(reasons) or "attention required",
                "severity": severity,
            }
        )

    return alerts


def _telemetry_rows(telemetry: pd.DataFrame, limit: int = 25) -> list:
    cols = ["timestamp", "machine_id", "line", "status", "temperature_c", "vibration_mm_s", "power_kw", "operator"]
    telemetry = telemetry.sort_values("timestamp", ascending=False).head(limit)
    out = []
    for _, row in telemetry.iterrows():
        out.append(
            {
                "timestamp": pd.to_datetime(row["timestamp"], utc=True).strftime("%Y-%m-%d %H:%M"),
                "machine_id": str(row.get("machine_id") or ""),
                "line": str(row.get("line") or ""),
                "status": str(row.get("status") or "UNKNOWN"),
                "temperature_c": _num(row.get("temperature_c")),
                "vibration_mm_s": _num(row.get("vibration_mm_s")),
                "power_kw": _num(row.get("power_kw")),
                "operator": str(row.get("operator") or ""),
            }
        )
    return out


@app.route("/bootstrap", methods=["GET"])
def bootstrap():
    try:
        days = int(request.args.get("days") or 14)
        production, quality, telemetry = _load_window(days)

        payload = {
            "status": "ok",
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "summary": _build_summary(production),
            "trend": _trend(production),
            "line_breakdown": _line_breakdown(production),
            "defect_pareto": _defect_pareto(quality),
            "alerts": _alerts(telemetry),
            "rows_in_scope": {
                "production": int(len(production)),
                "quality": int(len(quality)),
                "telemetry": int(len(telemetry)),
            },
        }
        return jsonify(payload)
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/live-feed", methods=["GET"])
def live_feed():
    try:
        days = int(request.args.get("days") or 14)
        _, _, telemetry = _load_window(days)
        return jsonify({"status": "ok", "rows": _telemetry_rows(telemetry, 25)})
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500
