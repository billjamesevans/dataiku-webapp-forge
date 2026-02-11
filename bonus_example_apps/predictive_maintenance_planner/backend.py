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
        "name": "Predictive Maintenance Planner",
        "subtitle": "Prioritize maintenance interventions before production impact",
    },
    "dataiku": {
        "sensor_dataset": "mf_sensor_readings",
        "work_orders_dataset": "mf_maintenance_work_orders",
        "parts_dataset": "mf_spare_parts_inventory",
    },
    "local_sources": {
        "sensor_csv": "sample_data/sensor_readings.csv",
        "work_orders_csv": "sample_data/maintenance_work_orders.csv",
        "parts_csv": "sample_data/spare_parts_inventory.csv",
    },
    "business_rules": {
        "downtime_cost_per_hour": 2750,
        "target_labor_hours_14d": 260,
    },
}


def _load_config() -> dict:
    for fname in ("config.json", "app_config.json"):
        path = os.path.join(BASE_DIR, fname)
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as stream:
                parsed = json.load(stream)
            if isinstance(parsed, dict):
                return parsed
    return DEFAULT_CONFIG


CONFIG = _load_config()


def _load_df(dataset_name: str, csv_key: str) -> pd.DataFrame:
    if HAS_DATAIKU:
        try:
            return dataiku.Dataset(dataset_name).get_dataframe(parse_dates=False)
        except Exception:
            pass

    rel = (CONFIG.get("local_sources") or {}).get(csv_key)
    if not rel:
        raise ValueError(f"Missing source key: {csv_key}")
    path = os.path.join(BASE_DIR, rel)
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    return pd.read_csv(path)


def _to_float(val) -> float:
    try:
        return float(val)
    except Exception:
        return 0.0


def _windowed_data(days: int):
    days = max(3, min(days, 90))

    datasets = CONFIG.get("dataiku") or {}
    sensor = _load_df(str(datasets.get("sensor_dataset") or "mf_sensor_readings"), "sensor_csv")
    work_orders = _load_df(str(datasets.get("work_orders_dataset") or "mf_maintenance_work_orders"), "work_orders_csv")
    parts = _load_df(str(datasets.get("parts_dataset") or "mf_spare_parts_inventory"), "parts_csv")

    sensor["timestamp"] = pd.to_datetime(sensor["timestamp"], errors="coerce", utc=True)
    work_orders["opened_date"] = pd.to_datetime(work_orders["opened_date"], errors="coerce", utc=True)
    work_orders["due_date"] = pd.to_datetime(work_orders["due_date"], errors="coerce", utc=True)

    max_ts = sensor["timestamp"].max()
    if pd.isna(max_ts):
        max_ts = datetime.now(timezone.utc)
    cutoff = max_ts - timedelta(days=days)

    sensor = sensor[sensor["timestamp"] >= cutoff].copy()
    return sensor, work_orders, parts


def _risk_from_latest(sensor: pd.DataFrame) -> pd.DataFrame:
    if sensor.empty:
        return pd.DataFrame(columns=["machine_id", "line", "machine_family", "risk_score", "rul_days"])

    idx = sensor.groupby("machine_id")["timestamp"].idxmax()
    latest = sensor.loc[idx].copy()

    temp = pd.to_numeric(latest["temperature_c"], errors="coerce").fillna(0)
    vib = pd.to_numeric(latest["vibration_mm_s"], errors="coerce").fillna(0)
    pressure = pd.to_numeric(latest["pressure_bar"], errors="coerce").fillna(0)
    current = pd.to_numeric(latest["current_amp"], errors="coerce").fillna(0)
    rul = pd.to_numeric(latest["rul_days"], errors="coerce").fillna(180)
    anomaly = pd.to_numeric(latest["anomaly_score"], errors="coerce").fillna(0)

    temp_n = ((temp - 72.0) / 20.0).clip(0, 1)
    vib_n = ((vib - 2.8) / 5.0).clip(0, 1)
    pressure_n = ((pressure - 8.0).abs() / 4.0).clip(0, 1)
    current_n = ((current - 110.0) / 70.0).clip(0, 1)
    rul_n = ((21.0 - rul) / 21.0).clip(0, 1)
    anomaly_n = (anomaly / 100.0).clip(0, 1)

    latest["risk_score"] = (
        temp_n * 0.20 + vib_n * 0.28 + pressure_n * 0.12 + current_n * 0.15 + rul_n * 0.17 + anomaly_n * 0.08
    ) * 100.0

    latest["risk_score"] = latest["risk_score"].clip(0, 100)

    return latest[["machine_id", "line", "machine_family", "risk_score", "rul_days"]].sort_values(
        "risk_score", ascending=False
    )


def _open_work_orders(work_orders: pd.DataFrame) -> pd.DataFrame:
    open_status = {"OPEN", "IN_PROGRESS", "PLANNED"}
    return work_orders[work_orders["status"].isin(open_status)].copy()


def _schedule_recommendations(risk_df: pd.DataFrame, work_orders_open: pd.DataFrame) -> list:
    if risk_df.empty:
        return []

    has_open = set(work_orders_open["machine_id"].astype(str).tolist())
    now = datetime.now(timezone.utc).date()

    recs = []
    for _, row in risk_df.head(12).iterrows():
        machine = str(row["machine_id"])
        score = _to_float(row["risk_score"])
        if score < 60 and machine in has_open:
            continue

        if score >= 82:
            bucket = "now"
            priority = "Critical"
            target = now + timedelta(days=1)
        elif score >= 68:
            bucket = "week"
            priority = "High"
            target = now + timedelta(days=4)
        else:
            bucket = "next"
            priority = "Medium"
            target = now + timedelta(days=9)

        action = "Bearing inspection + vibration rebalance" if score >= 72 else "Condition-based PM verification"
        team = "Reliability" if score >= 75 else "Mechanical"

        recs.append(
            {
                "machine_id": machine,
                "line": str(row["line"]),
                "priority": priority,
                "bucket": bucket,
                "target_date": target.strftime("%Y-%m-%d"),
                "action": action,
                "team": team,
                "risk_score": round(score, 1),
            }
        )

    recs.sort(key=lambda x: (0 if x["priority"] == "Critical" else 1 if x["priority"] == "High" else 2, -x["risk_score"]))
    return recs


def _parts_exposure(parts: pd.DataFrame, risk_df: pd.DataFrame) -> list:
    if parts.empty:
        return []

    high_risk_families = set(risk_df[risk_df["risk_score"] >= 68]["machine_family"].astype(str).tolist())

    out = []
    for _, row in parts.iterrows():
        family = str(row.get("machine_family") or "")
        if high_risk_families and family not in high_risk_families:
            continue

        on_hand = _to_float(row.get("on_hand"))
        reorder = _to_float(row.get("reorder_point"))
        lead = _to_float(row.get("lead_time_days"))
        daily = max(0.3, _to_float(row.get("daily_usage")))
        coverage = on_hand / daily

        if on_hand <= reorder * 0.7:
            risk = "Critical"
        elif on_hand <= reorder:
            risk = "High"
        elif coverage <= lead * 1.3:
            risk = "Medium"
        else:
            risk = "Low"

        if risk == "Low":
            continue

        out.append(
            {
                "part_name": str(row.get("part_name") or row.get("part_id") or ""),
                "machine_family": family,
                "on_hand": int(on_hand),
                "reorder_point": int(reorder),
                "lead_time_days": int(lead),
                "coverage_days": int(round(coverage)),
                "risk_level": risk,
                "unit_cost": _to_float(row.get("unit_cost")),
            }
        )

    order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
    out.sort(key=lambda x: (order.get(x["risk_level"], 9), x["coverage_days"]))
    return out[:16]


def _summary(risk_df: pd.DataFrame, work_orders_open: pd.DataFrame, parts_exposure: list) -> dict:
    rules = CONFIG.get("business_rules") or {}
    downtime_cost = _to_float(rules.get("downtime_cost_per_hour") or 2750)
    target_hours_14d = max(1.0, _to_float(rules.get("target_labor_hours_14d") or 260))

    high_risk = int((risk_df["risk_score"] >= 75).sum()) if not risk_df.empty else 0
    pred_fail = int((risk_df["risk_score"] >= 70).sum() * 0.6 + (risk_df["rul_days"] <= 14).sum() * 0.4) if not risk_df.empty else 0

    wo_hours = pd.to_numeric(work_orders_open["estimated_hours"], errors="coerce").fillna(0)
    scheduled_hours = float(wo_hours.sum())

    exposure_value = 0.0
    for item in parts_exposure:
        multiplier = 2 if item["risk_level"] == "Critical" else 1
        exposure_value += item["unit_cost"] * max(1, item["reorder_point"] - item["on_hand"]) * multiplier

    due_soon = int((work_orders_open["due_date"] <= (datetime.now(timezone.utc) + timedelta(days=7))).sum()) if not work_orders_open.empty else 0
    backlog_risk = (due_soon / max(1, len(work_orders_open))) * 100.0 if len(work_orders_open) else 0.0

    preventable_hours = max(0.0, (risk_df["risk_score"].clip(lower=60).sum() / 100.0) * 1.8 if not risk_df.empty else 0.0)
    projected_savings = preventable_hours * downtime_cost

    return {
        "machines_high_risk": high_risk,
        "predicted_failures_30d": int(round(pred_fail)),
        "scheduled_hours_14d": int(round(scheduled_hours)),
        "parts_exposure_value": int(round(exposure_value)),
        "projected_savings_usd": int(round(projected_savings)),
        "backlog_sla_risk_pct": backlog_risk,
        "labor_load_pct_of_target": (scheduled_hours / target_hours_14d) * 100.0,
    }


def _work_order_records(work_orders_open: pd.DataFrame) -> list:
    if work_orders_open.empty:
        return []

    open_sorted = work_orders_open.sort_values(["due_date", "priority"], ascending=[True, True]).head(30)
    records = []
    for _, row in open_sorted.iterrows():
        due = row.get("due_date")
        due_txt = pd.to_datetime(due, errors="coerce", utc=True)
        records.append(
            {
                "wo_id": str(row.get("wo_id") or ""),
                "machine_id": str(row.get("machine_id") or ""),
                "priority": str(row.get("priority") or "Medium"),
                "status": str(row.get("status") or "OPEN"),
                "due_date": due_txt.strftime("%Y-%m-%d") if not pd.isna(due_txt) else "",
                "estimated_hours": int(_to_float(row.get("estimated_hours"))),
                "failure_mode": str(row.get("failure_mode") or ""),
                "assigned_team": str(row.get("assigned_team") or ""),
            }
        )
    return records


def _machine_trend(sensor: pd.DataFrame, machine_id: str) -> list:
    if not machine_id:
        return []

    scoped = sensor[sensor["machine_id"].astype(str) == machine_id].copy()
    if scoped.empty:
        return []

    scoped["day"] = scoped["timestamp"].dt.strftime("%Y-%m-%d")

    grouped = scoped.groupby("day", as_index=False).agg(
        temperature_c=("temperature_c", "mean"),
        vibration_mm_s=("vibration_mm_s", "mean"),
        anomaly_score=("anomaly_score", "mean"),
        rul_days=("rul_days", "mean"),
    )

    points = []
    for _, row in grouped.sort_values("day").iterrows():
        temp_n = max(0.0, min(1.0, (_to_float(row["temperature_c"]) - 72.0) / 20.0))
        vib_n = max(0.0, min(1.0, (_to_float(row["vibration_mm_s"]) - 2.8) / 5.0))
        rul_n = max(0.0, min(1.0, (21.0 - _to_float(row["rul_days"])) / 21.0))
        anomaly_n = max(0.0, min(1.0, _to_float(row["anomaly_score"]) / 100.0))

        risk = (temp_n * 0.35 + vib_n * 0.35 + rul_n * 0.20 + anomaly_n * 0.10) * 100.0
        points.append({"day": str(row["day"]), "risk_score": round(risk, 1)})

    return points


@app.route("/dashboard", methods=["GET"])
def dashboard():
    try:
        days = int(request.args.get("days") or 14)
        sensor, work_orders, parts = _windowed_data(days)

        risk_df = _risk_from_latest(sensor)
        open_orders = _open_work_orders(work_orders)
        schedule = _schedule_recommendations(risk_df, open_orders)
        exposure = _parts_exposure(parts, risk_df)

        payload = {
            "status": "ok",
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "summary": _summary(risk_df, open_orders, exposure),
            "machine_risk": [
                {
                    "machine_id": str(row["machine_id"]),
                    "line": str(row["line"]),
                    "machine_family": str(row["machine_family"]),
                    "risk_score": round(_to_float(row["risk_score"]), 1),
                    "rul_days": round(_to_float(row["rul_days"]), 1),
                }
                for _, row in risk_df.head(24).iterrows()
            ],
            "schedule": schedule,
            "parts_exposure": exposure,
            "open_work_orders": _work_order_records(open_orders),
        }
        return jsonify(payload)
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/machine-trend", methods=["GET"])
def machine_trend():
    try:
        machine_id = str(request.args.get("machine_id") or "").strip()
        days = int(request.args.get("days") or 14)
        sensor, _, _ = _windowed_data(days)
        return jsonify({"status": "ok", "machine_id": machine_id, "points": _machine_trend(sensor, machine_id)})
    except Exception as exc:
        return jsonify({"status": "error", "message": str(exc)}), 500
