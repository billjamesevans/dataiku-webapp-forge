# Predictive Maintenance Planner

Dataiku Standard WebApp example focused on reliability engineering and proactive maintenance execution.

## Files

- `index.html` (body-only)
- `style.css`
- `script.js`
- `backend.py`
- `config.json`
- `sample_data/*.csv` fake data for sensors, work orders, and spare parts

## Dataiku dataset mapping

Map/import to these datasets:

- `mf_sensor_readings`
- `mf_maintenance_work_orders`
- `mf_spare_parts_inventory`

The backend uses local sample CSV files if Dataiku datasets are unavailable.

## Endpoints

- `/dashboard?days=14` summary KPIs, machine risk, recommendations, parts exposure, open work orders
- `/machine-trend?machine_id=<id>&days=14` trend for selected machine risk trajectory
