# Smart Factory Command Center

Dataiku Standard WebApp example for manufacturing operations leadership.

## Files

- `index.html` (body-only)
- `style.css`
- `script.js`
- `backend.py`
- `config.json`
- `sample_data/*.csv` fake manufacturing source data

## Dataiku dataset mapping

Map your Dataiku datasets (or import the sample CSV files) to these names:

- `mf_production_events`
- `mf_quality_events`
- `mf_machine_telemetry`

The backend will use local `sample_data` CSV files if Dataiku datasets are unavailable (useful for local preview/testing).

## Endpoints

- `/bootstrap?days=14` summary + trend + line breakdown + defects + alerts
- `/live-feed` latest telemetry records
