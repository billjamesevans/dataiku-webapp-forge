# dataiku-webapp-forge

Local Flask "generator" app that helps teams create simple Dataiku Standard WebApps from CSV exports (schema capture),
with filter/computed columns/templates, and then exports Dataiku-ready:

- `index.html` (body-only for Dataiku Standard WebApps)
- `style.css`, `script.js`
- `backend.py` (Flask backend for Dataiku, reads datasets with pandas)
- `requirements.txt`, `SETUP.md`, `app_config.json`

## Run the generator (local)

```bash
cd .
python3 -m venv .venv
source .venv/bin/activate
pip install -r dataiku_webapp_forge/requirements.txt
python3 -m dataiku_webapp_forge
```

Then open the printed URL in your browser.

## Repo notes

- `instance/` is local state (projects, uploads, presets). It is ignored by git and should not be committed.
