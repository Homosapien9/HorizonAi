# Horizon v5

Horizon v5 is a FastAPI application that builds evidence-backed technical career roadmaps. It combines live career-intelligence sources, curated fallback data, a roadmap graph, and PPP-adjusted salary estimates to generate a personalized study plan for roles such as software engineer, ML engineer, data scientist, DevOps engineer, cybersecurity analyst, and other technical tracks.

## Features

- Personalized roadmap generation with phased learning plans
- Progress-aware generation flow with loading and final roadmap pages
- Live career-intelligence endpoints for jobs, internships, trend signals, scholarships, and universities
- JD scan and resume-skill gap analysis
- SQLite-backed roadmap and progress APIs
- PPP-adjusted salary localization for multiple countries

## Project layout

- `main.py` — FastAPI app, page routes, generation flow, health and search endpoints
- `roadmap_engine.py` — roadmap generation, track logic, salary bands, progress updates
- `career_intelligence_pipeline.py` — live source aggregation and curated technical fallbacks
- `models.py` — Pydantic models, SQLite schema, and utility helpers
- `api/roadmaps.py` — roadmap graph, search, and progress APIs
- `templates/` — Jinja2 pages for landing, loading, roadmap, and error views
- `static/` — CSS and frontend JavaScript
- `tests/` — smoke and endpoint tests

## Requirements

- Python 3.11+
- `pip` for installing dependencies

Install dependencies:

```bash
pip install -r requirements.txt
```

## Running locally

Start the development server:

```bash
python3 -m uvicorn main:app --reload
```

Default URLs:

- App: `http://127.0.0.1:8000/`
- Swagger UI: `http://127.0.0.1:8000/docs`
- Health check: `http://127.0.0.1:8000/health`

## Main user flow

1. Open `/`
2. Submit a roadmap request with goal, level, country, and weekly study hours
3. The app creates a background job and sends the browser to `/loading/{job_id}`
4. The loading page polls `/api/job/{job_id}` until the roadmap is complete
5. The final roadmap renders at `/roadmap/{job_id}`

## API summary

### Core app endpoints

- `GET /` — homepage and roadmap form
- `GET /loading/{job_id}` — roadmap generation progress page
- `GET /roadmap/{job_id}` — rendered roadmap page
- `POST /generate` — create a roadmap generation job
- `GET /api/job/{job_id}` — job status and embedded result when complete
- `GET /api/roadmap/{job_id}` — roadmap JSON payload for a generated job
- `GET /health` — health and uptime metadata
- `GET /docs` — FastAPI Swagger UI

### Career intelligence endpoints

- `GET|POST /api/career-intelligence`
- `GET /api/live-search`
- `GET /api/jobs`
- `GET /api/internships`
- `GET /api/skills`
- `POST /api/jd-scan`
- `POST /api/resume`
- `POST /api/compare`

### Roadmap graph APIs

- `GET /api/roadmaps`
- `GET /api/roadmaps/{roadmap_id}`
- `GET /api/roadmaps/{roadmap_id}/nodes/{node_id}`
- `GET /api/search-nodes`
- `POST /api/generate-learning-path`
- `POST /api/progress/{user_id}/nodes/{node_id}/complete`
- `GET /api/progress/{user_id}/roadmap/{roadmap_id}`

## Optional external data sources

The application works without API keys using free/public sources and curated fallbacks. If configured, it can also enrich results with:

- Adzuna (`ADZUNA_APP_ID`, `ADZUNA_APP_KEY`)
- USAJobs (`USAJOBS_API_KEY`, `USAJOBS_USER_AGENT`)

## Testing and linting

Run tests:

```bash
pytest -q
```

Run lint checks:

```bash
ruff check .
```

## Notes

- `horizon.db` is created automatically on startup by `init_db()`.
- Cached or generated roadmap results are stored in SQLite.
- The app is designed to degrade gracefully when live sources time out.
