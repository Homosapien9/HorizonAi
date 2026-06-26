# Horizon v5 Fix Progress

- [x] C1 — /loading/{job_id} 404 on generate flow
- [x] H1 — ppp_data.json missing / PPP always 0.65
- [x] H2 — dead roadmap.html / loading.html / error.html templates
- [x] M1 — broken /api/docs nav link
- [x] M2 — unreferenced asyncio.create_task in /generate
- [x] M3 — wrong Adzuna country-code derivation
- [x] L1 — README.md has wrong content
- [x] L2 — stale "Horizon v4" docstring in config.py
- [x] L3 — misc ruff/lint cleanup

## Notes
- C1 — Added `GET /loading/{job_id}` in `main.py`, rewrote `static/progress.js` to poll `/api/job/{job_id}`, and verified the real generate → loading → roadmap flow against a running uvicorn server with no 404s.
- H1 — Added root `ppp_data.json`, hardened PPP normalization/matching in `roadmap_engine.py`, and verified `USA` resolves to `1.0` while `India` resolves to `0.28` during live app runs.
- H2 — Wired `roadmap.html`, `loading.html`, and `error.html` into `main.py`; registered Jinja globals and fixed DAG JSON serialization; verified `/roadmap/{job_id}` renders successfully with the template in tests and runtime.
- M1 — Updated `templates/base.html` to point the API nav link to `/docs`; verified `/docs` returns HTTP 200 in runtime checks.
- M2 — Added module-level background task tracking in `main.py` with automatic cleanup via `add_done_callback`; verified with `verify_targeted_fixes.py` that tracked tasks are retained until completion and then removed.
- M3 — Replaced Adzuna country slicing with an explicit ISO alpha-2 mapping in `career_intelligence_pipeline.py`; verified mappings such as Germany → `de`, USA → `us`, and UK → `gb` in `verify_targeted_fixes.py`.
- L1 — Replaced the root `README.md` pytest-cache placeholder with actual Horizon v5 documentation covering setup, endpoints, and the roadmap flow; verified by file inspection.
- L2 — Updated the `config.py` module docstring from Horizon v4 to Horizon v5; verified by file inspection.
- L3 — Added `api/__init__.py`, switched progress timestamps to UTC ISO strings, added `zip(..., strict=False)`, cleaned several lint/perf issues in `api/roadmaps.py`, and confirmed `ruff check .` passes; `ruff --select ALL` still reports broader legacy style warnings outside the blocking bug list.
