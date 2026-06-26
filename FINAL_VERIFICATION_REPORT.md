# Horizon v5 Final Verification Report

## Installation and startup

```text
2026-06-15 05:53:02,130 - api.roadmaps - INFO - Registering roadmap API endpoints
2026-06-15 05:53:02,136 - api.roadmaps - INFO - Roadmap API endpoints registered successfully
INFO:     Started server process [3112]
INFO:     Waiting for application startup.
2026-06-15 05:53:02,155 - main - INFO - Starting Horizon 5.0.0
2026-06-15 05:53:02,157 - models - INFO - Database initialized at /home/user/work/horizon_v5.4/horizon_v5/horizon.db
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8003 (Press CTRL+C to quit)
```

## Health and page endpoints

- /health → 200 with numeric uptime_seconds=0.7
- / → 200
- /search → 200
- /compare → 200
- /career-intelligence → 200

## Roadmap generation

- /generate returned job_id `37c50d57adb7493fa9577e74adfdf9a1`
- Job status reached `complete`
- /roadmap/{job_id} → 200
- Sample top skill in generated roadmap: Python

## Career intelligence query checks

| Query | Jobs | Skills | Data sources | Partial | Notice |
|---|---:|---:|---:|---|---|
| Software Engineer | 1 | 11 | 5 | False |  |
| Data Scientist | 1 | 12 | 5 | False |  |
| Mechanical Engineer | 1 | 12 | 5 | False |  |
| Civil Engineer | 1 | 12 | 5 | False |  |
| Cybersecurity Analyst | 6 | 6 | 4 | False |  |
| Electrical Engineer | 6 | 6 | 4 | False |  |
| Nurse | 0 | 0 | 1 | True | technical-only fallback |
| asdkjhaskjdh | 0 | 0 | 1 | True | technical-only fallback |

## Other API endpoints

- /api/jobs → total=1
- /api/live-search → total_jobs=1
- /api/internships → count=2
- /api/jd-scan → required_skills=['docker', 'kubernetes', 'python', 'sql']
- /api/resume → resume_skills=['docker', 'git', 'python', 'sql']
- /api/compare → tracks=['Machine Learning Engineer', 'Data Scientist']
- /api/skills → suggestions=['Python', 'Pytorch']

## Automated checks

```text
============================= test session starts ==============================
platform linux -- Python 3.13.13, pytest-8.3.5, pluggy-1.6.0
rootdir: /home/user/work/horizon_v5.4/horizon_v5
configfile: pytest.ini
testpaths: tests
plugins: anyio-4.13.0, asyncio-0.26.0, respx-0.23.1
asyncio: mode=Mode.AUTO, asyncio_default_fixture_loop_scope=function, asyncio_default_test_loop_scope=function
collected 6 items

tests/test_app.py ......                                                 [100%]

============================== 6 passed in 0.41s ===============================
```

```text
All checks passed!
```

## Endpoint/source matrix

| Endpoint | Sources | API key required | Tested |
|---|---|---|---|
| /api/career-intelligence | Arbeitnow, RemoteOK, Wikipedia REST, ESCO, GitHub REST, Hacker News Algolia, arXiv, curated technical fallback | No by default; optional Adzuna/USAJobs env vars | Yes |
| /api/jobs | Same free job-source stack as career-intelligence | No by default; optional Adzuna/USAJobs env vars | Yes |
| /api/live-search | Same free stack plus curated salaries, universities, scholarships, internships | No | Yes |
| /api/internships | Curated technical internships | No | Yes |
| /api/jd-scan | Local skill extraction against roadmap graph | No | Yes |
| /api/resume | Local skill extraction against roadmap graph | No | Yes |
| /api/compare | Local roadmap comparison | No | Yes |
| /api/skills | Local technical-skill autocomplete | No | Yes |
| /generate and /roadmap/{job_id} | Free market data sources + roadmap graph + curated fallbacks | No by default; optional Adzuna/USAJobs env vars | Yes |
