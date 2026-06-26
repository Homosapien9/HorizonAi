from __future__ import annotations

import json
import time

import httpx
import pytest
import respx
from fastapi.testclient import TestClient

import main
import models


@pytest.fixture
def client():
    with TestClient(main.app) as test_client:
        yield test_client


def test_health_endpoint(client: TestClient):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert isinstance(data["uptime_seconds"], (int, float))


def test_skills_autocomplete(client: TestClient):
    response = client.get("/api/skills", params={"q": "py"})
    assert response.status_code == 200
    data = response.json()
    assert "Python" in data["skills"]


def test_compare_endpoint(client: TestClient):
    response = client.post(
        "/api/compare",
        data={
            "goal_a": "ML Engineer",
            "goal_b": "Data Scientist",
            "skill_level": "beginner",
            "weekly_hours": 10,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["skills_shared"]
    assert data["track_a"]
    assert data["track_b"]


def test_jd_scan_endpoint(client: TestClient):
    response = client.post(
        "/api/jd-scan",
        data={
            "jd_text": "Looking for Python, SQL, Docker, Kubernetes and Git experience.",
            "weekly_hours": 10,
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "python" in data["required_skills"]
    assert data["total_weeks_to_close"] >= 1


@respx.mock
def test_career_intelligence_endpoint_mocked(client: TestClient):
    respx.get("https://www.arbeitnow.com/api/job-board-api").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": [
                    {
                        "slug": "data-scientist",
                        "title": "Data Scientist",
                        "company_name": "Acme",
                        "location": "Remote",
                        "description": "Python SQL Machine Learning",
                        "url": "https://example.com/job",
                        "tags": ["Python", "SQL", "Machine Learning"],
                        "job_types": ["Full-time"],
                    }
                ]
            },
        )
    )
    respx.get("https://remoteok.com/api").mock(return_value=httpx.Response(200, json=[]))
    respx.get("https://en.wikipedia.org/api/rest_v1/page/summary/Data_Scientist").mock(
        return_value=httpx.Response(
            200,
            json={
                "title": "Data Scientist",
                "extract": "Data scientists use Python, SQL and statistics.",
                "content_urls": {"desktop": {"page": "https://en.wikipedia.org/wiki/Data_scientist"}},
            },
        )
    )
    respx.get("https://ec.europa.eu/esco/api/search?language=en&type=occupation&text=Data+Scientist&limit=3").mock(
        return_value=httpx.Response(
            200,
            json={"_embedded": {"results": [{"title": "Data scientist", "description": "Python SQL statistics"}]}},
        )
    )
    respx.get("https://api.github.com/search/repositories?q=Data+Scientist&sort=stars&order=desc&per_page=5").mock(
        return_value=httpx.Response(
            200,
            json={"items": [{"full_name": "org/repo", "html_url": "https://github.com/org/repo", "description": "Python ML", "stargazers_count": 10, "language": "Python"}]},
        )
    )
    respx.get("https://hn.algolia.com/api/v1/search?query=Data+Scientist&hitsPerPage=5&tags=story").mock(
        return_value=httpx.Response(200, json={"hits": [{"title": "Hiring data scientists", "url": "https://news.ycombinator.com/item?id=1", "points": 5}]})
    )
    respx.get("https://export.arxiv.org/api/query?search_query=all:Data+Scientist&start=0&max_results=5").mock(
        return_value=httpx.Response(
            200,
            text="""
            <feed xmlns='http://www.w3.org/2005/Atom'>
              <entry>
                <title>Data Science Paper</title>
                <summary>Learning with data</summary>
                <link rel='alternate' href='https://arxiv.org/abs/1234.5678' />
              </entry>
            </feed>
            """,
        )
    )

    response = client.get("/api/career-intelligence", params={"q": "Data Scientist"})
    assert response.status_code == 200
    data = response.json()
    assert data["jobs"]
    assert data["skills"]["technical"]
    assert data["data_sources"]


def test_generate_roadmap_happy_path(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    async def fake_run_job(job_id: str, req: models.RoadmapRequest):
        result = {
            "job_id": job_id,
            "goal": req.goal,
            "skill_level": req.skill_level.value if hasattr(req.skill_level, "value") else str(req.skill_level),
            "country": req.country,
            "weekly_hours": req.weekly_hours,
            "total_weeks": 12,
            "executive_summary": "Test roadmap summary.",
            "phases": [
                {
                    "number": 1,
                    "name": "Foundations",
                    "duration_weeks": 6,
                    "description": "Build core skills.",
                    "phase_insight": "Python is frequently requested.",
                    "skills": [
                        {
                            "name": "Python",
                            "hours": 40,
                            "resources": [{"title": "Python Docs", "url": "https://docs.python.org/3/tutorial/"}],
                        }
                    ],
                }
            ],
            "top_skills": [{"name": "Python", "resources": [{"title": "Python Docs", "url": "https://docs.python.org/3/tutorial/"}]}],
            "salary_bands": [{"level": "Entry", "low": 80000, "high": 110000, "currency": "USD"}],
            "data_sources": ["Curated test data"],
        }
        await models.finish_job(job_id, json.dumps(result))

    monkeypatch.setattr(main, "run_job", fake_run_job)

    response = client.post(
        "/generate",
        data={
            "goal": "Software Engineer",
            "skill_level": "beginner",
            "country": "United States",
            "weekly_hours": 10,
        },
    )
    assert response.status_code == 200
    payload = response.json()
    job_id = payload["job_id"]

    deadline = time.time() + 5
    status_payload = None
    while time.time() < deadline:
        status_response = client.get(f"/api/job/{job_id}")
        status_payload = status_response.json()
        if status_payload["status"] == "complete":
            break
        time.sleep(0.1)

    assert status_payload is not None
    assert status_payload["status"] == "complete"
    assert status_payload["result"]["phases"]

    roadmap_page = client.get(f"/roadmap/{job_id}")
    assert roadmap_page.status_code == 200
    assert "Software Engineer" in roadmap_page.text
