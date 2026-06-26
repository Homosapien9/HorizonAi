from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest
from fastapi.testclient import TestClient
from httpx import ASGITransport

import main
import models


@pytest.fixture
def client():
    with TestClient(main.app) as test_client:
        yield test_client


async def _seed_roadmap() -> tuple[str, str, str]:
    await models.init_db()
    db = await models._get_connection()
    roadmap_id = "sample-roadmap"
    node_db_id = f"node-db-{uuid.uuid4().hex[:8]}"
    logical_node_id = "python-basics"
    await db.execute("DELETE FROM resources")
    await db.execute("DELETE FROM edges")
    await db.execute("DELETE FROM user_progress")
    await db.execute("DELETE FROM nodes")
    await db.execute("DELETE FROM roadmaps")
    await db.execute(
        "INSERT INTO roadmaps(id, name, category, description, total_nodes) VALUES (?,?,?,?,?)",
        (roadmap_id, "Sample Roadmap", "software", "Sample description", 1),
    )
    await db.execute(
        "INSERT INTO nodes(id, roadmap_id, node_id, label, type, position_x, position_y, description, metadata) VALUES (?,?,?,?,?,?,?,?,?)",
        (node_db_id, roadmap_id, logical_node_id, "Python Basics", "default", 10, 20, "Learn Python", '{"difficulty": "beginner"}'),
    )
    await db.execute(
        "INSERT INTO resources(id, node_id, title, url, resource_type, difficulty, duration_hours) VALUES (?,?,?,?,?,?,?)",
        ("resource-1", node_db_id, "Python Docs", "https://docs.python.org/3/tutorial/", "documentation", "beginner", 2),
    )
    await db.commit()
    return roadmap_id, node_db_id, logical_node_id


def test_roadmap_api_smoke(client: TestClient):
    roadmap_id, node_db_id, logical_node_id = asyncio.run(_seed_roadmap())

    response = client.get("/api/roadmaps")
    assert response.status_code == 200
    assert response.json()

    response = client.get(f"/api/roadmaps/{roadmap_id}")
    assert response.status_code == 200
    payload = response.json()
    assert payload["nodes"]

    response = client.get(f"/api/roadmaps/{roadmap_id}/nodes/{logical_node_id}")
    assert response.status_code == 200
    details = response.json()
    assert details["resources"]
    assert details["node"]["label"] == "Python Basics"

    response = client.get("/api/search-nodes", params={"q": "Python"})
    assert response.status_code == 200
    assert response.json()["results"]

    response = client.post(f"/api/progress/test-user/nodes/{node_db_id}/complete", json={"status": "completed"})
    assert response.status_code == 200

    response = client.get(f"/api/progress/test-user/roadmap/{roadmap_id}")
    assert response.status_code == 200
    progress = response.json()
    assert progress["completed_nodes"] == 1
    assert progress["completion_percentage"] == 100.0


@pytest.mark.asyncio
async def test_timeout_partial_results(monkeypatch: pytest.MonkeyPatch):
    async def fake_process_field(*args, **kwargs):
        raise asyncio.TimeoutError()

    monkeypatch.setattr(main.pipeline, "process_field", fake_process_field)
    transport = ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/api/career-intelligence", params={"q": "Software Engineer"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["partial_results"] is True
    assert payload["jobs"] == []


@pytest.mark.asyncio
async def test_concurrency_smoke(monkeypatch: pytest.MonkeyPatch):
    async def fast_process_field(query: str, filters: dict | None = None):
        del filters
        return {
            "field_name": query,
            "interpreted_input": query,
            "confidence_score": "100%",
            "regions_covered": ["Remote"],
            "sources_used": ["Curated test data"],
            "data_sources": ["Curated test data"],
            "jobs": [{"id": query, "title": query, "company": "Acme", "location": "Remote", "description": "", "url": "", "source": "test", "skills": ["Python"], "job_type": "", "remote": True, "country": "Remote", "relevance_score": 1.0, "raw_data": {}}],
            "job_roles": [query],
            "skills": {"technical": ["Python"], "soft": ["Communication"]},
            "tools_and_technologies": ["Python"],
            "companies": ["Acme"],
            "locations": ["Remote"],
            "salary": {"global_average_usd": 100000, "regional_breakdown": {"US": {"avg_min": 90000, "avg_max": 120000}}},
            "demand_by_region": {"Remote": 1},
            "career_path": [],
            "remote_opportunities": [],
            "free_resources": [],
            "courses": [],
            "universities": [],
            "scholarships": [],
            "internships": [],
            "research_papers": [],
            "top_companies": [],
            "data_gaps": [],
            "errors": [],
            "partial_results": False,
        }

    monkeypatch.setattr(main.pipeline, "process_field", fast_process_field)
    transport = ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        responses = await asyncio.gather(*[
            client.get("/api/career-intelligence", params={"q": query})
            for query in ["Software Engineer", "Data Scientist", "Mechanical Engineer", "Cybersecurity Analyst", "Nurse"]
        ])
    assert all(response.status_code == 200 for response in responses)
    assert all(response.json()["field_name"] for response in responses)


def test_endpoint_smoke_suite(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    async def fake_search_jobs(*args, **kwargs):
        del args, kwargs
        return main.search_jobs.__annotations__.get("return").model_validate(
            {
                "query": "Software Engineer",
                "location": "",
                "total": 1,
                "jobs": [],
                "sources_active": ["Curated test data"],
                "trending_skills": [{"skill": "Python", "count": 1}],
                "top_companies": [{"name": "Acme", "count": 1}],
                "salary_range": {"estimated": True, "global_average_usd": 100000, "regional_breakdown": {}},
                "took_ms": 1,
                "error": None,
            }
        )

    async def fake_live_search(*args, **kwargs):
        del args, kwargs
        return {
            "query": "Software Engineer",
            "timestamp": 0,
            "sources_active": ["Curated test data"],
            "jobs": [],
            "internships": [],
            "salary_data": [],
            "all_salary_data": {},
            "skill_trends": ["Python"],
            "scholarships": [],
            "universities": [],
            "research_papers": [],
            "top_companies": [],
            "total_jobs": 0,
            "total_internships": 0,
        }

    async def fake_internships(*args, **kwargs):
        del args, kwargs
        return {
            "query": "Software Engineer",
            "country": "",
            "internships": [],
            "sources_active": ["Curated test data"],
        }

    monkeypatch.setattr(main, "search_jobs", fake_search_jobs)
    monkeypatch.setattr(main, "live_search", fake_live_search)
    monkeypatch.setattr(main, "search_internships", fake_internships)

    endpoints = [
        ("/", {}),
        ("/health", {}),
        ("/api/skills", {"q": "py"}),
        ("/api/jobs", {"q": "Software Engineer"}),
        ("/api/internships", {"q": "Software Engineer"}),
        ("/api/live-search", {"q": "Software Engineer"}),
    ]
    for path, params in endpoints:
        response = client.get(path, params=params)
        assert response.status_code == 200, path
