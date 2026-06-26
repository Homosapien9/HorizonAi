"""Horizon v5 main application."""
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from api.roadmaps import register_roadmap_endpoints
from career_intelligence_pipeline import (
    AdaptiveIntelligenceController,
    live_search,
    search_internships,
    search_jobs,
    skills_autocomplete,
)
from config import settings
from models import (
    JobStatus,
    RoadmapRequest,
    RoadmapResult,
    SkillLevel,
    close_db,
    create_job,
    get_job,
    init_db,
    score_bar,
    uptime,
    weeks_to_human,
)
from roadmap_engine import RoadmapEngine, get_progress, new_job_id, run_job

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.globals["weeks_to_human"] = weeks_to_human
templates.env.globals["score_bar"] = score_bar
engine = RoadmapEngine()
pipeline = AdaptiveIntelligenceController()
_BACKGROUND_TASKS: set[asyncio.Task[Any]] = set()


def _partial_career_payload(query: str) -> dict[str, Any]:
    return {
        "field_name": query.title(),
        "interpreted_input": query,
        "confidence_score": "0%",
        "regions_covered": [],
        "sources_used": [],
        "data_sources": [],
        "jobs": [],
        "job_roles": [],
        "skills": {"technical": [], "soft": []},
        "tools_and_technologies": [],
        "companies": [],
        "locations": [],
        "salary": {"global_average_usd": 0, "regional_breakdown": {}},
        "demand_by_region": {},
        "career_path": [],
        "remote_opportunities": [],
        "free_resources": [],
        "courses": [],
        "universities": [],
        "scholarships": [],
        "internships": [],
        "research_papers": [],
        "top_companies": [],
        "data_gaps": ["Timed out before live sources responded."],
        "errors": ["Timed out after 45 seconds."],
        "partial_results": True,
    }


async def _maybe_json_body(request: Request) -> dict[str, Any]:
    if request.method == "GET":
        return {}
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            return await request.json()
        except json.JSONDecodeError:
            return {}
    return {}


def _track_background_task(task: asyncio.Task[Any]) -> asyncio.Task[Any]:
    _BACKGROUND_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_TASKS.discard)
    return task


def _normalize_skill_payload(skill: dict[str, Any], default_phase: str = "core") -> dict[str, Any]:
    payload = dict(skill)
    skill_name = str(payload.get("name", "Skill")).strip() or "Skill"
    payload.setdefault("name", skill_name)
    payload.setdefault("key", skill_name.lower().replace(" ", "_"))
    payload.setdefault("hours", 0)
    payload.setdefault("phase", default_phase)
    payload.setdefault("prerequisites", [])
    payload.setdefault("resources", [])
    payload.setdefault("why", "")
    payload.setdefault("frequency_score", 0.0)
    payload.setdefault("trend_score", 0.0)
    payload.setdefault("relevance_score", 0.0)
    payload.setdefault("market_insight", "")
    payload.setdefault("already_known", False)
    payload.setdefault("salary_impact", "")
    payload.setdefault("sparkline", [])
    return payload


def _load_roadmap_result(result_json: str) -> RoadmapResult:
    payload = json.loads(result_json)
    phases = []
    for phase in payload.get("phases", []):
        phase_payload = dict(phase)
        default_phase = str(phase_payload.get("name", "core")).lower().replace(" ", "_")
        phase_payload["skills"] = [
            _normalize_skill_payload(skill, default_phase=default_phase)
            for skill in phase_payload.get("skills", [])
        ]
        phases.append(phase_payload)
    payload["phases"] = phases
    payload["top_skills"] = [
        _normalize_skill_payload(skill)
        for skill in payload.get("top_skills", [])
    ]
    return RoadmapResult.model_validate(payload)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting %s %s", settings.app_name, settings.app_version)
    await init_db()
    yield
    await close_db()
    logger.info("Shut down %s", settings.app_name)


app = FastAPI(
    title=settings.app_name,
    description=settings.app_description,
    version=settings.app_version,
    lifespan=lifespan,
)

static_dir = BASE_DIR / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.middleware("http")
async def dispatch_middleware(request: Request, call_next):
    return await call_next(request)


@app.get("/", response_class=HTMLResponse)
async def home_page(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.get("/technical-roadmaps", response_class=HTMLResponse)
async def technical_roadmaps_page(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.get("/search", response_class=HTMLResponse)
async def search_page(request: Request):
    return templates.TemplateResponse(request=request, name="search.html")


@app.get("/compare", response_class=HTMLResponse)
async def compare_page(request: Request):
    return templates.TemplateResponse(request=request, name="compare.html")


@app.get("/career-intelligence", response_class=HTMLResponse)
async def career_intelligence_page(request: Request):
    return templates.TemplateResponse(request=request, name="career_intelligence.html")


@app.get("/health", response_class=JSONResponse)
async def health_check():
    return {
        "status": "healthy",
        "version": settings.app_version,
        "app_name": settings.app_name,
        "uptime_seconds": uptime(),
        "database_path": settings.database_path,
    }


@app.api_route("/api/career-intelligence", methods=["GET", "POST"])
async def career_intelligence_endpoint(
    request: Request,
    q: str | None = Query(default=None),
    country: str | None = Query(default=None),
    experience_level: str | None = Query(default=None),
    remote_only: bool = Query(default=False),
):
    payload = await _maybe_json_body(request)
    q = payload.get("q") or q
    country = payload.get("country") or country
    experience_level = payload.get("experience_level") or experience_level
    remote_only = payload.get("remote_only", remote_only)
    if not q:
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required")
    filters = {"country": country, "experience_level": experience_level, "remote_only": remote_only}
    filters = {key: value for key, value in filters.items() if value not in (None, "")}
    try:
        result = await asyncio.wait_for(pipeline.process_field(q, filters), timeout=45)
    except asyncio.TimeoutError:
        result = _partial_career_payload(q)
    except Exception as exc:
        logger.exception("career_intelligence_endpoint failed for %s", q)
        result = _partial_career_payload(q)
        result["errors"] = [f"Unexpected error: {type(exc).__name__}"]
        result["data_gaps"] = result.get("data_gaps", []) + ["Returned curated partial response after an internal error."]
    return JSONResponse(content=result)


@app.get("/api/live-search")
async def live_search_endpoint(
    q: str = Query(..., min_length=2),
    country: str = "",
    remote_only: bool = False,
):
    try:
        result = await asyncio.wait_for(live_search(q, country=country, remote_only=remote_only), timeout=45)
    except asyncio.TimeoutError:
        result = {
            "query": q,
            "timestamp": 0,
            "sources_active": [],
            "jobs": [],
            "internships": [],
            "salary_data": [],
            "all_salary_data": {},
            "skill_trends": [],
            "scholarships": [],
            "universities": [],
            "research_papers": [],
            "top_companies": [],
            "total_jobs": 0,
            "total_internships": 0,
            "partial_results": True,
        }
    except Exception as exc:
        logger.exception("live_search_endpoint failed for %s", q)
        result = {
            "query": q,
            "timestamp": 0,
            "sources_active": [],
            "jobs": [],
            "internships": [],
            "salary_data": [],
            "all_salary_data": {},
            "skill_trends": [],
            "scholarships": [],
            "universities": [],
            "research_papers": [],
            "top_companies": [],
            "total_jobs": 0,
            "total_internships": 0,
            "partial_results": True,
            "error": f"Unexpected error: {type(exc).__name__}",
        }
    return JSONResponse(content=result)


@app.get("/api/jobs")
async def jobs_endpoint(
    q: str = Query(..., min_length=2),
    location: str = "",
    country: str = "",
    limit: int = 20,
    remote_only: bool = False,
    sort_by: str = "relevance",
):
    try:
        result = await asyncio.wait_for(
            search_jobs(q, location=location, country=country, limit=limit, remote_only=remote_only, sort_by=sort_by),
            timeout=45,
        )
        return JSONResponse(content=result.model_dump())
    except asyncio.TimeoutError:
        return JSONResponse(
            content={
                "query": q,
                "location": location,
                "total": 0,
                "jobs": [],
                "sources_active": [],
                "trending_skills": [],
                "top_companies": [],
                "salary_range": None,
                "took_ms": 45000,
                "error": "Timed out after 45 seconds.",
                "partial_results": True,
            }
        )
    except Exception as exc:
        logger.exception("jobs_endpoint failed for %s", q)
        return JSONResponse(
            content={
                "query": q,
                "location": location,
                "total": 0,
                "jobs": [],
                "sources_active": [],
                "trending_skills": [],
                "top_companies": [],
                "salary_range": None,
                "took_ms": 0,
                "error": f"Unexpected error: {type(exc).__name__}",
                "partial_results": True,
            }
        )


@app.get("/api/internships")
async def internships_endpoint(
    q: str = Query(..., min_length=2),
    country: str = "",
    limit: int = 10,
):
    try:
        result = await asyncio.wait_for(search_internships(q, country=country, limit=limit), timeout=45)
    except asyncio.TimeoutError:
        result = {
            "query": q,
            "country": country,
            "internships": [],
            "sources_active": [],
            "partial_results": True,
        }
    except Exception as exc:
        logger.exception("internships_endpoint failed for %s", q)
        result = {
            "query": q,
            "country": country,
            "internships": [],
            "sources_active": [],
            "partial_results": True,
            "error": f"Unexpected error: {type(exc).__name__}",
        }
    return JSONResponse(content=result)


@app.get("/api/skills")
async def skills_endpoint(q: str = Query(..., min_length=1), limit: int = 12):
    result = await skills_autocomplete(q, limit=limit)
    return JSONResponse(content=result)


@app.post("/api/compare")
async def compare_endpoint(
    goal_a: str = Form(...),
    goal_b: str = Form(...),
    skill_level: str = Form(default="beginner"),
    weekly_hours: int = Form(default=10),
):
    comparison = engine.compare_goals(
        goal_a=goal_a,
        goal_b=goal_b,
        skill_level=SkillLevel(skill_level),
        weekly_hours=weekly_hours,
    )
    return JSONResponse(content=comparison.model_dump())


@app.api_route("/api/jd-scan", methods=["GET", "POST"])
async def jd_scan_endpoint(
    request: Request,
    jd_text: str | None = Form(default=None),
    weekly_hours: int = Form(default=10),
):
    payload = await _maybe_json_body(request)
    jd_text = payload.get("jd_text") or jd_text or request.query_params.get("jd_text")
    weekly_hours = int(payload.get("weekly_hours") or weekly_hours or request.query_params.get("weekly_hours", 10))
    if not jd_text:
        raise HTTPException(status_code=400, detail="jd_text is required")
    result = engine.analyze_jd(jd_text=jd_text, weekly_hours=weekly_hours)
    return JSONResponse(content=result)


@app.api_route("/api/resume", methods=["GET", "POST"])
async def resume_endpoint(
    request: Request,
    resume_text: str | None = Form(default=None),
    target_goal: str = Form(default="Software Engineer"),
):
    payload = await _maybe_json_body(request)
    resume_text = payload.get("resume_text") or resume_text or request.query_params.get("resume_text")
    target_goal = payload.get("target_goal") or target_goal or request.query_params.get("target_goal", "Software Engineer")
    if not resume_text:
        raise HTTPException(status_code=400, detail="resume_text is required")
    jd_result = engine.analyze_jd(jd_text=resume_text, weekly_hours=10)
    comparison = engine.compare_goals(goal_a=target_goal, goal_b=target_goal)
    return JSONResponse(
        content={
            "target_goal": target_goal,
            "resume_skills": jd_result["required_skills"],
            "matched_skills": jd_result["matched"],
            "missing_skills": jd_result["missing"],
            "weeks_per_skill": jd_result["weeks_per_skill"],
            "suggested_track": comparison.track_a,
        }
    )


@app.post("/generate")
async def generate_roadmap_endpoint(
    goal: str = Form(...),
    skill_level: str = Form(...),
    country: str = Form(...),
    weekly_hours: int = Form(...),
):
    req = RoadmapRequest(
        goal=goal,
        skill_level=skill_level,
        country=country,
        weekly_hours=weekly_hours,
    )
    job_id = new_job_id()
    await create_job(job_id)
    _track_background_task(asyncio.create_task(run_job(job_id, req)))
    return JSONResponse(
        content={
            "status": JobStatus.PENDING.value,
            "job_id": job_id,
            "progress_url": f"/api/job/{job_id}",
            "roadmap_url": f"/roadmap/{job_id}",
        }
    )


@app.get("/api/job/{job_id}")
async def job_status_endpoint(job_id: str):
    record = await get_job(job_id)
    progress = get_progress(job_id)
    if not record:
        raise HTTPException(status_code=404, detail="Job not found")
    payload = {
        "job_id": job_id,
        "status": record.get("status"),
        "progress": record.get("progress"),
        "message": record.get("message"),
        "error": record.get("error"),
        "updated_at": record.get("updated_at"),
        "progress_state": progress,
    }
    if record.get("result_json"):
        payload["result"] = json.loads(record["result_json"])
    return JSONResponse(content=payload)


@app.get("/api/roadmap/{job_id}")
async def roadmap_json_endpoint(job_id: str):
    record = await get_job(job_id)
    if not record:
        raise HTTPException(status_code=404, detail="Job not found")
    if not record.get("result_json"):
        return JSONResponse(content={"job_id": job_id, "status": record.get("status"), "message": record.get("message")})
    return JSONResponse(content=json.loads(record["result_json"]))


@app.get("/loading/{job_id}", response_class=HTMLResponse)
async def loading_page(job_id: str, request: Request):
    record = await get_job(job_id)
    if not record:
        raise HTTPException(status_code=404, detail="Job not found")
    if record.get("result_json"):
        return RedirectResponse(url=f"/roadmap/{job_id}", status_code=303)
    if record.get("status") == JobStatus.FAILED.value:
        return templates.TemplateResponse(
            request=request,
            name="error.html",
            context={
                "code": 500,
                "error": record.get("error") or record.get("message") or "Roadmap generation failed.",
            },
            status_code=500,
        )
    return templates.TemplateResponse(request=request, name="loading.html", context={"job_id": job_id})


@app.get("/roadmap/{job_id}", response_class=HTMLResponse)
async def roadmap_page(job_id: str, request: Request):
    record = await get_job(job_id)
    if not record:
        raise HTTPException(status_code=404, detail="Job not found")
    if record.get("result_json"):
        result = _load_roadmap_result(record["result_json"])
        return templates.TemplateResponse(
            request=request,
            name="roadmap.html",
            context={
                "dag_edges_json": [edge.model_dump(mode="json") for edge in result.dag_edges],
                "result": result,
            },
        )
    if record.get("status") == JobStatus.FAILED.value:
        return templates.TemplateResponse(
            request=request,
            name="error.html",
            context={
                "code": 500,
                "error": record.get("error") or record.get("message") or "Roadmap generation failed.",
            },
            status_code=500,
        )
    return templates.TemplateResponse(request=request, name="loading.html", context={"job_id": job_id})


register_roadmap_endpoints(app)
