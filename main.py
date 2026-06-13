"""
Horizon v5 — FastAPI application.
Roadmaps, comparison, live search, internships, and JD scanning.
"""
from __future__ import annotations
import os
import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from markupsafe import Markup
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
import models as database
from roadmap_engine import new_job_id, run_job, get_progress
import models as utils
from career_intelligence_pipeline import _kw_model, _get_kw_model, _get_embed_model
from config import settings
from models import HealthResponse, MetricsResponse, RoadmapRequest, SkillLevel

logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent

limiter = Limiter(key_func=get_remote_address)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("Horizon v5 starting…")
    await database.init_db()

    async def _cleanup_loop() -> None:
        while True:
            try:
                await asyncio.sleep(3600)
                deleted = await database.cleanup_cache()
                logger.info("Cache cleanup: %d entries removed", deleted)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Cache cleanup failed: %s", exc)

    cleanup_task = asyncio.create_task(_cleanup_loop())

    if settings.warm_nlp_models:
        logger.info("Warming up NLP models…")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _get_kw_model)
        await loop.run_in_executor(None, _get_embed_model)
        logger.info("Models loaded")
    else:
        logger.info("Skipping NLP warmup; models will load lazily")

    logger.info("Ready on http://%s:%d", settings.host, settings.port)
    try:
        yield
    finally:
        logger.info("Shutting down…")
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass
        await database.close_db()


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=settings.app_description,
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.globals["weeks_to_human"]  = utils.weeks_to_human
templates.env.globals["phase_color"]     = utils.phase_color
templates.env.globals["score_bar"]       = utils.score_bar
templates.env.globals["trend_badge"]     = lambda v: Markup(utils.trend_badge(v))
templates.env.filters["human_phase"]     = lambda p: p.replace("_", " ").title()

# Custom tojson filter that handles Pydantic BaseModel objects (e.g. DAGEdge)
import json as _json
from pydantic import BaseModel as _BaseModel

def _pydantic_aware_tojson(value, **kwargs):
    class _Encoder(_json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, _BaseModel):
                return obj.model_dump()
            return super().default(obj)
    return _json.dumps(value, cls=_Encoder)

templates.env.filters["tojson"] = _pydantic_aware_tojson


def _schedule_job(job_id: str, req: RoadmapRequest):
    task = asyncio.create_task(run_job(job_id, req))
    task.add_done_callback(lambda t: logger.error("Job %s crashed: %s", job_id, t.exception()) if t.exception() else None)


# ── Pages ──────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.get("/compare", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def compare_page(request: Request):
    return templates.TemplateResponse(request=request, name="compare.html")


# ── Roadmap generation ─────────────────────────────────────────────────────────

@app.post("/generate")
@limiter.limit(f"{settings.rate_limit_per_minute}/minute")
async def generate_roadmap(
    request: Request,
    goal: str          = Form(..., min_length=3, max_length=200),
    skill_level: str   = Form(...),
    country: str       = Form(..., min_length=2, max_length=100),
    weekly_hours: int  = Form(..., ge=1, le=168),
):
    try:
        level = SkillLevel(skill_level)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid skill level")

    req = RoadmapRequest(
        goal=goal, skill_level=level, country=country,
        weekly_hours=weekly_hours,
    )
    job_id = new_job_id()
    await database.create_job(job_id)
    _schedule_job(job_id, req)
    return JSONResponse({"job_id": job_id, "status": "pending"})


@app.get("/status/{job_id}")
@limiter.limit("120/minute")
async def job_status(request: Request, job_id: str):
    if not job_id.isalnum() or len(job_id) != 32:
        raise HTTPException(status_code=400, detail="Invalid job_id")
    return JSONResponse(get_progress(job_id))


@app.get("/loading/{job_id}", response_class=HTMLResponse)
async def loading_page(request: Request, job_id: str):
    if not job_id.isalnum() or len(job_id) != 32:
        raise HTTPException(status_code=400, detail="Invalid job_id")
    return templates.TemplateResponse(request=request, name="loading.html", context={"job_id": job_id})


@app.get("/events/{job_id}")
async def sse_events(request: Request, job_id: str):
    if not job_id.isalnum() or len(job_id) != 32:
        raise HTTPException(status_code=400, detail="Invalid job_id")

    async def stream() -> AsyncGenerator[str, None]:
        last = -1
        deadline = time.time() + max(settings.max_roadmap_generation_seconds + 120, 300)
        while time.time() < deadline:
            if await request.is_disconnected():
                break
            state = get_progress(job_id)
            prog = state.get("progress", 0)
            status = state.get("status", "pending")
            if prog != last:
                yield f"data: {json.dumps(state)}\n\n"
                last = prog
            if status in ("complete", "failed"):
                yield f"data: {json.dumps({**state, 'done': True})}\n\n"
                break
            await asyncio.sleep(0.5)

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


@app.get("/roadmap/{job_id}", response_class=HTMLResponse)
async def roadmap_page(request: Request, job_id: str):
    if not job_id.isalnum() or len(job_id) != 32:
        raise HTTPException(status_code=400, detail="Invalid job_id")

    job = await database.get_job(job_id)
    if not job:
        return templates.TemplateResponse(
            request=request,
            name="error.html",
            context={"error": "Job not found.", "code": 404},
            status_code=404,
        )

    if job["status"] in ("running", "pending"):
        return RedirectResponse(url=f"/loading/{job_id}", status_code=302)

    if job["status"] == "failed":
        error_detail = (
            job.get("error") or job.get("message") or "Unknown error during roadmap generation"
        ).strip() or "Unknown error during roadmap generation"
        return templates.TemplateResponse(
            request=request,
            name="error.html",
            context={"error": error_detail, "code": 500},
            status_code=500,
        )

    from models import RoadmapResult
    if not job.get("result_json"):
        return templates.TemplateResponse(
            request=request,
            name="error.html",
            context={"error": "Roadmap result missing.", "code": 500},
            status_code=500,
        )
    try:
        result = RoadmapResult.model_validate_json(job["result_json"])
    except Exception:
        return templates.TemplateResponse(
            request=request,
            name="error.html",
            context={"error": "Corrupt roadmap data.", "code": 500},
            status_code=500,
        )
    return templates.TemplateResponse(request=request, name="roadmap.html", context={"result": result})


# ── New v3 API Endpoints ───────────────────────────────────────────────────────


@app.post("/api/jd-scan")
@limiter.limit("20/minute")
async def jd_scan(
    request: Request,
    jd_text: str = Form(default=""),
    jd_url: str = Form(default=""),
    weekly_hours: int = Form(default=10, ge=1, le=168),
):
    """
    Job Description Scanner — extracts required skills from JD text or URL
    and estimates a baseline time to learn each one.
    """
    from career_intelligence_pipeline import fetch_url_text
    from roadmap_engine import RoadmapEngine

    text = jd_text.strip()

    # Fetch URL if provided
    if not text and jd_url.strip():
        try:
            text = await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(None, fetch_url_text, jd_url.strip()),
                timeout=15,
            )
        except Exception as e:
            logger.warning("JD URL fetch failed: %s", e)
            text = ""

    if not text or len(text) < 50:
        raise HTTPException(status_code=400, detail="No usable JD text found. Paste the JD text directly.")

    engine = RoadmapEngine()
    try:
        result = await asyncio.get_running_loop().run_in_executor(
            None, engine.analyze_jd, text, weekly_hours
        )
    except Exception as e:
        logger.error("JD analysis error: %s", e)
        raise HTTPException(status_code=500, detail="JD analysis failed")

    return JSONResponse(result)



@app.post("/api/compare")
@limiter.limit("30/minute")
async def compare_goals(
    request: Request,
    goal_a: str = Form(..., min_length=3, max_length=200),
    goal_b: str = Form(..., min_length=3, max_length=200),
    skill_level: str = Form(default="beginner"),
    weekly_hours: int = Form(default=10, ge=1, le=168),
):
    """
    Roadmap Comparison Mode — returns 3-column skill diff between two goals.
    """
    from roadmap_engine import RoadmapEngine

    try:
        level = SkillLevel(skill_level)
    except ValueError:
        level = SkillLevel.BEGINNER

    engine = RoadmapEngine()
    try:
        result = await asyncio.get_running_loop().run_in_executor(
            None, engine.compare_goals,
            goal_a.strip(), goal_b.strip(), level, weekly_hours,
        )
    except Exception as e:
        logger.error("Comparison error: %s", e)
        raise HTTPException(status_code=500, detail="Comparison failed")

    return JSONResponse(result.model_dump())


# ── Existing API endpoints ─────────────────────────────────────────────────────

@app.get("/api/roadmap/{job_id}")
async def roadmap_json(job_id: str):
    if not job_id.isalnum() or len(job_id) != 32:
        raise HTTPException(status_code=400, detail="Invalid job_id")
    job = await database.get_job(job_id)
    if not job or job["status"] != "complete":
        raise HTTPException(status_code=404, detail="Roadmap not ready")
    return JSONResponse(json.loads(job["result_json"]))


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="ok", version=settings.app_version,
        uptime_seconds=utils.uptime(),
        cache_entries=await database.cache_entry_count(),
        model_loaded=_kw_model is not None,
    )


@app.get("/metrics", response_model=MetricsResponse)
async def metrics():
    return MetricsResponse(**(await database.get_metrics_summary()))


# ── LIVE SEARCH ENDPOINT ────────────────────────────────────────────────────
@app.get("/api/live-search")
@limiter.limit("20/minute")
async def live_search(request: Request, q: str = "", country: str = "USA"):
    """
    Fully dynamic live market intelligence for any job/career query.
    Returns: jobs, internships, scholarships, salary data, trends, universities.
    """
    from career_intelligence_pipeline import fetch_all_market_data

    q = q.strip()[:200]
    if not q:
        raise HTTPException(status_code=400, detail="Query 'q' is required")

    try:
        data = await fetch_all_market_data(q)
        # Filter salary by country if possible
        salary_data = data.get("salary_data", [])
        country_upper = country.upper()
        country_salary = [s for s in salary_data if s.get("country", "").upper() == country_upper]
        if not country_salary:
            country_salary = [s for s in salary_data if s.get("country", "USA") == "USA"]

        return JSONResponse({
            "query": q,
            "country": country,
            "timestamp": time.time(),
            "sources_active": data.get("data_sources", []),
            "total_jobs": data.get("total_jobs_scraped", 0),
            "total_internships": data.get("total_internships", 0),

            # Core data
            "jobs": data.get("job_listings", [])[:30],
            "internships": data.get("internships", [])[:20],
            "scholarships": data.get("scholarships", [])[:10],
            "universities": data.get("universities", [])[:10],
            "salary_data": country_salary,
            "all_salary_data": salary_data,

            # Intelligence
            "github_trends": data.get("github_trends", [])[:15],
            "research_papers": data.get("arxiv_papers", [])[:10],
            "top_companies": data.get("top_companies", [])[:12],
            "skill_trends": data.get("trend_analysis", [])[:20],
            "hn_jobs": data.get("hn_jobs", [])[:10],
        })
    except Exception as exc:
        logger.error("Live search error for %s: %s", q, exc)
        raise HTTPException(status_code=500, detail=f"Search failed: {str(exc)}")


@app.get("/api/jobs")
@limiter.limit("30/minute")
async def search_jobs_endpoint(
    request: Request,
    q: str = "",
    location: str = "",
    limit: int = 30,
    remote_only: bool = False,
    country: str = "",
    sort_by: str = "relevance",
):
    """
    Multi-source job aggregation endpoint.
    Fetches live jobs from RemoteOK, WeWorkRemotely, Indeed, Craigslist, SimplyHired, AlmaMedia.
    Returns deduplicated, ranked, normalized results with trend analysis.
    """
    from career_intelligence_pipeline import search_jobs
    from models import JobSearchResponse

    try:
        result = await search_jobs(
            query=q or "",
            location=location or "",
            limit=min(max(limit, 1), 100),
            remote_only=remote_only,
            country=country or "",
            sort_by=sort_by or "relevance",
        )
        return JSONResponse(result.model_dump())
    except Exception as exc:
        logger.error("Job search error: %s", exc)
        return JSONResponse(
            JobSearchResponse(
                query=q or "", location=location or "", total=0,
                error=str(exc)[:200],
            ).model_dump(),
            status_code=500,
        )


@app.get("/api/internships")
@limiter.limit("30/minute")
async def search_internships(
    request: Request,
    q: str = "",
    country: str = "USA",
    limit: int = 20,
):
    """
    Dedicated internship search endpoint.
    Searches live sources for internship listings filtered by query and country.
    """
    from career_intelligence_pipeline import fetch_all_market_data

    query = (q.strip() or "software engineering intern")[:200]
    # Ensure the query includes 'intern' for relevance
    if "intern" not in query.lower():
        query = query + " intern"

    try:
        data = await fetch_all_market_data(query)
        internships = data.get("internships", [])
        # Also filter job listings for internship roles
        job_interns = [
            j for j in data.get("job_listings", [])
            if "intern" in j.get("title", "").lower()
            or "intern" in j.get("description", "").lower()
        ]
        # Merge and deduplicate by URL
        seen_urls: set[str] = set()
        merged = []
        for item in internships + job_interns:
            url = item.get("url", "")
            if url and url in seen_urls:
                continue
            seen_urls.add(url)
            merged.append(item)

        merged = merged[:limit]
        return JSONResponse({
            "query": query,
            "country": country,
            "total": len(merged),
            "internships": merged,
            "sources_active": data.get("data_sources", []),
            "salary_data": data.get("salary_data", []),
        })
    except Exception as exc:
        logger.error("Internship search error for %s: %s", query, exc)
        raise HTTPException(status_code=500, detail=f"Search failed: {str(exc)}")


@app.get("/search", response_class=HTMLResponse)
async def search_page(request: Request):
    """Dynamic live search dashboard page."""
    return templates.TemplateResponse(request=request, name="search.html")


@app.get("/internships", response_class=HTMLResponse)
async def internships_page(request: Request):
    """Redirect to home page internship section."""
    return RedirectResponse(url="/#internship-hunt", status_code=302)


# ── GLOBAL CAREER INTELLIGENCE ENDPOINT ────────────────────────────────────────────
@app.get("/api/career-intelligence")
@limiter.limit("15/minute")
async def career_intelligence(
    request: Request,
    q: str = "",
    country: str = "",
    experience_level: str = "",
    remote_only: bool = False,
):
    """
    Global Career Intelligence — Real-time data extraction pipeline.

    Extracts, aggregates, validates, and structures real-world career data for ANY field
    across ALL countries using ONLY free, publicly accessible APIs and web sources.

    Parameters:
    - q: Career field or role query (can be vague, empty, or random)
    - country: Optional country filter for regional data
    - experience_level: Optional experience level filter
    - remote_only: Filter for remote-only opportunities

    Returns real-time global career intelligence from external sources:
    - Multi-source data extraction (jobs, skills, tools, internships, companies, demand, salaries)
    - Global data normalization and validation
    - Confidence scoring and gap analysis
    - Projects, courses, and external resources
    """
    import career_intelligence_pipeline as ci

    q = q.strip()[:200]
    if not q:
        raise HTTPException(status_code=400, detail="Query 'q' is required")

    try:
        pipeline = ci.GlobalCareerIntelligencePipeline()

        # Prepare optional filters
        optional_filters = {}
        if country:
            optional_filters["country"] = country
        if experience_level:
            optional_filters["experience_level"] = experience_level
        if remote_only:
            optional_filters["remote_only"] = remote_only

        # Process the field with STRICT external data extraction
        result = await pipeline.process_field(q, optional_filters)

        return JSONResponse(result)
    except Exception as exc:
        logger.error("Career intelligence error for %s: %s", q, exc)
        raise HTTPException(status_code=500, detail=f"Career intelligence failed: {str(exc)}")


@app.get("/career-intelligence", response_class=HTMLResponse)
async def career_intelligence_page(request: Request):
    """Career intelligence dashboard page."""
    return templates.TemplateResponse(request=request, name="career_intelligence.html")


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info"
    )
