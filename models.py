"""
Horizon v5 — Data Models, Database, Cache and Utilities
"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field, computed_field, field_validator

import asyncio
import hashlib
import html
import json
import logging
import re
import time

import aiosqlite

from config import settings

logger = logging.getLogger(__name__)


class SkillLevel(str, Enum):
    BEGINNER = "beginner"
    INTERMEDIATE = "intermediate"
    ADVANCED = "advanced"


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"


class RoadmapRequest(BaseModel):
    goal: str = Field(..., min_length=3, max_length=200)
    skill_level: SkillLevel = SkillLevel.BEGINNER
    country: str = Field(..., min_length=2, max_length=100)
    weekly_hours: int = Field(..., ge=1, le=168)

    @field_validator("goal")
    @classmethod
    def sanitize_goal(cls, v: str) -> str:
        import html
        return html.escape(v.strip())

    @field_validator("country")
    @classmethod
    def sanitize_country(cls, v: str) -> str:
        import html
        return html.escape(v.strip())



class SkillNode(BaseModel):
    name: str
    key: str = ""
    hours: int
    phase: str
    prerequisites: list[str] = Field(default_factory=list)
    resources: list[dict[str, str]] = Field(default_factory=list)
    why: str = ""
    frequency_score: float = 0.0
    trend_score: float = 0.0
    relevance_score: float = 0.0
    market_insight: str = ""
    already_known: bool = False
    salary_impact: str = ""
    sparkline: list[float] = Field(default_factory=list)

    @computed_field
    @property
    def total_score(self) -> float:
        return round(self.frequency_score * 0.4 + self.trend_score * 0.3 + self.relevance_score * 0.3, 3)


class Phase(BaseModel):
    number: int
    name: str
    duration_weeks: int
    skills: list[SkillNode] = Field(default_factory=list)
    description: str = ""
    phase_insight: str = ""


class University(BaseModel):
    name: str
    country: str
    ranking: int
    program: str
    url: str
    description: str = ""


class Scholarship(BaseModel):
    name: str
    country: str
    amount: str
    deadline: str
    eligibility: str
    url: str
    relevance_score: float = 0.0


class MarketDemand(BaseModel):
    skill: str
    demand_score: float
    trend: str
    job_count_estimate: int
    top_employers: list[str] = Field(default_factory=list)
    job_posting_pct: float = 0.0
    sparkline: list[float] = Field(default_factory=list)


class SalaryBand(BaseModel):
    level: str
    low: int
    high: int
    currency: str = "USD"
    local_low: Optional[int] = None
    local_high: Optional[int] = None
    local_currency: str = "USD"
    ppp_multiplier: float = 1.0


class GapAnalysis(BaseModel):
    known_skills: list[str] = Field(default_factory=list)
    missing_skills: list[str] = Field(default_factory=list)
    gap_score: float = 0.0
    estimated_weeks_saved: int = 0
    readiness_label: str = ""
    jd_skills: list[str] = Field(default_factory=list)
    jd_missing: list[str] = Field(default_factory=list)
    jd_matched: list[str] = Field(default_factory=list)
    jd_weeks_per_skill: dict[str, int] = Field(default_factory=dict)


class DAGEdge(BaseModel):
    source: str
    target: str


class ComparisonResult(BaseModel):
    goal_a: str
    goal_b: str
    track_a: str
    track_b: str
    skills_only_a: list[str] = Field(default_factory=list)
    skills_shared: list[str] = Field(default_factory=list)
    skills_only_b: list[str] = Field(default_factory=list)
    weeks_a: int = 0
    weeks_b: int = 0
    switch_cost_weeks: int = 0


class Internship(BaseModel):
    title: str
    company: str
    location: str
    duration: str
    stipend: str
    source: str
    url: str


class TopCompany(BaseModel):
    name: str
    industry: str
    internship_count: int
    location: str
    popularity_score: float
    trend_score: float


class TrendAnalysis(BaseModel):
    skill: str
    trend_score: float
    demand_velocity: float
    future_proofing_score: float
    emerging_opportunity: bool


class RoadmapResult(BaseModel):
    job_id: str
    goal: str
    skill_level: SkillLevel
    country: str
    weekly_hours: int
    total_weeks: int
    executive_summary: str
    phases: list[Phase] = Field(default_factory=list)
    market_demand: list[MarketDemand] = Field(default_factory=list)
    universities: list[University] = Field(default_factory=list)
    scholarships: list[Scholarship] = Field(default_factory=list)
    top_skills: list[SkillNode] = Field(default_factory=list)
    salary_bands: list[SalaryBand] = Field(default_factory=list)
    internships: list[Internship] = Field(default_factory=list)
    top_companies: list[TopCompany] = Field(default_factory=list)
    trend_analysis: list[TrendAnalysis] = Field(default_factory=list)
    gap_analysis: Optional[GapAnalysis] = None
    dag_edges: list[DAGEdge] = Field(default_factory=list)
    data_signals: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    data_sources: list[str] = Field(default_factory=list)
    cache_hit: bool = False


class JobState(BaseModel):
    job_id: str
    status: JobStatus = JobStatus.PENDING
    progress: int = 0
    message: str = "Initializing..."
    result: Optional[RoadmapResult] = None
    error: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class JobListing(BaseModel):
    id: str = ""
    title: str
    company: str
    location: str = ""
    description: str = ""
    url: str = ""
    source: str = ""
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    salary_currency: str = "USD"
    skills: list[str] = Field(default_factory=list)
    job_type: str = ""
    posted_date: Optional[str] = None
    remote: bool = False
    country: str = ""
    relevance_score: float = 0.0
    raw_data: dict[str, Any] = Field(default_factory=dict)


class JobSearchResponse(BaseModel):
    query: str
    location: str = ""
    total: int
    jobs: list[JobListing] = Field(default_factory=list)
    sources_active: list[str] = Field(default_factory=list)
    trending_skills: list[dict[str, Any]] = Field(default_factory=list)
    top_companies: list[dict[str, Any]] = Field(default_factory=list)
    salary_range: Optional[dict[str, Any]] = None
    took_ms: int = 0
    error: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    version: str
    uptime_seconds: float
    cache_entries: int
    model_loaded: bool


class MetricsResponse(BaseModel):
    total_roadmaps: int
    cache_hit_rate: float
    avg_generation_time_seconds: float
    popular_goals: list[dict[str, Any]] = Field(default_factory=list)



_db_path = settings.database_path
_db_initialized = False
_db_lock = asyncio.Lock()
_db_conn: Optional[aiosqlite.Connection] = None
_db_conn_lock = asyncio.Lock()

CREATE_TABLES_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA cache_size=10000;
PRAGMA temp_store=MEMORY;

CREATE TABLE IF NOT EXISTS roadmap_cache (
    cache_key    TEXT PRIMARY KEY,
    payload      BLOB NOT NULL,
    compressed   INTEGER DEFAULT 0,
    created_at   REAL NOT NULL,
    accessed_at  REAL NOT NULL,
    access_count INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id       TEXT PRIMARY KEY,
    status       TEXT NOT NULL DEFAULT 'pending',
    progress     INTEGER DEFAULT 0,
    message      TEXT DEFAULT '',
    result_json  TEXT,
    error        TEXT,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS metrics (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    goal             TEXT,
    skill_level      TEXT,
    country          TEXT,
    generation_ms    INTEGER,
    cache_hit        INTEGER DEFAULT 0,
    created_at       REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cache_created ON roadmap_cache(created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_status    ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_metrics_goal   ON metrics(goal);
"""


async def _get_connection() -> aiosqlite.Connection:
    """Get or create the shared database connection."""
    global _db_conn, _db_initialized
    if _db_conn is not None:
        return _db_conn
    async with _db_conn_lock:
        if _db_conn is None:
            await ensure_db_ready()
            conn = await aiosqlite.connect(_db_path)
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("PRAGMA synchronous=NORMAL")
            await conn.execute("PRAGMA cache_size=10000")
            await conn.execute("PRAGMA temp_store=MEMORY")
            _db_conn = conn
        return _db_conn


async def close_db() -> None:
    """Close the shared database connection."""
    global _db_conn
    if _db_conn is not None:
        await _db_conn.close()
        _db_conn = None


async def init_db() -> None:
    """Initialize database schema."""
    global _db_initialized
    conn = await aiosqlite.connect(_db_path)
    await conn.executescript(CREATE_TABLES_SQL)
    await conn.commit()
    await conn.close()
    _db_initialized = True
    logger.info("Database initialized at %s", _db_path)


async def ensure_db_ready() -> None:
    """Lazily initialize the DB so endpoints don't fail before lifespan startup."""
    if _db_initialized:
        return
    async with _db_lock:
        if not _db_initialized:
            await init_db()


async def get_cache(key: str) -> Optional[Any]:
    """Retrieve a cached item; return None if missing or expired."""
    await ensure_db_ready()
    ttl_seconds = settings.cache_ttl_hours * 3600
    cutoff = time.time() - ttl_seconds
    db = await _get_connection()
    async with db.execute(
        "SELECT payload, compressed, created_at FROM roadmap_cache WHERE cache_key=?", (key,)
    ) as cursor:
        row = await cursor.fetchone()
    if row is None:
        return None
    if row["created_at"] < cutoff:
        await db.execute("DELETE FROM roadmap_cache WHERE cache_key=?", (key,))
        await db.commit()
        return None
    await db.execute(
        "UPDATE roadmap_cache SET accessed_at=?, access_count=access_count+1 WHERE cache_key=?",
        (time.time(), key),
    )
    await db.commit()
    payload = row["payload"]
    if row["compressed"]:
        try:
            import zstandard as zstd
        except ImportError:
            logger.error("zstandard not installed but payload is compressed")
            return None
        dctx = zstd.ZstdDecompressor()
        payload = dctx.decompress(payload)
    return json.loads(payload)


async def set_cache(key: str, value: Any) -> None:
    """Store a value in cache with optional compression."""
    await ensure_db_ready()
    raw = json.dumps(value, default=str).encode()
    compressed = 0
    payload: bytes = raw
    if len(raw) > 4096:
        try:
            import zstandard as zstd
            cctx = zstd.ZstdCompressor(level=3)
            payload = cctx.compress(raw)
            compressed = 1
        except Exception:
            payload = raw
            compressed = 0
    now = time.time()
    db = await _get_connection()
    await db.execute(
        """INSERT INTO roadmap_cache(cache_key, payload, compressed, created_at, accessed_at)
           VALUES (?,?,?,?,?)
           ON CONFLICT(cache_key) DO UPDATE SET
             payload=excluded.payload, compressed=excluded.compressed,
             created_at=excluded.created_at, accessed_at=excluded.accessed_at""",
        (key, payload, compressed, now, now),
    )
    await db.commit()


async def cleanup_cache() -> int:
    """Remove expired cache entries. Returns number of deleted rows."""
    await ensure_db_ready()
    cutoff = time.time() - settings.cache_ttl_hours * 3600
    db = await _get_connection()
    cursor = await db.execute("DELETE FROM roadmap_cache WHERE created_at < ?", (cutoff,))
    await db.commit()
    return cursor.rowcount


async def cache_entry_count() -> int:
    await ensure_db_ready()
    db = await _get_connection()
    async with db.execute("SELECT COUNT(*) FROM roadmap_cache") as cur:
        row = await cur.fetchone()
        return row[0] if row else 0


# ── Job management ──────────────────────────────────────────────────────────


async def create_job(job_id: str) -> None:
    await ensure_db_ready()
    now = time.time()
    db = await _get_connection()
    await db.execute(
        "INSERT INTO jobs(job_id, status, progress, message, created_at, updated_at) VALUES (?,?,?,?,?,?)",
        (job_id, "pending", 0, "Queued", now, now),
    )
    await db.commit()


async def update_job(job_id: str, status: str, progress: int, message: str) -> None:
    await ensure_db_ready()
    db = await _get_connection()
    await db.execute(
        "UPDATE jobs SET status=?, progress=?, message=?, updated_at=? WHERE job_id=?",
        (status, progress, message, time.time(), job_id),
    )
    await db.commit()


async def finish_job(job_id: str, result_json: str) -> None:
    await ensure_db_ready()
    db = await _get_connection()
    await db.execute(
        "UPDATE jobs SET status='complete', progress=100, message='Done', result_json=?, updated_at=? WHERE job_id=?",
        (result_json, time.time(), job_id),
    )
    await db.commit()


async def fail_job(job_id: str, error: str, progress: int = 0) -> None:
    await ensure_db_ready()
    # Ensure we never store a blank error message
    if not error or not error.strip():
        error = "Unknown error during roadmap generation"
    db = await _get_connection()
    await db.execute(
        "UPDATE jobs SET status='failed', progress=?, message=?, error=?, updated_at=? WHERE job_id=?",
        (progress, error, error, time.time(), job_id),
    )
    await db.commit()


async def get_job(job_id: str) -> Optional[dict]:
    await ensure_db_ready()
    db = await _get_connection()
    async with db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)) as cur:
        row = await cur.fetchone()
        return dict(row) if row else None


# ── Metrics ──────────────────────────────────────────────────────────────────


async def record_metric(goal: str, skill_level: str, country: str, ms: int, cache_hit: bool) -> None:
    await ensure_db_ready()
    db = await _get_connection()
    await db.execute(
        "INSERT INTO metrics(goal,skill_level,country,generation_ms,cache_hit,created_at) VALUES (?,?,?,?,?,?)",
        (goal, skill_level, country, ms, int(cache_hit), time.time()),
    )
    await db.commit()


async def get_metrics_summary() -> dict:
    await ensure_db_ready()
    db = await _get_connection()
    async with db.execute("SELECT COUNT(*), AVG(generation_ms), SUM(cache_hit) FROM metrics") as cur:
        row = await cur.fetchone()
    total = row[0] or 0
    avg_ms = row[1] or 0
    cache_hits = row[2] or 0
    async with db.execute(
        "SELECT goal, COUNT(*) as cnt FROM metrics GROUP BY goal ORDER BY cnt DESC LIMIT 10"
    ) as cur:
        goals = [dict(r) for r in await cur.fetchall()]
    return {
        "total_roadmaps": total,
        "cache_hit_rate": round(cache_hits / max(total, 1), 3),
        "avg_generation_time_seconds": round(avg_ms / 1000, 2),
        "popular_goals": goals,
    }



def make_key(*parts: str) -> str:
    raw = "|".join(str(p).lower().strip() for p in parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


async def get(key: str) -> Optional[Any]:
    try:
        return await get_cache(key)
    except Exception as exc:
        logger.warning("Cache get failed: %s", exc)
        return None


async def set(key: str, value: Any) -> None:
    try:
        await set_cache(key, value)
    except Exception as exc:
        logger.warning("Cache set failed: %s", exc)


async def cleanup() -> int:
    try:
        return await cleanup_cache()
    except Exception as exc:
        logger.warning("Cache cleanup failed: %s", exc)
        return 0


# ── NO BASELINE DATASETS ──────────────────────────────────────────────────────
# All data is now sourced live from online scraping. No fallbacks to static datasets.



def uptime() -> float:
    return round(time.time() - _START_TIME, 1)


def sanitize_input(text: str, max_len: int = 500) -> str:
    """Sanitize user-supplied text: strip HTML, truncate."""
    text = html.escape(text.strip())
    text = re.sub(r"\s+", " ", text)
    return text[:max_len]


def weeks_to_human(weeks: int) -> str:
    if weeks < 4:
        return f"{weeks} weeks"
    months = weeks / 4.33
    if months < 12:
        return f"{months:.1f} months"
    years = months / 12
    return f"{years:.1f} years"


def phase_color(phase: str) -> str:
    return {
        "foundation": "#6366f1",
        "core": "#0ea5e9",
        "specialization": "#10b981",
        "advanced": "#f59e0b",
    }.get(phase, "#8b5cf6")


def trend_badge(trend: str) -> str:
    if trend == "rising":
        return '<span class="badge badge-rising">↑ Rising</span>'
    if trend == "declining":
        return '<span class="badge badge-declining">↓ Declining</span>'
    return '<span class="badge badge-stable">→ Stable</span>'


def score_bar(score: float, max_width: int = 100) -> int:
    return max(4, min(max_width, round(score * max_width)))
