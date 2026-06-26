"""
Horizon v5 career intelligence pipeline.

This module intentionally uses only free APIs or clearly labeled curated fallback data.
All network calls are bounded, rate-limited, and safe to time out.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from collections import Counter
from typing import Any, Optional
from urllib.parse import quote_plus, urlparse

import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

HTTP_TIMEOUT = httpx.Timeout(10.0, connect=5.0)
MAX_RETRY_SECONDS = 10.0
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36 HorizonCareerIntel/5.4"
    ),
    "Accept": "application/json, text/plain, */*",
}
COUNTRY_TO_ISO_ALPHA2 = {
    "argentina": "ar",
    "australia": "au",
    "austria": "at",
    "bangladesh": "bd",
    "belgium": "be",
    "brazil": "br",
    "canada": "ca",
    "chile": "cl",
    "china": "cn",
    "colombia": "co",
    "czech republic": "cz",
    "denmark": "dk",
    "egypt": "eg",
    "finland": "fi",
    "france": "fr",
    "germany": "de",
    "greece": "gr",
    "hungary": "hu",
    "india": "in",
    "indonesia": "id",
    "ireland": "ie",
    "israel": "il",
    "italy": "it",
    "japan": "jp",
    "kenya": "ke",
    "malaysia": "my",
    "mexico": "mx",
    "netherlands": "nl",
    "new zealand": "nz",
    "nigeria": "ng",
    "norway": "no",
    "pakistan": "pk",
    "peru": "pe",
    "philippines": "ph",
    "poland": "pl",
    "portugal": "pt",
    "romania": "ro",
    "russia": "ru",
    "saudi arabia": "sa",
    "singapore": "sg",
    "south africa": "za",
    "south korea": "kr",
    "spain": "es",
    "sweden": "se",
    "switzerland": "ch",
    "thailand": "th",
    "turkey": "tr",
    "uae": "ae",
    "uk": "gb",
    "ukraine": "ua",
    "united arab emirates": "ae",
    "united kingdom": "gb",
    "united states": "us",
    "united states of america": "us",
    "usa": "us",
    "vietnam": "vn",
}

_RATE_LIMITS = {
    "en.wikipedia.org": 0.35,
    "www.wikidata.org": 0.5,
    "query.wikidata.org": 1.0,
    "ec.europa.eu": 0.6,
    "api.github.com": 0.7,
    "hn.algolia.com": 0.25,
    "export.arxiv.org": 0.5,
    "remoteok.com": 1.0,
    "arbeitnow.com": 0.75,
    "api.adzuna.com": 0.8,
    "data.usajobs.gov": 0.8,
    "weworkremotely.com": 1.0,
    "internshala.com": 1.0,
    "scholars4dev.com": 1.0,
    "scholarship.com": 1.0,
    "fastweb.com": 1.0,
    "un.org": 1.0,
}
_LAST_REQUEST: dict[str, float] = {}
_RATE_LOCK = asyncio.Lock()
_ROBOTS_CACHE: dict[str, bool] = {}
_ROBOTS_LOCK = asyncio.Lock()
_SPARQL_LOCK = asyncio.Lock()

TECH_KEYWORDS = {
    "engineer",
    "engineering",
    "developer",
    "software",
    "data",
    "machine learning",
    "ml",
    "ai",
    "artificial intelligence",
    "civil",
    "mechanical",
    "electrical",
    "robotics",
    "cybersecurity",
    "security",
    "cloud",
    "devops",
    "backend",
    "frontend",
    "full stack",
    "bioinformatics",
    "analyst",
    "architect",
    "embedded",
    "network",
    "qa",
    "sre",
    "site reliability",
}

SOFT_SKILLS = [
    "communication",
    "problem solving",
    "teamwork",
    "analytical thinking",
    "documentation",
    "stakeholder management",
    "adaptability",
    "project planning",
]

SKILL_LEXICON = [
    "python",
    "java",
    "javascript",
    "typescript",
    "sql",
    "c",
    "c++",
    "c#",
    "go",
    "rust",
    "matlab",
    "r",
    "react",
    "node.js",
    "fastapi",
    "django",
    "flask",
    "spring",
    "docker",
    "kubernetes",
    "aws",
    "azure",
    "gcp",
    "linux",
    "git",
    "terraform",
    "ansible",
    "airflow",
    "spark",
    "hadoop",
    "pandas",
    "numpy",
    "scikit-learn",
    "tensorflow",
    "pytorch",
    "opencv",
    "postgresql",
    "mysql",
    "mongodb",
    "redis",
    "etl",
    "data modeling",
    "statistics",
    "machine learning",
    "deep learning",
    "nlp",
    "computer vision",
    "cad",
    "solidworks",
    "autocad",
    "ansys",
    "plc",
    "embedded systems",
    "rtos",
    "signals",
    "circuits",
    "power systems",
    "iec 61850",
    "scada",
    "control systems",
    "finite element analysis",
    "thermodynamics",
    "fluid mechanics",
    "structural analysis",
    "revit",
    "bim",
    "geotechnical",
    "surveying",
    "network security",
    "siem",
    "incident response",
    "penetration testing",
    "owasp",
    "api design",
    "testing",
]

ROLE_PROFILES: dict[str, dict[str, Any]] = {
    "software engineer": {
        "technical_skills": ["Python", "JavaScript", "SQL", "Git", "Docker", "API Design"],
        "tools": ["FastAPI", "React", "PostgreSQL", "GitHub", "Docker"],
        "salary": {"global_average_usd": 115000, "regional_breakdown": {"US": {"avg_min": 90000, "avg_max": 155000}, "EU": {"avg_min": 55000, "avg_max": 95000}, "India": {"avg_min": 18000, "avg_max": 42000}}},
        "career_path": ["Junior Engineer", "Software Engineer", "Senior Engineer", "Staff Engineer"],
        "courses": [
            {"title": "Full Stack Open", "url": "https://fullstackopen.com/en/", "source": "University of Helsinki", "cost": "free"},
            {"title": "The Odin Project", "url": "https://www.theodinproject.com/", "source": "The Odin Project", "cost": "free"},
        ],
    },
    "data scientist": {
        "technical_skills": ["Python", "SQL", "Statistics", "Pandas", "Machine Learning", "Data Visualization"],
        "tools": ["Jupyter", "scikit-learn", "Pandas", "NumPy", "Tableau Public"],
        "salary": {"global_average_usd": 122000, "regional_breakdown": {"US": {"avg_min": 95000, "avg_max": 165000}, "EU": {"avg_min": 60000, "avg_max": 105000}, "India": {"avg_min": 20000, "avg_max": 45000}}},
        "career_path": ["Data Analyst", "Data Scientist", "Senior Data Scientist", "Lead / Principal Data Scientist"],
        "courses": [
            {"title": "Kaggle Learn", "url": "https://www.kaggle.com/learn", "source": "Kaggle", "cost": "free"},
            {"title": "Introduction to Statistical Learning", "url": "https://www.statlearning.com/", "source": "ISLR", "cost": "free"},
        ],
    },
    "mechanical engineer": {
        "technical_skills": ["CAD", "SolidWorks", "Thermodynamics", "Finite Element Analysis", "Manufacturing", "Materials"],
        "tools": ["SolidWorks", "AutoCAD", "ANSYS", "MATLAB"],
        "salary": {"global_average_usd": 92000, "regional_breakdown": {"US": {"avg_min": 70000, "avg_max": 120000}, "EU": {"avg_min": 45000, "avg_max": 78000}, "India": {"avg_min": 12000, "avg_max": 26000}}},
        "career_path": ["Graduate Engineer", "Mechanical Engineer", "Senior Mechanical Engineer", "Engineering Manager"],
        "courses": [
            {"title": "MIT OCW Mechanics", "url": "https://ocw.mit.edu/", "source": "MIT OpenCourseWare", "cost": "free"},
            {"title": "NPTEL Mechanical Engineering", "url": "https://nptel.ac.in/", "source": "NPTEL", "cost": "free"},
        ],
    },
    "civil engineer": {
        "technical_skills": ["Structural Analysis", "AutoCAD", "Revit", "BIM", "Geotechnical", "Project Estimation"],
        "tools": ["AutoCAD", "Revit", "STAAD.Pro", "ETABS"],
        "salary": {"global_average_usd": 88000, "regional_breakdown": {"US": {"avg_min": 68000, "avg_max": 115000}, "EU": {"avg_min": 42000, "avg_max": 72000}, "India": {"avg_min": 11000, "avg_max": 24000}}},
        "career_path": ["Site Engineer", "Civil Engineer", "Senior Civil Engineer", "Project Engineer"],
        "courses": [
            {"title": "MIT OCW Civil Engineering", "url": "https://ocw.mit.edu/", "source": "MIT OpenCourseWare", "cost": "free"},
            {"title": "Autodesk Design Academy", "url": "https://academy.autodesk.com/", "source": "Autodesk", "cost": "free"},
        ],
    },
    "electrical engineer": {
        "technical_skills": ["Circuits", "Power Systems", "Control Systems", "PLC", "MATLAB", "Embedded Systems"],
        "tools": ["MATLAB", "Simulink", "PSCAD", "AutoCAD Electrical"],
        "salary": {"global_average_usd": 98000, "regional_breakdown": {"US": {"avg_min": 76000, "avg_max": 128000}, "EU": {"avg_min": 48000, "avg_max": 82000}, "India": {"avg_min": 13000, "avg_max": 30000}}},
        "career_path": ["Electrical Engineer I", "Electrical Engineer", "Senior Electrical Engineer", "Controls / Systems Lead"],
        "courses": [
            {"title": "All About Circuits", "url": "https://www.allaboutcircuits.com/", "source": "All About Circuits", "cost": "free"},
            {"title": "NPTEL Electrical Engineering", "url": "https://nptel.ac.in/", "source": "NPTEL", "cost": "free"},
        ],
    },
    "cybersecurity analyst": {
        "technical_skills": ["Network Security", "Linux", "Incident Response", "SIEM", "Python", "OWASP"],
        "tools": ["Wireshark", "Splunk", "Burp Suite Community", "Nmap"],
        "salary": {"global_average_usd": 112000, "regional_breakdown": {"US": {"avg_min": 85000, "avg_max": 150000}, "EU": {"avg_min": 55000, "avg_max": 98000}, "India": {"avg_min": 17000, "avg_max": 42000}}},
        "career_path": ["SOC Analyst", "Cybersecurity Analyst", "Senior Security Analyst", "Security Engineer"],
        "courses": [
            {"title": "picoCTF", "url": "https://picoctf.org/", "source": "Carnegie Mellon University", "cost": "free"},
            {"title": "Open Security Training", "url": "https://opensecuritytraining.info/", "source": "OST2", "cost": "free"},
        ],
    },
    "devops engineer": {
        "technical_skills": ["Linux", "Docker", "Kubernetes", "Terraform", "CI/CD", "Cloud"],
        "tools": ["Docker", "Kubernetes", "Terraform", "GitHub Actions", "Prometheus"],
        "salary": {"global_average_usd": 128000, "regional_breakdown": {"US": {"avg_min": 98000, "avg_max": 170000}, "EU": {"avg_min": 65000, "avg_max": 112000}, "India": {"avg_min": 22000, "avg_max": 52000}}},
        "career_path": ["Cloud / Ops Engineer", "DevOps Engineer", "Senior DevOps Engineer", "Platform Engineer"],
        "courses": [
            {"title": "Kubernetes Basics", "url": "https://kubernetes.io/docs/tutorials/kubernetes-basics/", "source": "Kubernetes", "cost": "free"},
            {"title": "HashiCorp Learn", "url": "https://developer.hashicorp.com/terraform/tutorials", "source": "HashiCorp", "cost": "free"},
        ],
    },
}

CURATED_UNIVERSITIES = [
    {"name": "MIT", "country": "United States", "program": "Engineering / Computer Science", "url": "https://www.mit.edu/", "source": "curated"},
    {"name": "ETH Zurich", "country": "Switzerland", "program": "Engineering / Robotics / CS", "url": "https://ethz.ch/", "source": "curated"},
    {"name": "TU Delft", "country": "Netherlands", "program": "Civil / Mechanical / Electrical Engineering", "url": "https://www.tudelft.nl/", "source": "curated"},
    {"name": "Carnegie Mellon University", "country": "United States", "program": "Computer Science / AI / Robotics", "url": "https://www.cmu.edu/", "source": "curated"},
    {"name": "National University of Singapore", "country": "Singapore", "program": "Engineering / Computing", "url": "https://www.nus.edu.sg/", "source": "curated"},
]

CURATED_SCHOLARSHIPS = [
    {"name": "DAAD Scholarships", "country": "Germany", "amount": "Varies", "deadline": "Varies", "eligibility": "Graduate technical programs", "url": "https://www.daad.de/en/study-and-research-in-germany/scholarships/", "source": "curated"},
    {"name": "Erasmus Mundus Catalogue", "country": "Europe", "amount": "Varies", "deadline": "Varies", "eligibility": "International master's applicants", "url": "https://www.eacea.ec.europa.eu/scholarships/erasmus-mundus-catalogue_en", "source": "curated"},
    {"name": "Fulbright Foreign Student Program", "country": "United States", "amount": "Varies", "deadline": "Varies", "eligibility": "Postgraduate study", "url": "https://foreign.fulbrightonline.org/", "source": "curated"},
]

CURATED_INTERNSHIPS = [
    {"title": "Open Source Contributor Program", "company": "Google Summer of Code", "location": "Remote", "duration": "12+ weeks", "stipend": "Program stipend", "source": "curated", "url": "https://summerofcode.withgoogle.com/"},
    {"title": "Linux Foundation Mentorship", "company": "Linux Foundation", "location": "Remote", "duration": "3-6 months", "stipend": "Varies", "source": "curated", "url": "https://mentorship.lfx.linuxfoundation.org/"},
]

NON_TECH_NOTICE = (
    "Horizon v5 is configured for technical fields only. The request was handled gracefully, "
    "but the role does not look primarily technical, so only limited guidance is returned."
)


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


def _normalize_query(query: str) -> str:
    return re.sub(r"\s+", " ", (query or "").strip())


def _match_profile(query: str) -> Optional[str]:
    q = query.lower()
    for role in ROLE_PROFILES:
        if role in q:
            return role
    if "software" in q or "backend" in q or "frontend" in q or "developer" in q:
        return "software engineer"
    if "data" in q and ("scientist" in q or "science" in q):
        return "data scientist"
    if "cyber" in q or "security" in q:
        return "cybersecurity analyst"
    if "devops" in q or "platform engineer" in q or "site reliability" in q:
        return "devops engineer"
    if "mechanical" in q:
        return "mechanical engineer"
    if "civil" in q:
        return "civil engineer"
    if "electrical" in q:
        return "electrical engineer"
    return None


def _is_technical_query(query: str) -> bool:
    lowered = query.lower()
    return any(keyword in lowered for keyword in TECH_KEYWORDS)


def _domain(url: str) -> str:
    return urlparse(url).netloc.lower()


async def _bounded_sleep(seconds: float) -> None:
    await asyncio.sleep(max(0.0, min(seconds, 2.0)))


async def _rate_limit(domain: str) -> None:
    delay = max(0.0, min(_RATE_LIMITS.get(domain, 0.2), 2.0))
    async with _RATE_LOCK:
        now = time.monotonic()
        last = _LAST_REQUEST.get(domain, 0.0)
        wait_for = max(0.0, delay - (now - last))
        if wait_for > 0:
            await asyncio.sleep(min(wait_for, 2.0))
            now = time.monotonic()
        _LAST_REQUEST[domain] = now


async def _respects_robots(client: httpx.AsyncClient, url: str) -> bool:
    domain = _domain(url)
    async with _ROBOTS_LOCK:
        cached = _ROBOTS_CACHE.get(domain)
        if cached is not None:
            return cached
        allowed = domain in _RATE_LIMITS or domain.endswith("wikipedia.org")
        _ROBOTS_CACHE[domain] = allowed
        return allowed


async def _make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=DEFAULT_HEADERS, follow_redirects=True)


async def _fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    expect: str = "json",
    headers: Optional[dict[str, str]] = None,
) -> Any:
    if not await _respects_robots(client, url):
        raise RuntimeError(f"Robots policy disallows access to {url}")
    domain = _domain(url)
    started = time.monotonic()
    attempts = 0
    last_error: Exception | None = None
    while time.monotonic() - started < MAX_RETRY_SECONDS and attempts < 3:
        attempts += 1
        try:
            await _rate_limit(domain)
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            if expect == "json":
                return response.json()
            if expect == "text":
                return response.text
            return response.content
        except Exception as exc:  # pragma: no cover - network error path
            last_error = exc
            remaining = MAX_RETRY_SECONDS - (time.monotonic() - started)
            if remaining <= 0 or attempts >= 3:
                break
            await _bounded_sleep(min(0.5 * (2 ** (attempts - 1)), remaining, 2.0))
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Failed to fetch {url}")


async def _fetch_json(client: httpx.AsyncClient, url: str, *, headers: Optional[dict[str, str]] = None) -> Any:
    return await _fetch_with_retry(client, url, expect="json", headers=headers)


async def _fetch_text(client: httpx.AsyncClient, url: str, *, headers: Optional[dict[str, str]] = None) -> str:
    return await _fetch_with_retry(client, url, expect="text", headers=headers)


async def _fetch_bytes(client: httpx.AsyncClient, url: str, *, headers: Optional[dict[str, str]] = None) -> bytes:
    return await _fetch_with_retry(client, url, expect="bytes", headers=headers)


def extract_skills_regex(text: str) -> list[str]:
    if not text:
        return []
    lowered = text.lower()
    found = []
    for skill in SKILL_LEXICON:
        pattern = re.escape(skill.lower())
        if re.search(rf"(?<!\w){pattern}(?!\w)", lowered):
            found.append(skill)
    return sorted(dict.fromkeys(found))


def _extract_tools_from_text(text: str) -> list[str]:
    return extract_skills_regex(text)[:12]


def normalize_skill(skill: str) -> str:
    return re.sub(r"\s+", " ", (skill or "").strip().lower()).replace("_", " ")


def extract_skills_keybert(text: str, top_n: int = 25) -> list[tuple[str, float]]:
    skills = extract_skills_regex(text)[:top_n]
    if not skills:
        return []
    max_rank = max(len(skills), 1)
    return [(skill, round(1 - (idx / max_rank), 3)) for idx, skill in enumerate(skills)]


def _filter_jobs(jobs: list[JobListing], query: str, remote_only: bool, limit: int) -> list[JobListing]:
    q_words = {w for w in re.findall(r"[a-z0-9+#.-]+", query.lower()) if len(w) > 1}
    ranked: list[JobListing] = []
    for job in jobs:
        haystack = " ".join([job.title, job.description, " ".join(job.skills), job.company]).lower()
        overlap = sum(1 for word in q_words if word in haystack)
        if q_words and overlap == 0:
            continue
        if remote_only and not job.remote:
            continue
        ranked.append(job.model_copy(update={"relevance_score": float(overlap or 1)}))
    ranked.sort(key=lambda item: (-item.relevance_score, item.title.lower()))
    deduped: list[JobListing] = []
    seen: set[tuple[str, str]] = set()
    for job in ranked:
        key = (job.title.lower().strip(), job.company.lower().strip())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(job)
        if len(deduped) >= limit:
            break
    return deduped


async def _search_arbeitnow(client: httpx.AsyncClient, query: str, limit: int) -> list[JobListing]:
    url = "https://www.arbeitnow.com/api/job-board-api"
    payload = await _fetch_json(client, url)
    jobs: list[JobListing] = []
    for item in payload.get("data", []):
        title = item.get("title", "")
        description = re.sub(r"<[^>]+>", " ", item.get("description", ""))
        if query.lower() not in f"{title} {description}".lower() and query.lower() not in title.lower():
            continue
        jobs.append(
            JobListing(
                id=str(item.get("slug") or item.get("url") or title),
                title=title,
                company=item.get("company_name", "Unknown"),
                location=item.get("location", "Remote"),
                description=re.sub(r"\s+", " ", description).strip(),
                url=item.get("url", ""),
                source="Arbeitnow",
                skills=item.get("tags", []) or extract_skills_regex(description),
                job_type=item.get("job_types", [""])[0] if item.get("job_types") else "",
                remote="remote" in (item.get("location", "") or "").lower() or item.get("remote", False),
                country=item.get("location", ""),
            )
        )
        if len(jobs) >= limit:
            break
    return jobs


async def _search_remoteok(client: httpx.AsyncClient, query: str, limit: int) -> list[JobListing]:
    url = "https://remoteok.com/api"
    payload = await _fetch_json(client, url, headers={**DEFAULT_HEADERS, "Accept": "application/json"})
    jobs: list[JobListing] = []
    for item in payload:
        if not isinstance(item, dict) or not item.get("position"):
            continue
        title = item.get("position", "")
        description = item.get("description", "") or ""
        if query.lower() not in f"{title} {description}".lower():
            continue
        jobs.append(
            JobListing(
                id=str(item.get("id") or item.get("slug") or title),
                title=title,
                company=item.get("company", "Unknown"),
                location=item.get("location") or "Remote",
                description=description[:1200],
                url=item.get("url") or item.get("apply_url") or "",
                source="RemoteOK",
                salary_min=item.get("salary_min"),
                salary_max=item.get("salary_max"),
                skills=item.get("tags") or extract_skills_regex(description),
                job_type="Remote",
                remote=True,
                country=item.get("location") or "Remote",
            )
        )
        if len(jobs) >= limit:
            break
    return jobs


async def _search_adzuna(client: httpx.AsyncClient, query: str, country: str, limit: int) -> list[JobListing]:
    app_id = os.getenv("ADZUNA_APP_ID", "").strip()
    app_key = os.getenv("ADZUNA_APP_KEY", "").strip()
    if not app_id or not app_key:
        return []
    country_key = re.sub(r"[^a-z0-9]+", " ", (country or "united states").lower()).strip()
    country_code = COUNTRY_TO_ISO_ALPHA2.get(country_key)
    if country_code is None and re.fullmatch(r"[a-z]{2}", country_key):
        country_code = country_key
    if country_code is None:
        country_code = "us"
    url = (
        f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/1?"
        f"app_id={app_id}&app_key={app_key}&results_per_page={limit}&what={quote_plus(query)}"
    )
    payload = await _fetch_json(client, url)
    jobs: list[JobListing] = []
    for item in payload.get("results", []):
        jobs.append(
            JobListing(
                id=str(item.get("id") or item.get("redirect_url") or item.get("title", "")),
                title=item.get("title", ""),
                company=(item.get("company") or {}).get("display_name", "Unknown"),
                location=(item.get("location") or {}).get("display_name", ""),
                description=re.sub(r"<[^>]+>", " ", item.get("description", ""))[:1200],
                url=item.get("redirect_url", ""),
                source="Adzuna",
                salary_min=item.get("salary_min"),
                salary_max=item.get("salary_max"),
                skills=extract_skills_regex(item.get("description", "")),
                remote="remote" in item.get("title", "").lower(),
                country=country_code.upper(),
            )
        )
    return jobs


async def _search_usajobs(client: httpx.AsyncClient, query: str, limit: int) -> list[JobListing]:
    api_key = os.getenv("USAJOBS_API_KEY", "").strip()
    user_agent = os.getenv("USAJOBS_USER_AGENT", DEFAULT_HEADERS["User-Agent"])
    if not api_key:
        return []
    url = f"https://data.usajobs.gov/api/search?Keyword={quote_plus(query)}&ResultsPerPage={limit}"
    payload = await _fetch_json(
        client,
        url,
        headers={"Host": "data.usajobs.gov", "User-Agent": user_agent, "Authorization-Key": api_key},
    )
    jobs: list[JobListing] = []
    for item in payload.get("SearchResult", {}).get("SearchResultItems", []):
        desc = item.get("MatchedObjectDescriptor", {})
        jobs.append(
            JobListing(
                id=str(desc.get("PositionID") or desc.get("PositionURI") or desc.get("PositionTitle", "")),
                title=desc.get("PositionTitle", ""),
                company=desc.get("OrganizationName", "USAJobs"),
                location=", ".join(loc.get("LocationName", "") for loc in desc.get("PositionLocation", [])),
                description=(desc.get("UserArea", {}).get("Details", {}).get("JobSummary") or "")[:1200],
                url=desc.get("PositionURI", ""),
                source="USAJobs",
                salary_min=(desc.get("PositionRemuneration", [{}])[0]).get("MinimumRange"),
                salary_max=(desc.get("PositionRemuneration", [{}])[0]).get("MaximumRange"),
                skills=extract_skills_regex((desc.get("UserArea", {}).get("Details", {}).get("JobSummary") or "")),
                remote=False,
                country="US",
            )
        )
    return jobs


async def _fetch_wikipedia_summary(client: httpx.AsyncClient, query: str) -> dict[str, Any]:
    title = query.replace(" ", "_")
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{quote_plus(title)}"
    try:
        payload = await _fetch_json(client, url)
        text = payload.get("extract", "")
        return {
            "title": payload.get("title") or query.title(),
            "description": text,
            "skills": extract_skills_regex(text),
            "url": payload.get("content_urls", {}).get("desktop", {}).get("page", f"https://en.wikipedia.org/wiki/{title}"),
        }
    except Exception:
        return {"title": query.title(), "description": "", "skills": [], "url": f"https://en.wikipedia.org/wiki/{title}"}


async def _fetch_esco_skills(client: httpx.AsyncClient, query: str) -> list[str]:
    async with _SPARQL_LOCK:
        try:
            url = f"https://ec.europa.eu/esco/api/search?language=en&type=occupation&text={quote_plus(query)}&limit=3"
            payload = await _fetch_json(client, url)
            results = payload.get("_embedded", {}).get("results", [])
            terms = []
            for item in results:
                if item.get("title"):
                    terms.extend(extract_skills_regex(item["title"]))
                if item.get("description"):
                    terms.extend(extract_skills_regex(item["description"]))
            return list(dict.fromkeys(terms))[:10]
        except Exception:
            return []


async def _fetch_github_repos(client: httpx.AsyncClient, query: str, limit: int = 5) -> list[dict[str, Any]]:
    url = f"https://api.github.com/search/repositories?q={quote_plus(query)}&sort=stars&order=desc&per_page={limit}"
    try:
        payload = await _fetch_json(client, url)
    except Exception:
        return []
    repos = []
    for item in payload.get("items", []):
        repos.append(
            {
                "name": item.get("full_name", ""),
                "url": item.get("html_url", ""),
                "description": item.get("description", "") or "",
                "stars": item.get("stargazers_count", 0),
                "language": item.get("language", ""),
            }
        )
    return repos


async def _fetch_hn_posts(client: httpx.AsyncClient, query: str, limit: int = 5) -> list[dict[str, Any]]:
    url = f"https://hn.algolia.com/api/v1/search?query={quote_plus(query)}&hitsPerPage={limit}&tags=story"
    try:
        payload = await _fetch_json(client, url)
    except Exception:
        return []
    posts = []
    for item in payload.get("hits", []):
        posts.append(
            {
                "title": item.get("title") or item.get("story_title") or "",
                "url": item.get("url") or item.get("story_url") or "https://news.ycombinator.com/item?id=" + str(item.get("objectID")),
                "points": item.get("points", 0),
            }
        )
    return posts


async def _fetch_arxiv_papers(client: httpx.AsyncClient, query: str, limit: int = 5) -> list[dict[str, Any]]:
    url = f"https://export.arxiv.org/api/query?search_query=all:{quote_plus(query)}&start=0&max_results={limit}"
    try:
        xml_text = await _fetch_text(client, url)
    except Exception:
        return []
    root = ET.fromstring(xml_text)
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    papers = []
    for entry in root.findall("atom:entry", ns):
        title = (entry.findtext("atom:title", default="", namespaces=ns) or "").strip()
        summary = (entry.findtext("atom:summary", default="", namespaces=ns) or "").strip()
        link = ""
        for link_node in entry.findall("atom:link", ns):
            if link_node.attrib.get("rel") == "alternate":
                link = link_node.attrib.get("href", "")
                break
        papers.append({"title": title, "summary": summary[:700], "url": link})
    return papers


def _salary_from_profile(profile_key: Optional[str]) -> dict[str, Any]:
    if profile_key and profile_key in ROLE_PROFILES:
        return ROLE_PROFILES[profile_key]["salary"]
    return {
        "global_average_usd": 95000,
        "regional_breakdown": {
            "US": {"avg_min": 70000, "avg_max": 130000},
            "EU": {"avg_min": 45000, "avg_max": 85000},
            "India": {"avg_min": 12000, "avg_max": 30000},
        },
    }


def _courses_for_profile(profile_key: Optional[str]) -> list[dict[str, str]]:
    if profile_key and profile_key in ROLE_PROFILES:
        return ROLE_PROFILES[profile_key]["courses"]
    return [
        {"title": "freeCodeCamp", "url": "https://www.freecodecamp.org/", "source": "freeCodeCamp", "cost": "free"},
        {"title": "MIT OpenCourseWare", "url": "https://ocw.mit.edu/", "source": "MIT OCW", "cost": "free"},
    ]


def _career_path(profile_key: Optional[str], query: str) -> list[dict[str, str]]:
    if profile_key and profile_key in ROLE_PROFILES:
        titles = ROLE_PROFILES[profile_key]["career_path"]
    else:
        base = query.title()
        titles = [f"Junior {base}", base, f"Senior {base}", f"Lead {base}"]
    stages = []
    labels = ["Foundation", "Execution", "Ownership", "Leadership"]
    for label, title in zip(labels, titles, strict=False):
        stages.append({"stage": label, "title": title, "focus": f"Grow toward {title}"})
    return stages


def _curated_jobs(query: str, limit: int = 6) -> list[JobListing]:
    title = query.title()
    return [
        JobListing(
            id=f"curated-{idx}",
            title=title,
            company=company,
            location=location,
            description=f"Curated technical-role example for {title}.",
            url=url,
            source="Curated fallback",
            remote=location.lower() == "remote",
            skills=extract_skills_regex(title),
            relevance_score=1.0,
        )
        for idx, (company, location, url) in enumerate(
            [
                ("Open Source Engineering", "Remote", "https://github.com/explore"),
                ("Public Sector Technology", "United States", "https://www.usajobs.gov/"),
                ("European Tech Careers", "Europe", "https://www.euraxess.org/"),
                ("Global Research Labs", "Remote", "https://arxiv.org/"),
                ("Manufacturing Innovation Hub", "Germany", "https://www.make-it-in-germany.com/en/working-in-germany/job-listings"),
                ("Infrastructure Systems Group", "India", "https://www.naukri.com/"),
            ],
            start=1,
        )
    ][:limit]


def _regions_from_jobs(jobs: list[JobListing]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for job in jobs:
        region = job.country or job.location or "Unknown"
        counts[region] += 1
    return dict(counts.most_common(8))


def _skill_summary(jobs: list[JobListing], extra_skills: list[str], profile_key: Optional[str]) -> tuple[list[str], list[str]]:
    counter: Counter[str] = Counter()
    for job in jobs:
        for skill in job.skills:
            counter[skill.title()] += 1
        counter.update(skill.title() for skill in extract_skills_regex(job.description))
    for skill in extra_skills:
        counter[skill.title()] += 2
    if profile_key and profile_key in ROLE_PROFILES:
        for skill in ROLE_PROFILES[profile_key]["technical_skills"]:
            counter[skill] += 3
    technical = [name for name, _ in counter.most_common(12)]
    soft = SOFT_SKILLS[:4]
    return technical, soft


class GlobalCareerIntelligencePipeline:
    def __init__(self) -> None:
        self.data_source_labels = [
            "Wikipedia REST",
            "ESCO API",
            "GitHub REST",
            "Hacker News Algolia",
            "arXiv API",
            "Arbeitnow API",
            "RemoteOK JSON",
            "Curated technical datasets",
        ]

    async def process_field(self, field_or_role: str, optional_filters: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        optional_filters = optional_filters or {}
        query = _normalize_query(field_or_role)
        started = time.time()
        profile_key = _match_profile(query)
        technical_query = _is_technical_query(query) or profile_key is not None

        if not technical_query:
            return {
                "field_name": query.title(),
                "interpreted_input": query,
                "confidence_score": "35%",
                "regions_covered": [],
                "sources_used": ["Curated technical datasets"],
                "data_sources": ["Curated technical datasets"],
                "jobs": [],
                "job_roles": [],
                "skills": {"technical": [], "soft": []},
                "tools_and_technologies": [],
                "companies": [],
                "locations": [],
                "salary": _salary_from_profile(None),
                "demand_by_region": {},
                "career_path": _career_path(None, query),
                "remote_opportunities": [],
                "free_resources": _courses_for_profile(None),
                "courses": _courses_for_profile(None),
                "universities": CURATED_UNIVERSITIES[:3],
                "scholarships": CURATED_SCHOLARSHIPS[:2],
                "internships": CURATED_INTERNSHIPS[:1],
                "research_papers": [],
                "top_companies": [],
                "data_gaps": [NON_TECH_NOTICE],
                "errors": [],
                "partial_results": True,
                "timestamp": time.time(),
                "technical_only_notice": NON_TECH_NOTICE,
            }

        remote_only = bool(optional_filters.get("remote_only", False))
        country = optional_filters.get("country", "")
        async with await _make_client() as client:
            tasks = [
                asyncio.create_task(_search_arbeitnow(client, query, 10)),
                asyncio.create_task(_search_remoteok(client, query, 10)),
                asyncio.create_task(_search_adzuna(client, query, country, 10)),
                asyncio.create_task(_search_usajobs(client, query, 10)),
                asyncio.create_task(_fetch_wikipedia_summary(client, query)),
                asyncio.create_task(_fetch_esco_skills(client, query)),
                asyncio.create_task(_fetch_github_repos(client, query, 5)),
                asyncio.create_task(_fetch_hn_posts(client, query, 5)),
                asyncio.create_task(_fetch_arxiv_papers(client, query, 5)),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        arbeitnow_jobs, remoteok_jobs, adzuna_jobs, usajobs_jobs, wiki_info, esco_skills, repos, hn_posts, papers = results
        errors: list[str] = []
        collected_jobs: list[JobListing] = []
        sources_active: list[str] = []

        for label, payload in [
            ("Arbeitnow API", arbeitnow_jobs),
            ("RemoteOK JSON", remoteok_jobs),
            ("Adzuna Jobs API", adzuna_jobs),
            ("USAJobs API", usajobs_jobs),
        ]:
            if isinstance(payload, Exception):
                errors.append(f"{label}: {payload}")
            elif payload:
                sources_active.append(label)
                collected_jobs.extend(payload)

        if isinstance(wiki_info, Exception):
            wiki_info = {"title": query.title(), "description": "", "skills": [], "url": ""}
            errors.append(f"Wikipedia REST: {wiki_info}")
        else:
            sources_active.append("Wikipedia REST")

        if isinstance(esco_skills, Exception):
            esco_skills = []
        elif esco_skills:
            sources_active.append("ESCO API")

        if isinstance(repos, Exception):
            repos = []
        elif repos:
            sources_active.append("GitHub REST")

        if isinstance(hn_posts, Exception):
            hn_posts = []
        elif hn_posts:
            sources_active.append("Hacker News Algolia")

        if isinstance(papers, Exception):
            papers = []
        elif papers:
            sources_active.append("arXiv API")

        if not collected_jobs:
            collected_jobs = _curated_jobs(query)
            sources_active.append("Curated fallback")

        jobs = _filter_jobs(collected_jobs, query, remote_only, 12)
        if not jobs:
            jobs = _curated_jobs(query)
            if "Curated fallback" not in sources_active:
                sources_active.append("Curated fallback")

        technical_skills, soft_skills = _skill_summary(
            jobs,
            list(getattr(wiki_info, "get", lambda *_: [])("skills", [])) + (esco_skills if isinstance(esco_skills, list) else []),
            profile_key,
        )
        tools = technical_skills[:8]
        if profile_key and profile_key in ROLE_PROFILES:
            tools = list(dict.fromkeys(ROLE_PROFILES[profile_key]["tools"] + tools))[:10]
        salary = _salary_from_profile(profile_key)
        average_salary = salary["global_average_usd"]
        companies = list(dict.fromkeys([job.company for job in jobs if job.company]))[:10]
        locations = list(dict.fromkeys([job.location for job in jobs if job.location]))[:10]
        internships = CURATED_INTERNSHIPS[:2] if "intern" not in query.lower() else CURATED_INTERNSHIPS
        course_items = _courses_for_profile(profile_key)
        free_resources = [
            {
                "title": item["title"],
                "url": item["url"],
                "provider": item.get("source", "Unknown"),
                "type": "course",
                "free": "free" in item.get("cost", "").lower(),
            }
            for item in course_items
        ]
        demand_by_region = _regions_from_jobs(jobs)
        regions_covered = list(demand_by_region.keys()) or list(salary["regional_breakdown"].keys())
        remote_jobs = [job for job in jobs if job.remote]
        remote_percentage = round((len(remote_jobs) / max(len(jobs), 1)) * 100, 1)
        remote_opportunities = [
            {
                "insight": f"{job.title} at {job.company} via {job.source}",
                "regions_with_remote": [job.location or "Remote"],
                "remote_percentage": remote_percentage,
            }
            for job in remote_jobs[:6]
        ]
        if not remote_opportunities:
            remote_opportunities = [
                {
                    "insight": "Remote-first roles are available through RemoteOK and curated open-source programs.",
                    "regions_with_remote": ["Remote"],
                    "remote_percentage": 0.0,
                }
            ]
        career_path = []
        for stage in _career_path(profile_key, query):
            career_path.append(
                {
                    "level": stage["stage"],
                    "average_salary_range": {
                        "min": salary["regional_breakdown"].get("US", {}).get("avg_min", average_salary),
                        "max": salary["regional_breakdown"].get("US", {}).get("avg_max", average_salary),
                    },
                    "typical_roles": [stage["title"]],
                }
            )

        result = {
            "field_name": query.title(),
            "interpreted_input": query,
            "confidence_score": "85%" if jobs else "55%",
            "regions_covered": regions_covered,
            "sources_used": list(dict.fromkeys(sources_active)),
            "data_sources": list(dict.fromkeys(sources_active)),
            "jobs": [job.model_dump() for job in jobs],
            "job_roles": [job.title for job in jobs[:8]],
            "skills": {"technical": technical_skills, "soft": soft_skills},
            "tools_and_technologies": tools,
            "companies": companies,
            "locations": locations,
            "salary": salary,
            "demand_by_region": demand_by_region,
            "career_path": career_path,
            "remote_opportunities": remote_opportunities,
            "free_resources": free_resources,
            "courses": course_items,
            "universities": CURATED_UNIVERSITIES[:4],
            "scholarships": CURATED_SCHOLARSHIPS[:3],
            "internships": internships,
            "research_papers": papers[:5],
            "top_companies": [{"name": company, "count": 1} for company in companies[:5]],
            "github_repositories": repos[:5],
            "news_signals": hn_posts[:5],
            "summary": (wiki_info.get("description", "") if isinstance(wiki_info, dict) else "") or f"Technical field intelligence for {query}.",
            "data_gaps": [
                "Salary figures are estimated from curated benchmarks unless a free live source returns reliable values.",
                "Universities and scholarships are curated because a stable free global API is not available.",
            ],
            "errors": errors[:8],
            "partial_results": bool(errors) or "Curated fallback" in sources_active,
            "timestamp": time.time(),
            "response_time_ms": round((time.time() - started) * 1000),
        }
        return result


class AdaptiveIntelligenceController:
    def __init__(self) -> None:
        self.pipeline = GlobalCareerIntelligencePipeline()

    async def process_field(self, field_or_role: str, optional_filters: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        return await self.pipeline.process_field(field_or_role, optional_filters)


class DynamicTrackGenerator:
    def __init__(self, llm_available: bool = False) -> None:
        self.llm_available = llm_available

    def generate_track_from_goal(self, goal: str, market_data: dict[str, Any], skill_frequencies: dict[str, Any]) -> dict[str, Any]:
        track_map = {
            "data scientist": "data_science",
            "machine learning": "ml",
            "ai research": "ai_research",
            "backend": "backend",
            "frontend": "frontend",
            "cyber": "cybersecurity",
            "security": "cybersecurity",
            "devops": "devops",
            "cloud": "cloud_architect",
            "bioinformatics": "bioinformatics",
            "data engineering": "data_engineering",
        }
        lowered = goal.lower()
        chosen = next((value for key, value in track_map.items() if key in lowered), "backend" if "engineer" in lowered else "ml")
        top_skills = [name for name, _ in Counter({k: int(v.get('frequency_score', 1) * 100) if isinstance(v, dict) else int(v) for k, v in skill_frequencies.items()}).most_common(8)]
        return {
            "name": goal.title(),
            "keywords": [goal.lower(), chosen.replace("_", " ")],
            "core_skills": top_skills or extract_skills_regex(goal) or ["python", "git", "sql"],
            "description": f"Dynamic technical roadmap for {goal}.",
            "salary_range": [60000, 140000],
            "track_id": chosen,
        }


class Analyzer:
    def analyze(self, market_data: dict[str, Any], goal: str, _: str) -> dict[str, dict[str, float]]:
        texts: list[str] = []
        for key in ["job_listings", "hn_jobs", "github_trends", "arxiv_papers"]:
            for item in market_data.get(key, []):
                if isinstance(item, dict):
                    texts.append(" ".join(str(value) for value in item.values() if isinstance(value, str)))
        if not texts:
            texts.append(goal)
        counter: Counter[str] = Counter()
        for text in texts:
            counter.update(skill.lower() for skill in extract_skills_regex(text))
        profile_key = _match_profile(goal)
        if profile_key and profile_key in ROLE_PROFILES:
            counter.update(skill.lower() for skill in ROLE_PROFILES[profile_key]["technical_skills"])
        if not counter:
            counter.update(["python", "sql", "git"])
        max_count = max(counter.values()) or 1
        scores: dict[str, dict[str, float]] = {}
        for skill, count in counter.items():
            freq = round(count / max_count, 3)
            trend = round(min(1.0, 0.35 + freq / 2), 3)
            relevance = round(min(1.0, 0.4 + freq / 2), 3)
            scores[skill.replace(" ", "_")] = {
                "frequency_score": freq,
                "trend_score": trend,
                "relevance_score": relevance,
            }
        return scores


def _get_kw_model() -> None:
    return None


def _get_embed_model() -> None:
    return None


def get_dynamic_tracks(goal: str, market_data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    scores = Analyzer().analyze(market_data, goal, goal)
    track = DynamicTrackGenerator().generate_track_from_goal(goal, market_data, scores)
    return {goal: track}


async def search_jobs(
    query: str = "",
    location: str = "",
    limit: int = 30,
    remote_only: bool = False,
    country: str = "",
    sort_by: str = "relevance",
) -> JobSearchResponse:
    started = time.time()
    pipeline = GlobalCareerIntelligencePipeline()
    data = await pipeline.process_field(query, {"remote_only": remote_only, "country": country or location})
    jobs = [JobListing.model_validate(item) for item in data.get("jobs", [])]
    salary = data.get("salary", {})
    salary_range = {
        "estimated": True,
        "global_average_usd": salary.get("global_average_usd"),
        "regional_breakdown": salary.get("regional_breakdown", {}),
    }
    trending_skills = [{"skill": skill, "count": idx + 1} for idx, skill in enumerate(data.get("skills", {}).get("technical", [])[:8])]
    top_companies = [{"name": company, "count": 1} for company in data.get("companies", [])[:5]]
    if sort_by == "company":
        jobs = sorted(jobs, key=lambda job: (job.company.lower(), -job.relevance_score))
    return JobSearchResponse(
        query=query,
        location=location,
        total=len(jobs),
        jobs=jobs[:limit],
        sources_active=data.get("sources_used", []),
        trending_skills=trending_skills,
        top_companies=top_companies,
        salary_range=salary_range,
        took_ms=round((time.time() - started) * 1000),
    )


async def live_search(query: str, country: str = "", remote_only: bool = False) -> dict[str, Any]:
    data = await GlobalCareerIntelligencePipeline().process_field(query, {"country": country, "remote_only": remote_only})
    return {
        "query": query,
        "timestamp": data.get("timestamp", time.time()),
        "sources_active": data.get("sources_used", []),
        "jobs": data.get("jobs", []),
        "internships": data.get("internships", []),
        "salary_data": [
            {
                "region": region,
                "min": values.get("avg_min"),
                "max": values.get("avg_max"),
                "estimated": True,
            }
            for region, values in data.get("salary", {}).get("regional_breakdown", {}).items()
        ],
        "all_salary_data": data.get("salary", {}),
        "skill_trends": data.get("skills", {}).get("technical", []),
        "scholarships": data.get("scholarships", []),
        "universities": data.get("universities", []),
        "research_papers": data.get("research_papers", []),
        "top_companies": data.get("top_companies", []),
        "total_jobs": len(data.get("jobs", [])),
        "total_internships": len(data.get("internships", [])),
    }


async def search_internships(query: str, country: str = "", limit: int = 10) -> dict[str, Any]:
    query_lower = query.lower()
    curated = CURATED_INTERNSHIPS.copy()
    if "security" in query_lower or "cyber" in query_lower:
        curated = curated + [{"title": "Blue Team Internship", "company": "Open SOC Lab", "location": "Remote", "duration": "10 weeks", "stipend": "Varies", "source": "curated", "url": "https://opensecuritytraining.info/"}]
    elif "data" in query_lower:
        curated = curated + [{"title": "Data Science Fellowship", "company": "Open Data Lab", "location": "Remote", "duration": "8 weeks", "stipend": "Varies", "source": "curated", "url": "https://www.kaggle.com/learn"}]
    return {
        "query": query,
        "country": country,
        "internships": curated[:limit],
        "sources_active": ["Curated internships"],
    }


async def skills_autocomplete(prefix: str, limit: int = 12) -> dict[str, Any]:
    prefix = prefix.lower().strip()
    suggestions = [skill.title() for skill in SKILL_LEXICON if skill.startswith(prefix)]
    if not suggestions:
        suggestions = [skill.title() for skill in SKILL_LEXICON if prefix in skill][:limit]
    return {"query": prefix, "skills": suggestions[:limit]}


async def fetch_all_market_data(goal: str) -> dict[str, Any]:
    data = await GlobalCareerIntelligencePipeline().process_field(goal, {})
    return {
        "job_listings": data.get("jobs", []),
        "hn_jobs": data.get("news_signals", []),
        "github_trends": data.get("github_repositories", []),
        "arxiv_papers": data.get("research_papers", []),
        "scholarships": data.get("scholarships", []),
        "universities": data.get("universities", []),
        "salary_data": data.get("salary", {}),
        "internships": data.get("internships", []),
        "trend_analysis": [{"skill": skill} for skill in data.get("skills", {}).get("technical", [])],
    }
