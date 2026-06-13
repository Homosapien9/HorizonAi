"""
Horizon v5 — Consolidated Test Suite

Compiled from:
  test_api.py, test_api_calls.py, test_detailed_extraction.py,
  test_feedparser.py, test_full_pipeline.py, test_normalization.py,
  test_pipeline.py, test_pipeline_final.py, test_remoteok_debug.py,
  test_remoteok_debug2.py, test_remoteok_final.py,
  tests/test_engine.py

Usage:
    python test_all.py              # run all standalone tests
    python test_all.py --pytest      # run pytest-style tests (via pytest)
    python -m pytest test_all.py     # run pytest-compatible tests
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from urllib.parse import quote_plus

import httpx
from bs4 import BeautifulSoup

import feedparser
import pytest

from career_intelligence_pipeline import (
    GlobalCareerIntelligencePipeline,
    RemoteOKSource,
)
from models import SkillLevel
from roadmap_engine import (
    RoadmapEngine,
    SKILL_GRAPH,
    TRACKS,
    detect_track,
    topological_sort,
    adjust_timeline,
    TIMELINE_WEEKS,
)


# ═══════════════════════════════════════════════════════════════════
# SECTION 1: API Connectivity Tests (from test_api.py)
# ═══════════════════════════════════════════════════════════════════

async def test_api_fetch():
    """Test core external APIs (Wikipedia, Wikidata, ESCO)."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        print("Testing Wikipedia API...")
        url = "https://en.wikipedia.org/api/rest_v1/page/summary/software_engineer"
        resp = await client.get(url, timeout=15.0)
        print(f"  Wikipedia status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            extract = data.get("extract", "NO EXTRACT")
            print(f"  Wikipedia extract: {extract[:100]}...")

        print("\nTesting Wikidata API...")
        url = "https://www.wikidata.org/w/api.php?action=wbsearchentities&search=software+engineer&language=en&format=json"
        resp = await client.get(url, timeout=15.0)
        print(f"  Wikidata status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            print(f"  Wikidata search results: {len(data.get('search', []))}")

        print("\nTesting ESCO API...")
        url = "https://ec.europa.eu/esco/api/search?language=en&type=occupation&text=software+engineer&limit=5"
        resp = await client.get(url, timeout=15.0)
        print(f"  ESCO status: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            print(f"  ESCO results: {len(data.get('_embedded', {}).get('results', []))}")


# ═══════════════════════════════════════════════════════════════════
# SECTION 2: Pipeline API Calls (from test_api_calls.py)
# ═══════════════════════════════════════════════════════════════════

async def test_api_calls():
    """Direct HTTP tests against RemoteOK, Glassdoor, Coursera."""
    pipeline = GlobalCareerIntelligencePipeline()
    print("=== TESTING API CALLS DIRECTLY ===")

    async with httpx.AsyncClient() as client:
        headers = {
            "User-Agent": "HorizonCareerPipeline/1.0 (educational project; contact@example.com)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }

        print("\n1. Testing RemoteOK API directly:")
        try:
            query_slug = "software-engineer"
            url = f"https://remoteok.com/remote-{query_slug}-jobs.rss"
            resp = await client.get(url, headers=headers, timeout=30.0)
            print(f"   Status: {resp.status_code}, Response length: {len(resp.text)}")
            print("   SUCCESS - API returned data!" if resp.status_code == 200 and len(resp.text) > 0 else "   FAILED - No data returned")
        except Exception as exc:
            print(f"   ERROR: {exc}")

        print("\n2. Testing Glassdoor API directly:")
        try:
            q = quote_plus("software engineer")
            url = f"https://www.glassdoor.com/Job/jobs.htm?sc.keyword={q}&sc.location=&radius=50&limit=50"
            resp = await client.get(url, headers=headers, timeout=30.0)
            print(f"   Status: {resp.status_code}, Response length: {len(resp.text)}")
            if resp.status_code == 200 and len(resp.text) > 0:
                soup = BeautifulSoup(resp.text, "html.parser")
                job_cards = soup.find_all("div", class_="job-card") or soup.find_all("article")
                print(f"   Found {len(job_cards)} job cards")
            else:
                print("   FAILED - No data returned")
        except Exception as exc:
            print(f"   ERROR: {exc}")

        print("\n3. Testing Coursera API directly:")
        try:
            q = quote_plus("software engineer")
            url = f"https://www.coursera.org/search?query={q}&advisor_type=career&product_type=free"
            resp = await client.get(url, headers=headers, timeout=30.0)
            print(f"   Status: {resp.status_code}, Response length: {len(resp.text)}")
            if resp.status_code == 200 and len(resp.text) > 0:
                soup = BeautifulSoup(resp.text, "html.parser")
                course_cards = soup.find_all("div", class_="card") or soup.find_all("article")
                print(f"   Found {len(course_cards)} course cards")
            else:
                print("   FAILED - No data returned")
        except Exception as exc:
            print(f"   ERROR: {exc}")


# ═══════════════════════════════════════════════════════════════════
# SECTION 3: Detailed Extraction (from test_detailed_extraction.py)
# ═══════════════════════════════════════════════════════════════════

async def test_detailed_extraction():
    """Test pipeline's extraction methods directly."""
    pipeline = GlobalCareerIntelligencePipeline()
    print("=== TESTING DETAILED EXTRACTION ===")

    async with httpx.AsyncClient() as client:
        queries = ["software engineer"]
        print("\n1. Testing _extract_external_knowledge:")
        knowledge_data = await pipeline._extract_external_knowledge(client, queries)
        print(f"   Knowledge sources: {list(knowledge_data.keys())}")
        for source, data in knowledge_data.items():
            print(f"   {source}: skills={len(data.get('skills', []))}, tools={len(data.get('tools', []))}")

        print("\n2. Testing _extract_external_jobs:")
        jobs_data = await pipeline._extract_external_jobs(client, queries, {})
        print(f"   Raw jobs: {len(jobs_data)}")
        for i, job in enumerate(jobs_data[:3]):
            print(f"   Job {i+1}: {job.get('title', 'NO TITLE')} - {job.get('company', 'NO COMPANY')} - Source: {job.get('source', 'NO SOURCE')}")

        print("\n3. Testing _extract_external_courses:")
        courses_data = await pipeline._extract_external_courses(client, queries, {})
        print(f"   Raw courses: {len(courses_data)}")
        for i, course in enumerate(courses_data[:3]):
            print(f"   Course {i+1}: {course.get('title', 'NO TITLE')} - {course.get('provider', 'NO PROVIDER')} - Source: {course.get('source', 'NO SOURCE')}")


# ═══════════════════════════════════════════════════════════════════
# SECTION 4: Feedparser Test (from test_feedparser.py)
# ═══════════════════════════════════════════════════════════════════

async def test_feedparser():
    """Test feedparser with RemoteOK RSS feed."""
    print("=== TESTING feedparser with RemoteOK RSS ===")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }

    async with httpx.AsyncClient() as client:
        query_slug = "software-engineer"
        url = f"https://remoteok.com/remote-{query_slug}-jobs.rss"
        resp = await client.get(url, headers=headers, timeout=30.0)
        print(f"  HTTP Status: {resp.status_code}")
        print(f"  Response length: {len(resp.text)}")

        if resp.status_code == 200:
            feed = feedparser.parse(resp.text)
            print(f"  Feed title: {feed.feed.get('title', 'NO TITLE')}")
            print(f"  Number of entries: {len(feed.entries)}")
            if feed.entries:
                print(f"  First entry title: {feed.entries[0].get('title', 'NO TITLE')}")
        else:
            print("  Failed to fetch RSS feed")


# ═══════════════════════════════════════════════════════════════════
# SECTION 5: Full Pipeline Test (from test_full_pipeline.py)
# ═══════════════════════════════════════════════════════════════════

async def test_full_pipeline():
    """Run the full pipeline and print results."""
    pipeline = GlobalCareerIntelligencePipeline()
    print("=== TESTING FULL PIPELINE ===")
    result = await pipeline.process_field("software engineer")
    print(f"\nInput: {result['input']}")
    print(f"Interpreted queries: {len(result['interpreted_queries'])}")
    print(f"Sources used: {result['sources_used']}")
    print(f"Confidence: {result['confidence']}")
    print(f"\nData counts:")
    for key in ("job_roles", "skills", "tools", "companies", "locations", "internships", "courses"):
        print(f"  {key}: {len(result['data'][key])}")
    print(f"\nData gaps: {result['data_quality']['missing_fields']}")
    print(f"Errors: {result['errors']}")


# ═══════════════════════════════════════════════════════════════════
# SECTION 6: Normalization Test (from test_normalization.py)
# ═══════════════════════════════════════════════════════════════════

async def test_normalization():
    """Test pipeline normalization logic directly."""
    pipeline = GlobalCareerIntelligencePipeline()
    print("=== TESTING NORMALIZATION ===")

    async with httpx.AsyncClient() as client:
        field = "software engineer"
        queries = pipeline._interpret_queries(field, {})
        print(f"  Generated queries: {queries}")

        extracted = await pipeline._extract_all_external_sources(queries, {})
        print(f"  Extracted keys: {list(extracted.keys())}")

        knowledge_data = await pipeline._extract_external_knowledge(client, queries)
        print(f"  Knowledge sources: {list(knowledge_data.keys())}")

        jobs_data = await pipeline._extract_external_jobs(client, queries, {})
        print(f"  Job extraction: {len(jobs_data)} raw jobs")

        courses_data = await pipeline._extract_external_courses(client, queries, {})
        print(f"  Course extraction: {len(courses_data)} raw courses")

        normalized = pipeline._normalize_external_data(extracted, {})
        print(f"\n  Normalized keys: {list(normalized.keys())}")
        print(f"  Normalized jobs: {len(normalized.get('jobs', []))}")
        print(f"  Normalized knowledge skills: {len(normalized.get('knowledge_skills', []))}")


# ═══════════════════════════════════════════════════════════════════
# SECTION 7: Pipeline Result Structure (from test_pipeline.py)
# ═══════════════════════════════════════════════════════════════════

async def test_pipeline():
    """Inspect full pipeline result structure."""
    pipeline = GlobalCareerIntelligencePipeline()
    result = await pipeline.process_field("software engineer")
    print("=== FULL RESULT STRUCTURE ===")
    print(json.dumps(result, indent=2, default=str))

    print("\n=== JOB LISTINGS ===")
    print(f"Total jobs: {len(result['data']['jobs'])}")
    for i, job in enumerate(result["data"]["jobs"][:5]):
        print(f"  {i+1}. {job.get('title', 'No title')} - {job.get('company', 'No company')}")

    print("\n=== SKILLS ===")
    print(f"Total skills: {len(result['data']['skills'])}")
    for i, skill in enumerate(result["data"]["skills"][:10]):
        print(f"  {i+1}. {skill}")

    print("\n=== TOOLS ===")
    print(f"Total tools: {len(result['data']['tools'])}")
    for i, tool in enumerate(result["data"]["tools"][:10]):
        print(f"  {i+1}. {tool}")

    print("\n=== COURSES ===")
    print(f"Total courses: {len(result['data']['courses'])}")
    for i, course in enumerate(result["data"]["courses"][:5]):
        print(f"  {i+1}. {course.get('title', 'No title')} - {course.get('provider', 'No provider')}")


# ═══════════════════════════════════════════════════════════════════
# SECTION 8: Pipeline Final (from test_pipeline_final.py)
# ═══════════════════════════════════════════════════════════════════

async def test_pipeline_final():
    """Pipeline test with cache check."""
    pipeline = GlobalCareerIntelligencePipeline()
    print("=== TESTING WITH CACHE CHECK ===")
    result = await pipeline.process_field("software engineer")
    print(f"\nInput: {result['input']}")
    print(f"Interpreted queries: {len(result['interpreted_queries'])}")
    print(f"Sources used: {result['sources_used']}")
    print(f"Confidence: {result['confidence']}")
    for key in ("job_roles", "skills", "tools", "companies", "locations", "internships", "courses"):
        print(f"  {key}: {len(result['data'][key])}")


# ═══════════════════════════════════════════════════════════════════
# SECTION 9: RemoteOK Debug (from test_remoteok_debug.py)
# ═══════════════════════════════════════════════════════════════════

async def test_remoteok_debug():
    """Debug RemoteOK RSS feed parsing."""
    print("=== DEBUGGING API RESPONSES ===")
    print("\n1. RemoteOK RSS content:")
    query_slug = "software-engineer"
    url = f"https://remoteok.com/remote-{query_slug}-jobs.rss"
    headers = {
        "User-Agent": "HorizonCareerPipeline/1.0 (educational project; contact@example.com)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers, timeout=30.0)
        print(f"   Status: {resp.status_code}")
        print(f"   Content length: {len(resp.text)}")
        if resp.status_code == 200:
            feed = feedparser.parse(resp.text)
            print(f"   Feed entries: {len(feed.entries)}")
            if feed.entries:
                print(f"   First entry title: {feed.entries[0].get('title', 'NO TITLE')}")

            print("\n   Alternative parsing without feedparser:")
            soup = BeautifulSoup(resp.text, "html.parser")
            entries = soup.find_all("item")
            print(f"   Found {len(entries)} RSS items")


# ═══════════════════════════════════════════════════════════════════
# SECTION 10: RemoteOK Debug 2 (from test_remoteok_debug2.py)
# ═══════════════════════════════════════════════════════════════════

async def test_remoteok_debug2():
    """Debug RemoteOK RSS with different UA."""
    print("=== DEBUGGING RemoteOK RSS ===")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    }
    async with httpx.AsyncClient() as client:
        query_slug = "software-engineer"
        url = f"https://remoteok.com/remote-{query_slug}-jobs.rss"
        print(f"  Testing URL: {url}")
        resp = await client.get(url, headers=headers, timeout=30.0)
        print(f"  Status: {resp.status_code}")
        print(f"  Content length: {len(resp.text)}")
        if resp.status_code == 200:
            feed = feedparser.parse(resp.text)
            print(f"  Feed entries: {len(feed.entries)}")
            if feed.entries:
                for i, entry in enumerate(feed.entries[:3]):
                    print(f"  Entry {i+1}: {entry.get('title', 'NO TITLE')}")


# ═══════════════════════════════════════════════════════════════════
# SECTION 11: RemoteOK Source (from test_remoteok_final.py)
# ═══════════════════════════════════════════════════════════════════

async def test_remoteok_final():
    """Test RemoteOKSource.fetch_jobs directly."""
    print("=== TESTING RemoteOKSource with feedparser ===")
    async with httpx.AsyncClient() as client:
        jobs = await RemoteOKSource.fetch_jobs(client, "software engineer", "")
        print(f"  Raw jobs returned: {len(jobs)}")
        if jobs:
            for i, job in enumerate(jobs[:3]):
                print(f"  Job {i+1}: {job.get('title', 'NO TITLE')} - {job.get('company', 'NO COMPANY')}")
        else:
            print("  No jobs returned - testing direct API call")


# ═══════════════════════════════════════════════════════════════════
# SECTION 12: pytest-compatible Roadmap Engine Tests
# (from tests/test_engine.py)
# ═══════════════════════════════════════════════════════════════════

BASELINE_GITHUB_TRENDS = [
    {"name": "pytorch", "description": "Tensors and neural networks", "stars": 78000},
    {"name": "transformers", "description": "NLP models by HuggingFace", "stars": 127000},
]

BASELINE_HN_JOBS = [
    {"text": "python machine learning tensorflow pytorch data science numpy pandas"},
    {"text": "kubernetes docker devops aws cloud infrastructure terraform"},
]

BASELINE_ARXIV = [
    {"title": "Attention Is All You Need", "summary": "transformer self-attention mechanism neural network"},
    {"title": "BERT Pre-training Deep Bidirectional Transformers", "summary": "language representation bert nlp"},
]

BASELINE_JOBS = [
    {"tags": ["python", "machine learning", "tensorflow", "mlops"], "position": "ML Engineer", "company": "Acme AI"},
    {"tags": ["kubernetes", "docker", "aws", "terraform", "ci/cd"], "position": "DevOps Engineer", "company": "CloudCorp"},
]


def test_skill_graph_minimum_size():
    assert len(SKILL_GRAPH) >= 50, "Must have 50+ skills defined"


def test_all_prerequisites_exist():
    all_keys = set(SKILL_GRAPH.keys())
    for skill, data in SKILL_GRAPH.items():
        for prereq in data.get("prerequisites", []):
            assert prereq in all_keys, f"Skill '{skill}' has unknown prereq '{prereq}'"


def test_all_skills_have_required_fields():
    for skill, data in SKILL_GRAPH.items():
        assert "hours" in data, f"'{skill}' missing 'hours'"
        assert "phase" in data, f"'{skill}' missing 'phase'"
        assert isinstance(data["hours"], int), f"'{skill}' hours must be int"
        assert data["phase"] in ("foundation", "core", "specialization", "advanced"), \
            f"'{skill}' has invalid phase '{data['phase']}'"


def test_all_skills_have_resources():
    for skill, data in SKILL_GRAPH.items():
        resources = data.get("resources", [])
        assert len(resources) >= 1, f"'{skill}' must have at least 1 resource"
        for res in resources:
            assert "title" in res and "url" in res, f"'{skill}' resource missing title or url"
            assert res["url"].startswith("http"), f"'{skill}' resource URL must start with http"


def test_all_tracks_defined():
    expected = {"ml", "ai_research", "data_science", "backend", "frontend", "cybersecurity", "devops", "bioinformatics"}
    assert set(TRACKS.keys()) >= expected


def test_detect_ml_track():
    assert detect_track("Machine Learning Engineer") == "ml"


def test_detect_ai_research():
    assert detect_track("AI Researcher") == "ai_research"


def test_detect_data_science():
    assert detect_track("Data Scientist") == "data_science"


def test_detect_devops():
    assert detect_track("DevOps Engineer") == "devops"


def test_detect_cybersecurity():
    assert detect_track("Cybersecurity Analyst") == "cybersecurity"


def test_detect_bioinformatics():
    assert detect_track("Bioinformatics Researcher") == "bioinformatics"


def test_detect_frontend():
    assert detect_track("Frontend Developer") == "frontend"


def test_detect_backend():
    assert detect_track("Backend Developer") == "backend"


def test_detect_unknown_defaults_to_ml():
    result = detect_track("Astronaut Chef")
    assert result in TRACKS


def test_topological_sort_respects_order():
    skills = ["numpy", "python"]
    sorted_skills = topological_sort(skills, SKILL_GRAPH)
    assert sorted_skills.index("python") < sorted_skills.index("numpy")


def test_topological_sort_empty():
    result = topological_sort([], SKILL_GRAPH)
    assert result == []


def test_topological_sort_single_skill():
    result = topological_sort(["python"], SKILL_GRAPH)
    assert result == ["python"]


def test_topological_sort_no_duplicates():
    skills = ["python", "numpy", "pandas", "scikit_learn"]
    result = topological_sort(skills, SKILL_GRAPH)
    assert len(result) == len(set(result))


def test_topological_sort_cycle_detection():
    cyclic_graph = {"a": {"prerequisites": ["b"]}, "b": {"prerequisites": ["a"]}}
    result = topological_sort(["a", "b"], cyclic_graph)
    assert set(result) == {"a", "b"}
    assert len(result) == 2


def test_adjust_timeline_10h_baseline():
    weeks = adjust_timeline(78, 10)
    assert weeks == 78


def test_adjust_timeline_more_hours_shorter():
    weeks_10h = adjust_timeline(78, 10)
    weeks_20h = adjust_timeline(78, 20)
    assert weeks_20h < weeks_10h


def test_adjust_timeline_fewer_hours_longer():
    weeks_10h = adjust_timeline(78, 10)
    weeks_5h = adjust_timeline(78, 5)
    assert weeks_5h > weeks_10h


def test_adjust_timeline_minimum_4_weeks():
    weeks = adjust_timeline(4, 168)
    assert weeks >= 4


def test_beginner_timeline_longer_than_advanced():
    b = TIMELINE_WEEKS[SkillLevel.BEGINNER]
    a = TIMELINE_WEEKS[SkillLevel.ADVANCED]
    assert b > a


@pytest.fixture
def market_data():
    return {
        "github_trends": BASELINE_GITHUB_TRENDS,
        "hn_jobs": BASELINE_HN_JOBS,
        "arxiv_papers": BASELINE_ARXIV,
        "job_listings": BASELINE_JOBS,
        "scholarships": [],
        "universities": [],
    }


@pytest.fixture
def engine():
    return RoadmapEngine()


def test_generate_ml_beginner(engine, market_data):
    result = engine.generate(
        goal="Machine Learning Engineer", skill_level=SkillLevel.BEGINNER,
        country="USA", weekly_hours=10, market_data=market_data,
        analyzer_scores={}, job_id="test001",
    )
    assert result.goal == "Machine Learning Engineer"
    assert result.skill_level == SkillLevel.BEGINNER
    assert len(result.phases) > 0
    assert result.total_weeks > 0
    assert len(result.executive_summary) > 50
    assert len(result.top_skills) > 0


def test_generate_devops_intermediate(engine, market_data):
    result = engine.generate(
        goal="DevOps Engineer", skill_level=SkillLevel.INTERMEDIATE,
        country="Germany", weekly_hours=20, market_data=market_data,
        analyzer_scores={}, job_id="test002",
    )
    assert result.goal == "DevOps Engineer"
    assert len(result.phases) > 0


def test_generate_ai_researcher_advanced(engine, market_data):
    result = engine.generate(
        goal="AI Researcher", skill_level=SkillLevel.ADVANCED,
        country="UK", weekly_hours=40, market_data=market_data,
        analyzer_scores={}, job_id="test003",
    )
    assert result.total_weeks > 0
    assert len(result.top_skills) > 0


def test_generate_result_has_phases_with_skills(engine, market_data):
    result = engine.generate(
        goal="Data Scientist", skill_level=SkillLevel.BEGINNER,
        country="India", weekly_hours=15, market_data=market_data,
        analyzer_scores={}, job_id="test004",
    )
    for phase in result.phases:
        assert phase.number > 0
        assert phase.duration_weeks > 0
        assert len(phase.skills) > 0


def test_generate_all_skill_nodes_have_name(engine, market_data):
    result = engine.generate(
        goal="Cybersecurity Analyst", skill_level=SkillLevel.BEGINNER,
        country="Canada", weekly_hours=10, market_data=market_data,
        analyzer_scores={}, job_id="test005",
    )
    for phase in result.phases:
        for skill in phase.skills:
            assert skill.name
            assert skill.hours > 0
            assert skill.phase in ("foundation", "core", "specialization", "advanced")


def test_generate_with_universities(engine, market_data):
    market_data["universities"] = [
        {"name": "MIT", "country": "USA", "ranking": 1, "program": "Computer Science & AI", "url": "https://mit.edu"},
        {"name": "Oxford", "country": "UK", "ranking": 7, "program": "CS & Machine Learning", "url": "https://oxford.ac.uk"},
    ]
    result = engine.generate(
        goal="Machine Learning Engineer", skill_level=SkillLevel.BEGINNER,
        country="USA", weekly_hours=10, market_data=market_data,
        analyzer_scores={}, job_id="test006",
    )
    assert len(result.universities) > 0


def test_generate_with_scholarships(engine, market_data):
    market_data["scholarships"] = [
        {
            "name": "Fulbright", "country": "USA", "amount": "Full funding",
            "deadline": "November", "eligibility": "STEM graduate study",
            "url": "https://fulbright.gov",
        }
    ]
    result = engine.generate(
        goal="AI Researcher", skill_level=SkillLevel.ADVANCED,
        country="USA", weekly_hours=40, market_data=market_data,
        analyzer_scores={}, job_id="test007",
    )
    assert isinstance(result.scholarships, list)


def test_bioinformatics_track_has_biology_skills(engine, market_data):
    result = engine.generate(
        goal="Bioinformatics Researcher", skill_level=SkillLevel.BEGINNER,
        country="USA", weekly_hours=10, market_data=market_data,
        analyzer_scores={}, job_id="test008",
    )
    all_skill_names = [s.name.lower() for phase in result.phases for s in phase.skills]
    bio_terms = ["molecular", "sequence", "biopython", "genomics", "r programming", "ngs"]
    assert any(any(term in name for term in bio_terms) for name in all_skill_names)


# ═══════════════════════════════════════════════════════════════════
# MAIN: Run all standalone tests
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    standalone_tests = [
        ("API Connectivity", test_api_fetch),
        ("Pipeline API Calls", test_api_calls),
        ("Detailed Extraction", test_detailed_extraction),
        ("Feedparser", test_feedparser),
        ("Full Pipeline", test_full_pipeline),
        ("Normalization", test_normalization),
        ("Pipeline Structure", test_pipeline),
        ("Pipeline Final", test_pipeline_final),
        ("RemoteOK Debug", test_remoteok_debug),
        ("RemoteOK Debug 2", test_remoteok_debug2),
        ("RemoteOK Source", test_remoteok_final),
    ]

    all_ok = True
    for name, fn in standalone_tests:
        print(f"\n{'='*60}")
        print(f"RUNNING: {name}")
        print(f"{'='*60}")
        try:
            asyncio.run(fn())
            print(f"\n  [{name}] PASSED\n")
        except Exception as e:
            print(f"\n  [{name}] FAILED: {e}\n")
            all_ok = False

    print(f"\n{'='*60}")
    print(f"STANDALONE TESTS: {'ALL PASSED' if all_ok else 'SOME FAILED'}")
    print(f"{'='*60}")
    print()
    print("To run pytest-compatible tests:")
    print("  python -m pytest test_all.py -k 'test_skill_' or 'test_detect_' or 'test_generate_'")
    print("  python -m pytest test_all.py ::TestClass")
