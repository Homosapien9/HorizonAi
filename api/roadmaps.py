"""
Horizon v5 — Roadmap API endpoints.

Provides read/search/progress endpoints for roadmap graph data while
remaining SQLite-compatible and resilient to missing optional records.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query

import models
from models import NodeDetailsResponse, RoadmapApiResponse

logger = logging.getLogger(__name__)


def _safe_metadata(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _score_match(query: str, label: str, description: str) -> float:
    query_lower = query.lower().strip()
    label_lower = (label or "").lower()
    desc_lower = (description or "").lower()
    if not query_lower:
        return 0.0
    if query_lower == label_lower:
        return 1.0
    if query_lower in label_lower:
        return 0.9
    label_words = set(label_lower.split())
    desc_words = set(desc_lower.split())
    query_words = {word for word in query_lower.split() if word}
    if not query_words:
        return 0.0
    overlap = len(query_words & (label_words | desc_words)) / max(len(query_words), 1)
    return round(min(0.8, overlap), 3)


def create_roadmap_api(app: FastAPI) -> None:
    @app.get("/api/roadmaps", response_model=list[RoadmapApiResponse])
    async def get_all_roadmaps():
        try:
            db = await models._get_connection()
            async with db.execute(
                "SELECT id, name, category, description FROM roadmaps ORDER BY name"
            ) as cursor:
                rows = await cursor.fetchall()
            return [
                {
                    "id": row["id"],
                    "name": row["name"],
                    "category": row["category"],
                    "description": row["description"],
                }
                for row in rows
            ]
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Error fetching roadmaps")
            raise HTTPException(
                status_code=500,
                detail="Failed to fetch roadmaps",
            ) from exc

    @app.get("/api/roadmaps/{roadmap_id}", response_model=dict)
    async def get_roadmap_with_nodes_and_edges(roadmap_id: str):
        try:
            db = await models._get_connection()
            async with db.execute(
                "SELECT id, name, category, description FROM roadmaps WHERE id = ?",
                (roadmap_id,),
            ) as cursor:
                roadmap = await cursor.fetchone()
            if not roadmap:
                raise HTTPException(status_code=404, detail="Roadmap not found")

            async with db.execute(
                """
                SELECT id, node_id, label, type, position_x, position_y, description, metadata
                FROM nodes
                WHERE roadmap_id = ?
                ORDER BY position_y, position_x, label
                """,
                (roadmap_id,),
            ) as cursor:
                node_rows = await cursor.fetchall()

            nodes: list[dict[str, Any]] = []
            node_map: dict[str, dict[str, Any]] = {}
            for row in node_rows:
                node = {
                    "id": row["id"],
                    "node_id": row["node_id"],
                    "label": row["label"],
                    "type": row["type"],
                    "position": {"x": row["position_x"], "y": row["position_y"]},
                    "data": {
                        "label": row["label"],
                        "description": row["description"],
                        "metadata": _safe_metadata(row["metadata"]),
                    },
                }
                nodes.append(node)
                node_map[row["id"]] = node

            async with db.execute(
                """
                SELECT id, source, target, animated, label
                FROM edges
                WHERE roadmap_id = ?
                ORDER BY id
                """,
                (roadmap_id,),
            ) as cursor:
                edge_rows = await cursor.fetchall()

            edges = [
                {
                    "id": row["id"],
                    "source": row["source"],
                    "target": row["target"],
                    "animated": bool(row["animated"]),
                    "label": row["label"],
                }
                for row in edge_rows
                if row["source"] in node_map and row["target"] in node_map
            ]

            return {
                "id": roadmap["id"],
                "name": roadmap["name"],
                "description": roadmap["description"],
                "category": roadmap["category"],
                "nodes": nodes,
                "edges": edges,
            }
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Error fetching roadmap %s", roadmap_id)
            raise HTTPException(status_code=500, detail="Failed to fetch roadmap") from exc

    @app.get("/api/roadmaps/{roadmap_id}/nodes/{node_id}", response_model=NodeDetailsResponse)
    async def get_node_details(roadmap_id: str, node_id: str):
        try:
            db = await models._get_connection()
            async with db.execute(
                """
                SELECT n.id, n.node_id, n.label, n.type, n.description, n.metadata,
                       r.title, r.url, r.resource_type, r.difficulty
                FROM nodes n
                LEFT JOIN resources r ON n.id = r.node_id
                WHERE n.roadmap_id = ? AND n.node_id = ?
                """,
                (roadmap_id, node_id),
            ) as cursor:
                rows = await cursor.fetchall()

            if not rows:
                raise HTTPException(status_code=404, detail="Node not found")

            first = rows[0]
            node_data = {
                "id": first["id"],
                "node_id": first["node_id"],
                "label": first["label"],
                "type": first["type"],
                "description": first["description"],
                "metadata": _safe_metadata(first["metadata"]),
            }
            resources = [
                {
                    "title": row["title"],
                    "url": row["url"],
                    "type": row["resource_type"],
                    "difficulty": row["difficulty"],
                }
                for row in rows
                if row["title"]
            ]

            async with db.execute(
                """
                SELECT n.id, n.node_id, n.label, n.type
                FROM edges e
                JOIN nodes n ON e.source = n.id
                WHERE e.target = (
                    SELECT id FROM nodes WHERE roadmap_id = ? AND node_id = ?
                )
                ORDER BY n.label
                """,
                (roadmap_id, node_id),
            ) as cursor:
                prereq_rows = await cursor.fetchall()

            prerequisites = [
                {
                    "id": row["id"],
                    "node_id": row["node_id"],
                    "label": row["label"],
                    "type": row["type"],
                }
                for row in prereq_rows
            ]
            return {
                "node": node_data,
                "resources": resources,
                "prerequisites": prerequisites,
            }
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Error fetching node details %s/%s", roadmap_id, node_id)
            raise HTTPException(status_code=500, detail="Failed to fetch node details") from exc

    @app.get("/api/search-nodes", response_model=dict)
    async def search_nodes(q: str = Query(..., min_length=1)):
        try:
            db = await models._get_connection()
            search_term = f"%{q}%"
            async with db.execute(
                """
                SELECT n.id, n.label, n.node_id, n.description, r.name as roadmap_name,
                       r.category, n.position_x, n.position_y
                FROM nodes n
                JOIN roadmaps r ON n.roadmap_id = r.id
                WHERE n.label LIKE ? OR n.description LIKE ?
                ORDER BY n.label ASC
                LIMIT 100
                """,
                (search_term, search_term),
            ) as cursor:
                rows = await cursor.fetchall()

            results = [
                {
                    "node_id": row["id"],
                    "label": row["label"],
                    "slug": row["node_id"],
                    "roadmap_name": row["roadmap_name"],
                    "category": row["category"],
                    "position": {"x": row["position_x"], "y": row["position_y"]},
                    "relevance_score": _score_match(
                        q,
                        row["label"] or "",
                        row["description"] or "",
                    ),
                }
                for row in rows
            ]
            results.sort(
                key=lambda item: (-item["relevance_score"], item["label"] or ""),
            )
            return {"results": results[:50]}
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Error searching nodes")
            raise HTTPException(
                status_code=500,
                detail="Failed to search nodes",
            ) from exc

    @app.post("/api/generate-learning-path", response_model=dict)
    async def generate_learning_path(request: dict[str, Any]):
        try:
            goal = str(request.get("goal", "")).strip()
            current_skills = request.get("current_skills", []) or []
            weekly_hours = int(request.get("available_hours_per_week", 10) or 10)
            if not goal:
                raise HTTPException(status_code=400, detail="Goal is required")

            db = await models._get_connection()
            search_term = f"%{goal}%"
            async with db.execute(
                """
                SELECT n.id, n.label, n.node_id, r.name as roadmap_name, n.position_x, n.position_y
                FROM nodes n
                JOIN roadmaps r ON n.roadmap_id = r.id
                WHERE n.label LIKE ? OR n.description LIKE ?
                LIMIT 20
                """,
                (search_term, search_term),
            ) as cursor:
                rows = await cursor.fetchall()

            nodes = [
                {
                    "id": row["id"],
                    "node_id": row["node_id"],
                    "label": row["label"],
                    "position": {"x": row["position_x"], "y": row["position_y"]},
                    "roadmap_name": row["roadmap_name"],
                    "estimated_hours": 5,
                    "priority": 1,
                }
                for row in rows
            ]
            return {
                "goal": goal,
                "current_skills": current_skills,
                "available_hours_per_week": weekly_hours,
                "path": nodes,
                "nodes": nodes,
                "edges": [],
                "total_duration_weeks": max(1, len(nodes) * 2),
                "estimated_completion_date": None,
            }
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Error generating learning path")
            raise HTTPException(status_code=500, detail="Failed to generate learning path") from exc

    @app.post("/api/progress/{user_id}/nodes/{node_id}/complete")
    async def mark_node_completed(user_id: str, node_id: str, request: dict[str, Any]):
        try:
            status = str(request.get("status", "completed"))
            notes = str(request.get("notes", ""))
            db = await models._get_connection()
            async with db.execute("SELECT id FROM nodes WHERE id = ?", (node_id,)) as cursor:
                exists = await cursor.fetchone()
            if not exists:
                raise HTTPException(status_code=404, detail="Node not found")

            now = datetime.now(timezone.utc).isoformat()
            await db.execute(
                """
                INSERT OR REPLACE INTO user_progress
                (id, user_id, node_id, status, completed_at, notes)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (f"{user_id}_{node_id}", user_id, node_id, status, now, notes),
            )
            await db.commit()
            return {
                "status": status,
                "completed_at": now,
                "user_id": user_id,
                "node_id": node_id,
            }
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Error marking node as completed")
            raise HTTPException(
                status_code=500,
                detail="Failed to mark node as completed",
            ) from exc

    @app.get("/api/progress/{user_id}/roadmap/{roadmap_id}")
    async def get_user_progress(user_id: str, roadmap_id: str):
        try:
            db = await models._get_connection()
            async with db.execute(
                """
                SELECT n.id, n.label, n.node_id, n.type, n.position_x, n.position_y,
                       up.status, up.completed_at
                FROM nodes n
                LEFT JOIN user_progress up ON n.id = up.node_id AND up.user_id = ?
                WHERE n.roadmap_id = ?
                ORDER BY n.position_y, n.position_x, n.label
                """,
                (user_id, roadmap_id),
            ) as cursor:
                rows = await cursor.fetchall()

            nodes = []
            completed_count = 0
            for row in rows:
                status = row["status"] or "not_started"
                if status == "completed":
                    completed_count += 1
                completed_at = row["completed_at"]
                nodes.append(
                    {
                        "id": row["id"],
                        "label": row["label"],
                        "node_id": row["node_id"],
                        "type": row["type"],
                        "position": {"x": row["position_x"], "y": row["position_y"]},
                        "progress_status": status,
                        "completed_at": (
                            completed_at.isoformat()
                            if hasattr(completed_at, "isoformat")
                            else completed_at
                        ),
                    }
                )
            total_nodes = len(nodes)
            completion_percentage = (
                round((completed_count / total_nodes) * 100, 1)
                if total_nodes
                else 0.0
            )
            return {
                "nodes": nodes,
                "completion_percentage": completion_percentage,
                "completed_nodes": completed_count,
                "total_nodes": total_nodes,
            }
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("Error fetching user progress")
            raise HTTPException(
                status_code=500,
                detail="Failed to fetch user progress",
            ) from exc


def register_roadmap_endpoints(app: FastAPI) -> None:
    logger.info("Registering roadmap API endpoints")
    create_roadmap_api(app)
    logger.info("Roadmap API endpoints registered successfully")
