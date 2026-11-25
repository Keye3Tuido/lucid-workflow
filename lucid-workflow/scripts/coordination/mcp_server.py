"""MCP Streamable HTTP adapter for Lucid Workflow coordination."""

from __future__ import annotations

import hashlib
import json
import time
from argparse import Namespace
from pathlib import Path
from typing import Any, Callable, TypedDict

from mcp.server.mcpserver import MCPServer

from check_doc_forest import check_forest
from forest_context import find_context
from state_files import (
    initialize_requirements,
    load_valid,
    remove_requirement,
    require_session,
    review_requirements,
    upsert_requirement,
)

from .core import Coordinator


class TaskCreated(TypedDict):
    task_id: str


class ClaimResult(TypedDict):
    task_id: str | None


class OperationResult(TypedDict):
    ok: bool


class RemovedResult(TypedDict):
    removed: list[str]


class TasksResult(TypedDict):
    tasks: list[dict[str, Any]]


class QuestionCreated(TypedDict):
    question_id: str


class QuestionsResult(TypedDict):
    questions: list[dict[str, Any]]


class EventsResult(TypedDict):
    events: list[dict[str, Any]]


class StatusResult(TypedDict):
    project: str
    service_version: str
    tasks: list[dict[str, Any]]
    open_questions: int


class DocumentResult(TypedDict):
    document: dict[str, Any]


class MutationResult(TypedDict):
    result: dict[str, Any]


class ReportResult(TypedDict):
    report: dict[str, Any]


class BootstrapResult(TypedDict):
    project: str
    service_version: str
    requirements: dict[str, Any]
    tasks: list[dict[str, Any]]
    open_questions: list[dict[str, Any]]


class CheckpointResult(TypedDict):
    tasks: list[dict[str, Any]]
    open_questions: list[dict[str, Any]]
    cleaned: list[str]


def create_server(
    database: Path,
    *,
    lease_seconds: float = 30,
    clock: Callable[[], float] = time.monotonic,
) -> MCPServer:
    coordinator = Coordinator(database, lease_seconds=lease_seconds, clock=clock)
    server = MCPServer(
        name="lucid-workflow-coordination-poc",
        title="Lucid Workflow Coordination PoC",
        version=(Path(__file__).resolve().parents[2] / "VERSION").read_text(encoding="utf-8").strip(),
        description="Experimental multi-agent coordination without production integration.",
        log_level="ERROR",
    )
    state_directory = database.parent

    def requirements_path(session_id: str) -> Path:
        session_key = hashlib.sha256(session_id.encode("utf-8")).hexdigest()
        return state_directory / "sessions" / session_key / "requirements.yml"

    def project_path(project_root: str) -> Path:
        path = Path(project_root)
        if not path.is_absolute():
            raise ValueError("project_root 必须是绝对路径")
        root = path.resolve()
        if not root.is_dir():
            raise ValueError("项目根目录不存在或不是目录")
        return root

    def read_requirements(session_id: str, project_root: str) -> tuple[Path, Path, dict[str, Any]]:
        root = project_path(project_root)
        path = requirements_path(session_id)
        if not path.exists():
            initialize_requirements(
                Namespace(
                    file=path,
                    session_id=session_id,
                    project_root=root,
                    command="init",
                )
            )
        document = load_valid(path, "requirements")
        require_session(document, session_id, root)
        return path, root, document

    @server.tool(name="tasks.add", structured_output=True)
    def add_task(
        project: str,
        title: str,
        acceptance_criteria: list[str],
        dependencies: list[str] | None = None,
        write_scope: str = "",
    ) -> TaskCreated:
        task_id = coordinator.add_task(project, title, acceptance_criteria, dependencies, write_scope)
        return {"task_id": task_id}

    @server.tool(name="tasks.claim", structured_output=True)
    def claim(project: str, task_id: str, agent: str, session_id: str) -> ClaimResult:
        """Claim the specified task, or return a null task_id if it cannot be claimed."""
        return {"task_id": coordinator.claim(project, task_id, agent, session_id)}

    @server.tool(name="tasks.claim_next", structured_output=True)
    def claim_next(project: str, agent: str, session_id: str) -> ClaimResult:
        """Return the agent's active task or claim the next eligible task by ID order."""
        return {"task_id": coordinator.claim_next(project, agent, session_id)}

    @server.tool(name="tasks.heartbeat", structured_output=True)
    def heartbeat(project: str, agent: str, task_id: str) -> OperationResult:
        coordinator.heartbeat(project, agent, task_id)
        return {"ok": True}

    @server.tool(name="tasks.complete", structured_output=True)
    def complete(project: str, agent: str, task_id: str, verification: str) -> OperationResult:
        coordinator.complete(project, agent, task_id, verification)
        return {"ok": True}

    @server.tool(name="tasks.set_blocked", structured_output=True)
    def set_blocked(project: str, task_id: str, reason: str | None = None) -> OperationResult:
        coordinator.set_blocked(project, task_id, reason)
        return {"ok": True}

    @server.tool(name="tasks.accept", structured_output=True)
    def accept(project: str, task_ids: list[str], evidence: str) -> OperationResult:
        coordinator.accept(project, task_ids, evidence)
        return {"ok": True}

    @server.tool(name="tasks.clean_accepted", structured_output=True)
    def clean_accepted(project: str) -> RemovedResult:
        return {"removed": coordinator.clean_accepted(project)}

    @server.tool(name="tasks.list", structured_output=True)
    def list_tasks(project: str) -> TasksResult:
        return {"tasks": coordinator.tasks(project)}

    @server.tool(name="questions.ask", structured_output=True)
    def ask_question(
        project: str,
        session_id: str,
        question: str,
        scope: str = "project",
    ) -> QuestionCreated:
        return {"question_id": coordinator.ask_question(project, session_id, question, scope)}

    @server.tool(name="questions.answer", structured_output=True)
    def answer_question(project: str, question_id: str, answer: str) -> OperationResult:
        coordinator.answer_question(project, question_id, answer)
        return {"ok": True}

    @server.tool(name="questions.list", structured_output=True)
    def list_questions(project: str, session_id: str) -> QuestionsResult:
        return {"questions": coordinator.questions(project, session_id)}

    @server.tool(name="events.list", structured_output=True)
    def list_events(project: str) -> EventsResult:
        return {"events": coordinator.events(project)}

    @server.tool(name="workflow.status", structured_output=True)
    def status(project: str) -> StatusResult:
        tasks = coordinator.tasks(project)
        return {
            "project": project,
            "service_version": server.version,
            "tasks": tasks,
            "open_questions": len(
                [question for question in coordinator.questions(project, "") if question["status"] == "open"]
            ),
        }

    @server.tool(name="workflow.bootstrap", structured_output=True)
    def bootstrap(project: str, session_id: str, project_root: str) -> BootstrapResult:
        _, _, document = read_requirements(session_id, project_root)
        questions = coordinator.questions(project, session_id)
        return {
            "project": project,
            "service_version": server.version,
            "requirements": document,
            "tasks": coordinator.tasks(project),
            "open_questions": [question for question in questions if question["status"] == "open"],
        }

    @server.tool(name="workflow.checkpoint", structured_output=True)
    def checkpoint(
        project: str,
        session_id: str,
        task_ids: list[str] | None = None,
        user_evidence: str = "",
    ) -> CheckpointResult:
        cleaned = []
        if task_ids and user_evidence.strip():
            coordinator.accept(project, task_ids, user_evidence)
            cleaned = coordinator.clean_accepted(project)
        questions = coordinator.questions(project, session_id)
        return {
            "tasks": coordinator.tasks(project),
            "open_questions": [question for question in questions if question["status"] == "open"],
            "cleaned": cleaned,
        }

    @server.tool(name="requirements.list", structured_output=True)
    def list_requirements(session_id: str, project_root: str) -> DocumentResult:
        _, _, document = read_requirements(session_id, project_root)
        return {"document": document}

    @server.tool(name="requirements.upsert", structured_output=True)
    def upsert(
        session_id: str,
        project_root: str,
        text: str,
        requirement_id: str | None = None,
        polarity: str | None = None,
        scope: str | None = None,
        task_ids: list[str] | None = None,
        source_quote: str | None = None,
        expires_when: str | None = None,
    ) -> MutationResult:
        path, root, _ = read_requirements(session_id, project_root)
        result = upsert_requirement(
            Namespace(
                file=path,
                session_id=session_id,
                project_root=root,
                command="upsert",
                id=requirement_id,
                text=text,
                polarity=polarity,
                scope=scope,
                task_id=task_ids,
                source_quote=source_quote,
                expires_when=expires_when,
            )
        )
        return {"result": result}

    @server.tool(name="requirements.remove", structured_output=True)
    def remove(session_id: str, project_root: str, requirement_id: str) -> MutationResult:
        path, root, _ = read_requirements(session_id, project_root)
        result = remove_requirement(
            Namespace(
                file=path,
                session_id=session_id,
                project_root=root,
                command="remove",
                id=requirement_id,
            )
        )
        return {"result": result}

    @server.tool(name="requirements.review", structured_output=True)
    def review(
        session_id: str,
        project_root: str,
        results: list[dict[str, Any]],
    ) -> MutationResult:
        path, root, _ = read_requirements(session_id, project_root)
        result = review_requirements(
            Namespace(
                file=path,
                session_id=session_id,
                project_root=root,
                command="review",
                results=json.dumps(results, ensure_ascii=False),
            )
        )
        return {"result": result}

    @server.tool(name="forest.locate", structured_output=True)
    def locate(
        project_root: str,
        task: str,
        keywords: list[str] | None = None,
        paths: list[str] | None = None,
        max_results: int = 10,
        no_cache: bool = False,
    ) -> ReportResult:
        root = project_path(project_root)
        cache_key = hashlib.sha256(str(root).encode("utf-8")).hexdigest()
        report = find_context(
            root,
            task,
            keywords=keywords or [],
            anchors=paths or [],
            max_results=max_results,
            use_cache=not no_cache,
            cache_path=state_directory / "cache" / f"{cache_key}.sqlite",
        )
        return {"report": report}

    @server.tool(name="forest.validate", structured_output=True)
    def validate(project_root: str, paths: list[str] | None = None) -> ReportResult:
        return {"report": check_forest(project_path(project_root), paths or [])}

    return server
