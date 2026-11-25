"""Transactional coordination core for the optional MCP service."""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator


class CoordinationError(ValueError):
    pass


class LeaseLost(CoordinationError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class Coordinator:
    def __init__(
        self,
        database: Path,
        lease_seconds: float = 30,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.database = database
        self.lease_seconds = lease_seconds
        self.clock = clock
        database.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    project TEXT NOT NULL,
                    id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    status TEXT NOT NULL,
                    executor TEXT,
                    session_id TEXT,
                    lease_until REAL,
                    verification TEXT,
                    acceptance_evidence TEXT,
                    acceptance_criteria TEXT NOT NULL,
                    write_scope TEXT NOT NULL DEFAULT '',
                    blocked_reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    accepted_at TEXT,
                    PRIMARY KEY (project, id)
                );
                CREATE TABLE IF NOT EXISTS dependencies (
                    project TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    dependency_id TEXT NOT NULL,
                    PRIMARY KEY (project, task_id, dependency_id)
                );
                CREATE TABLE IF NOT EXISTS questions (
                    project TEXT NOT NULL,
                    id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    question TEXT NOT NULL,
                    status TEXT NOT NULL,
                    answer TEXT,
                    PRIMARY KEY (project, id)
                );
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    project TEXT NOT NULL,
                    agent TEXT,
                    action TEXT NOT NULL,
                    task_id TEXT,
                    detail TEXT
                );
                CREATE TABLE IF NOT EXISTS counters (
                    project TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    next_number INTEGER NOT NULL,
                    PRIMARY KEY (project, kind)
                );
                """
            )
        connection.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _read(self, sql: str, parameters: tuple = ()) -> list[dict]:
        connection = self._connect()
        try:
            return [dict(row) for row in connection.execute(sql, parameters).fetchall()]
        finally:
            connection.close()

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        project: str,
        action: str,
        agent: str | None = None,
        task_id: str | None = None,
        detail: str | None = None,
    ) -> None:
        connection.execute(
            "INSERT INTO events(project, agent, action, task_id, detail) VALUES (?, ?, ?, ?, ?)",
            (project, agent, action, task_id, detail),
        )

    @staticmethod
    def _next_number(connection: sqlite3.Connection, project: str, kind: str, prefix: str) -> str:
        connection.execute(
            "INSERT OR IGNORE INTO counters(project, kind, next_number) VALUES (?, ?, 1)",
            (project, kind),
        )
        number = connection.execute(
            "SELECT next_number FROM counters WHERE project = ? AND kind = ?",
            (project, kind),
        ).fetchone()[0]
        connection.execute(
            "UPDATE counters SET next_number = ? WHERE project = ? AND kind = ?",
            (number + 1, project, kind),
        )
        return f"{prefix}-{number:03d}"

    def add_task(
        self,
        project: str,
        title: str,
        acceptance_criteria: list[str],
        dependencies: list[str] | None = None,
        write_scope: str = "",
    ) -> str:
        if not title.strip():
            raise CoordinationError("title is required")
        if not acceptance_criteria or not all(isinstance(item, str) and item.strip() for item in acceptance_criteria):
            raise CoordinationError("acceptance_criteria must contain non-empty strings")
        now = utc_now()
        with self._write() as connection:
            task_id = self._next_number(connection, project, "tasks", "TASK")
            for dependency in dict.fromkeys(dependencies or []):
                exists = connection.execute(
                    "SELECT 1 FROM tasks WHERE project = ? AND id = ?",
                    (project, dependency),
                ).fetchone()
                if not exists:
                    raise CoordinationError(f"unknown dependency: {dependency}")
                connection.execute(
                    "INSERT INTO dependencies(project, task_id, dependency_id) VALUES (?, ?, ?)",
                    (project, task_id, dependency),
                )
            connection.execute(
                """
                INSERT INTO tasks(
                    project, id, title, status, write_scope, acceptance_criteria, created_at, updated_at
                ) VALUES (?, ?, ?, 'todo', ?, ?, ?, ?)
                """,
                (
                    project,
                    task_id,
                    title,
                    write_scope,
                    json.dumps(acceptance_criteria, ensure_ascii=False),
                    now,
                    now,
                ),
            )
            self._event(connection, project, "task_added", task_id=task_id)
            return task_id

    def claim_next(self, project: str, agent: str, session_id: str) -> str | None:
        return self._claim(project, agent, session_id)

    def claim(self, project: str, task_id: str, agent: str, session_id: str) -> str | None:
        return self._claim(project, agent, session_id, task_id=task_id)

    def _claim(
        self, project: str, agent: str, session_id: str, *, task_id: str | None = None
    ) -> str | None:
        with self._write() as connection:
            now = self.clock()
            active = connection.execute(
                """
                SELECT id FROM tasks
                WHERE project = ? AND executor = ? AND status = 'working' AND lease_until > ?
                """,
                (project, agent, now),
            ).fetchone()
            if active:
                return active["id"] if task_id is None or active["id"] == task_id else None
            expired = connection.execute(
                """
                SELECT id, executor FROM tasks
                WHERE project = ? AND status = 'working' AND lease_until <= ?
                  AND (? IS NULL OR id = ?)
                """,
                (project, now, task_id, task_id),
            ).fetchall()
            for task in expired:
                connection.execute(
                    """
                    UPDATE tasks SET status = 'todo', executor = NULL, session_id = NULL,
                        lease_until = NULL, started_at = NULL, updated_at = ?
                    WHERE project = ? AND id = ?
                    """,
                    (utc_now(), project, task["id"]),
                )
                self._event(
                    connection,
                    project,
                    "lease_expired",
                    agent=task["executor"],
                    task_id=task["id"],
                )
            candidate = connection.execute(
                """
                SELECT T.id FROM tasks AS T
                WHERE T.project = ? AND T.status = 'todo'
                  AND (? IS NULL OR T.id = ?)
                  AND NOT EXISTS (
                      SELECT 1 FROM dependencies AS D
                      JOIN tasks AS P ON P.project = D.project AND P.id = D.dependency_id
                      WHERE D.project = T.project AND D.task_id = T.id
                        AND P.status NOT IN ('done', 'accepted')
                  )
                  AND (
                      T.write_scope = '' OR NOT EXISTS (
                          SELECT 1 FROM tasks AS W
                          WHERE W.project = T.project AND W.status = 'working'
                            AND W.lease_until > ? AND W.write_scope = T.write_scope
                      )
                  )
                ORDER BY T.id LIMIT 1
                """,
                (project, task_id, task_id, now),
            ).fetchone()
            if not candidate:
                return None
            task_id = candidate["id"]
            now_text = utc_now()
            connection.execute(
                """
                UPDATE tasks SET status = 'working', executor = ?, session_id = ?, lease_until = ?,
                    started_at = ?, updated_at = ?
                WHERE project = ? AND id = ? AND status = 'todo'
                """,
                (agent, session_id, now + self.lease_seconds, now_text, now_text, project, task_id),
            )
            self._event(connection, project, "claimed", agent=agent, task_id=task_id)
            return task_id

    def heartbeat(self, project: str, agent: str, task_id: str) -> None:
        now = self.clock()
        with self._write() as connection:
            task = connection.execute(
                "SELECT status, executor, lease_until FROM tasks WHERE project = ? AND id = ?",
                (project, task_id),
            ).fetchone()
            if not task or task["status"] != "working" or task["executor"] != agent:
                raise LeaseLost("task is not owned by this agent")
            if task["lease_until"] <= now:
                raise LeaseLost("lease_lost")
            connection.execute(
                "UPDATE tasks SET lease_until = ? WHERE project = ? AND id = ?",
                (now + self.lease_seconds, project, task_id),
            )
            self._event(connection, project, "heartbeat", agent=agent, task_id=task_id)

    def complete(self, project: str, agent: str, task_id: str, verification: str) -> None:
        if not verification.strip():
            raise CoordinationError("verification is required")
        now = self.clock()
        with self._write() as connection:
            task = connection.execute(
                "SELECT status, executor, lease_until FROM tasks WHERE project = ? AND id = ?",
                (project, task_id),
            ).fetchone()
            if not task:
                raise CoordinationError("task not found in project")
            if task["status"] != "working":
                raise CoordinationError("only working tasks can complete")
            if task["executor"] != agent or task["lease_until"] <= now:
                raise LeaseLost("lease_lost")
            now_text = utc_now()
            connection.execute(
                """
                UPDATE tasks SET status = 'done', lease_until = NULL, verification = ?,
                    completed_at = ?, updated_at = ?
                WHERE project = ? AND id = ?
                """,
                (verification, now_text, now_text, project, task_id),
            )
            self._event(connection, project, "done", agent=agent, task_id=task_id)

    def set_blocked(self, project: str, task_id: str, reason: str | None) -> None:
        with self._write() as connection:
            task = connection.execute(
                "SELECT status FROM tasks WHERE project = ? AND id = ?",
                (project, task_id),
            ).fetchone()
            if not task:
                raise CoordinationError("task not found in project")
            if reason is not None and not reason.strip():
                raise CoordinationError("blocked reason must be non-empty")
            if reason is not None and task["status"] not in ("todo", "working"):
                raise CoordinationError("only todo or working tasks can be blocked")
            connection.execute(
                "UPDATE tasks SET blocked_reason = ?, updated_at = ? WHERE project = ? AND id = ?",
                (reason, utc_now(), project, task_id),
            )
            self._event(
                connection,
                project,
                "blocked" if reason is not None else "block_cleared",
                task_id=task_id,
            )

    def accept(self, project: str, task_ids: list[str], evidence: str) -> None:
        if not evidence.strip() or not task_ids:
            raise CoordinationError("explicit evidence and task scope are required")
        with self._write() as connection:
            for task_id in task_ids:
                task = connection.execute(
                    "SELECT status FROM tasks WHERE project = ? AND id = ?",
                    (project, task_id),
                ).fetchone()
                if not task or task["status"] != "done":
                    raise CoordinationError("only included done tasks can be accepted")
                now_text = utc_now()
                connection.execute(
                    """
                    UPDATE tasks SET status = 'accepted', acceptance_evidence = ?,
                        accepted_at = ?, updated_at = ?
                    WHERE project = ? AND id = ?
                    """,
                    (evidence, now_text, now_text, project, task_id),
                )
                self._event(connection, project, "accepted", task_id=task_id)

    def clean_accepted(self, project: str) -> list[str]:
        with self._write() as connection:
            accepted = [
                row["id"]
                for row in connection.execute(
                    "SELECT id FROM tasks WHERE project = ? AND status = 'accepted' ORDER BY id",
                    (project,),
                ).fetchall()
            ]
            if not accepted:
                return []
            placeholders = ",".join("?" for _ in accepted)
            connection.execute(
                f"""
                DELETE FROM dependencies WHERE project = ?
                  AND (task_id IN ({placeholders}) OR dependency_id IN ({placeholders}))
                """,
                (project, *accepted, *accepted),
            )
            connection.execute(
                f"DELETE FROM tasks WHERE project = ? AND id IN ({placeholders})",
                (project, *accepted),
            )
            self._event(connection, project, "accepted_cleaned", detail=",".join(accepted))
            return accepted

    def ask_question(
        self,
        project: str,
        session_id: str,
        question: str,
        scope: str = "project",
    ) -> str:
        if scope not in ("project", "session"):
            raise CoordinationError("invalid question scope")
        if not question.strip():
            raise CoordinationError("question is required")
        with self._write() as connection:
            if scope == "project":
                existing = connection.execute(
                    """
                    SELECT id FROM questions
                    WHERE project = ? AND scope = 'project' AND question = ?
                    ORDER BY id LIMIT 1
                    """,
                    (project, question),
                ).fetchone()
                if existing:
                    return existing["id"]
            question_id = self._next_number(connection, project, "questions", "Q")
            connection.execute(
                """
                INSERT INTO questions(project, id, session_id, scope, question, status)
                VALUES (?, ?, ?, ?, ?, 'open')
                """,
                (project, question_id, session_id, scope, question),
            )
            self._event(connection, project, "question_asked", task_id=question_id)
            return question_id

    def answer_question(self, project: str, question_id: str, answer: str) -> None:
        if not answer.strip():
            raise CoordinationError("answer is required")
        with self._write() as connection:
            result = connection.execute(
                """
                UPDATE questions SET status = 'answered', answer = ?
                WHERE project = ? AND id = ? AND status = 'open'
                """,
                (answer, project, question_id),
            )
            if result.rowcount != 1:
                raise CoordinationError("open question not found")
            self._event(connection, project, "question_answered", task_id=question_id)

    def questions(self, project: str, session_id: str) -> list[dict]:
        return self._read(
            """
            SELECT * FROM questions WHERE project = ? AND (scope = 'project' OR session_id = ?)
            ORDER BY id
            """,
            (project, session_id),
        )

    def tasks(self, project: str) -> list[dict]:
        tasks = self._read("SELECT * FROM tasks WHERE project = ? ORDER BY id", (project,))
        for task in tasks:
            try:
                task["acceptance_criteria"] = json.loads(task["acceptance_criteria"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise CoordinationError("stored acceptance_criteria is invalid") from exc
        return tasks

    def events(self, project: str) -> list[dict]:
        return self._read("SELECT * FROM events WHERE project = ? ORDER BY sequence", (project,))
