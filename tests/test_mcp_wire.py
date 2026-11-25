import asyncio
import json
import logging
import sys
import tempfile
import unittest
from contextlib import AsyncExitStack
from functools import wraps
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "lucid-workflow" / "scripts"
sys.path.insert(0, str(SCRIPTS))

try:
    import uvicorn
    from mcp import Client
    from coordination.mcp_server import create_server
except ImportError:
    uvicorn = None
    Client = None
    create_server = None

logging.getLogger("mcp").setLevel(logging.CRITICAL)


class FakeClock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value


def with_clients(method):
    @wraps(method)
    async def run(self):
        await self.open_clients()
        try:
            return await method(self)
        finally:
            await self.close_clients()

    return run


@unittest.skipUnless(Client is not None, "需要安装 mcp==2.3.0")
class McpWireExperiments(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.database = self.base / "state" / "coordination.sqlite"
        self.clock = FakeClock()
        self.project = "wire-project"
        self.identities = [("A", "session-a"), ("B", "session-b"), ("C", "session-c")]
        self.client_stack = None
        self.clients = []
        self.http_server = None
        self.server_task = None
        await self.start_server()

    async def asyncTearDown(self):
        await self.stop_server()
        self.temporary.cleanup()

    async def start_server(self):
        mcp = create_server(self.database, lease_seconds=30, clock=self.clock)
        app = mcp.streamable_http_app(json_response=True, stateless_http=True)
        config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="error")
        self.http_server = uvicorn.Server(config)
        self.server_task = asyncio.create_task(self.http_server.serve())
        for _ in range(100):
            if self.http_server.started:
                break
            if self.server_task.done():
                exception = self.server_task.exception()
                raise exception or RuntimeError("MCP test server exited before startup")
            await asyncio.sleep(0.05)
        if not self.http_server.started:
            raise TimeoutError("MCP test server startup timed out")
        socket = self.http_server.servers[0].sockets[0]
        self.url = f"http://127.0.0.1:{socket.getsockname()[1]}/mcp"

    async def open_clients(self):
        self.client_stack = AsyncExitStack()
        self.clients = [
            await self.client_stack.enter_async_context(Client(self.url))
            for _ in self.identities
        ]

    async def close_clients(self):
        if self.client_stack is not None:
            await self.client_stack.aclose()
            self.client_stack = None
            self.clients = []

    async def stop_server(self):
        await self.close_clients()
        if self.http_server is not None and self.server_task is not None:
            self.http_server.should_exit = True
            await asyncio.wait_for(self.server_task, timeout=10)
            self.http_server = None
            self.server_task = None

    async def call(self, client_index, name, arguments):
        return await self.clients[client_index].call_tool(name, arguments)

    def data(self, result):
        self.assertFalse(result.is_error, result)
        self.assertIsNotNone(result.structured_content)
        return result.structured_content

    async def add_task(self, title, dependencies=None, write_scope=""):
        result = await self.call(
            0,
            "tasks.add",
            {
                "project": self.project,
                "title": title,
                "acceptance_criteria": ["MCP 调用验收条件"],
                "dependencies": dependencies,
                "write_scope": write_scope,
            },
        )
        return self.data(result)["task_id"]

    async def claim(self, client_index):
        agent, session = self.identities[client_index]
        result = await self.call(
            client_index,
            "tasks.claim_next",
            {"project": self.project, "agent": agent, "session_id": session},
        )
        return self.data(result)["task_id"]

    async def complete(self, client_index, task_id):
        agent, _ = self.identities[client_index]
        result = await self.call(
            client_index,
            "tasks.complete",
            {
                "project": self.project,
                "agent": agent,
                "task_id": task_id,
                "verification": "真实 MCP 调用验证通过",
            },
        )
        return self.data(result)["ok"]

    @with_clients
    async def test_three_http_clients_race_with_protocol_and_schema_checks(self):
        for client in self.clients:
            self.assertEqual(client.protocol_version, "2026-07-28")
            self.assertEqual(client.server_info.name, "lucid-workflow-coordination-poc")
        tools = await self.clients[0].list_tools()
        names = {tool.name for tool in tools.tools}
        self.assertIn("tasks.claim_next", names)
        self.assertIn("questions.answer", names)
        self.assertTrue(all(tool.output_schema for tool in tools.tools))
        task_id = await self.add_task("单任务竞争")
        results = await asyncio.gather(*(self.claim(index) for index in range(3)))
        self.assertEqual([value for value in results if value], [task_id])
        events = self.data(await self.call(2, "events.list", {"project": self.project}))["events"]
        self.assertEqual(len([event for event in events if event["action"] == "claimed"]), 1)

    @with_clients
    async def test_http_claim_by_id_only_claims_the_requested_task(self):
        tools = {tool.name: tool for tool in (await self.clients[0].list_tools()).tools}
        self.assertCountEqual(
            tools["tasks.claim"].input_schema["required"], ["project", "task_id", "agent", "session_id"]
        )
        first = await self.add_task("排在前面")
        target = await self.add_task("指定任务")

        async def claim_target(index, task_id):
            agent, session = self.identities[index]
            return self.data(await self.call(index, "tasks.claim", {
                "project": self.project, "task_id": task_id, "agent": agent, "session_id": session,
            }))

        claimed, duplicate = await asyncio.gather(claim_target(0, target), claim_target(1, target))
        self.assertCountEqual([claimed["task_id"], duplicate["task_id"]], [target, None])
        self.assertEqual(await claim_target(2, "TASK-999"), {"task_id": None})
        self.assertEqual(await self.claim(2), first)

    @with_clients
    async def test_three_http_clients_unlock_and_race_for_dependency(self):
        roots = [await self.add_task(f"前置 {index}") for index in range(3)]
        dependent = await self.add_task("汇合任务", dependencies=roots)
        claimed = await asyncio.gather(*(self.claim(index) for index in range(3)))
        self.assertCountEqual(claimed, roots)
        await asyncio.gather(
            *(self.complete(index, task_id) for index, task_id in enumerate(claimed))
        )
        results = await asyncio.gather(*(self.claim(index) for index in range(3)))
        self.assertEqual([value for value in results if value], [dependent])
        accepted = await self.call(
            0,
            "tasks.accept",
            {
                "project": self.project,
                "task_ids": [roots[0], roots[2]],
                "evidence": "用户准许提交这两个成果",
            },
        )
        self.assertTrue(self.data(accepted)["ok"])
        tasks = self.data(await self.call(1, "tasks.list", {"project": self.project}))["tasks"]
        status = {task["id"]: task["status"] for task in tasks}
        self.assertEqual(status[roots[1]], "done")
        self.assertEqual(status[dependent], "working")

    @with_clients
    async def test_http_question_sharing_and_business_error_mapping(self):
        question = "采用哪种存储？"
        first = self.data(
            await self.call(
                0,
                "questions.ask",
                {"project": self.project, "session_id": "session-a", "question": question},
            )
        )["question_id"]
        duplicate = self.data(
            await self.call(
                1,
                "questions.ask",
                {"project": self.project, "session_id": "session-b", "question": question},
            )
        )["question_id"]
        self.assertEqual(first, duplicate)
        self.data(
            await self.call(
                2,
                "questions.answer",
                {"project": self.project, "question_id": first, "answer": "项目外 SQLite"},
            )
        )
        questions = self.data(
            await self.call(
                0, "questions.list", {"project": self.project, "session_id": "session-a"}
            )
        )["questions"]
        self.assertEqual(questions[0]["answer"], "项目外 SQLite")
        task_id = await self.add_task("不得提前完成")
        failure = await self.call(
            0,
            "tasks.complete",
            {
                "project": self.project,
                "agent": "A",
                "task_id": task_id,
                "verification": "非法迁移",
            },
        )
        self.assertTrue(failure.is_error)
        self.assertEqual(failure.result_type, "complete")
        tasks = self.data(await self.call(0, "tasks.list", {"project": self.project}))["tasks"]
        self.assertEqual(tasks[0]["status"], "todo")

    @with_clients
    async def test_http_lease_takeover_rejects_stale_writer(self):
        task_id = await self.add_task("租约恢复")
        self.assertEqual(await self.claim(0), task_id)
        self.clock.value += 31
        self.assertEqual(await self.claim(1), task_id)
        stale = await self.call(
            0,
            "tasks.complete",
            {
                "project": self.project,
                "agent": "A",
                "task_id": task_id,
                "verification": "旧执行者写入",
            },
        )
        self.assertTrue(stale.is_error)
        self.assertTrue(await self.complete(1, task_id))
        tasks = self.data(await self.call(2, "tasks.list", {"project": self.project}))["tasks"]
        self.assertEqual(tasks[0]["status"], "done")
        self.assertEqual(tasks[0]["executor"], "B")

    @with_clients
    async def test_workflow_requirements_and_forest_use_external_state(self):
        project_root = self.base / "project"
        source = project_root / "src"
        source.mkdir(parents=True)
        (project_root / ".doc-forest").mkdir()
        (project_root / ".doc-forest.yml").write_text(
            'schema_version: "1.0"\n'
            'write_check:\n  excluded_paths: []\n'
            'forest:\n  allowlist: []\n  excluded_paths: []\n',
            encoding="utf-8",
        )
        (project_root / ".doc-forest" / "info.yml").write_text(
            'schema_version: "1.0"\n'
            'purpose: 测试项目\n'
            'contents_summary: 购买恢复\n'
            'directories:\n  - name: src\n',
            encoding="utf-8",
        )
        (source / "main.py").write_text(
            "def restore_purchase():\n    return True\n", encoding="utf-8"
        )
        (project_root / ".doc-forest" / "src" / "info.yml").parent.mkdir()
        (project_root / ".doc-forest" / "src" / "info.yml").write_text(
            'schema_version: "1.0"\n'
            'purpose: 购买恢复模块\n'
            'contents_summary: 恢复购买实现\n'
            'files:\n  - name: main.py\n    purpose: 恢复购买入口\n',
            encoding="utf-8",
        )
        bootstrap = self.data(
            await self.call(
                0,
                "workflow.bootstrap",
                {
                    "project": self.project,
                    "session_id": "session-a",
                    "project_root": str(project_root),
                },
            )
        )
        self.assertEqual(bootstrap["requirements"]["requirements"], [])
        upserted = self.data(
            await self.call(
                0,
                "requirements.upsert",
                {
                    "session_id": "session-a",
                    "project_root": str(project_root),
                    "text": "不得删除用户数据",
                },
            )
        )
        self.assertEqual(upserted["result"]["id"], "REQ-001")
        document = self.data(
            await self.call(
                1,
                "requirements.list",
                {"session_id": "session-a", "project_root": str(project_root)},
            )
        )["document"]
        self.assertEqual(document["requirements"][0]["text"], "不得删除用户数据")
        other_session = self.data(
            await self.call(
                2,
                "workflow.bootstrap",
                {
                    "project": self.project,
                    "session_id": "session-b",
                    "project_root": str(project_root),
                },
            )
        )
        self.assertEqual(other_session["requirements"]["requirements"], [])
        located = self.data(
            await self.call(
                0,
                "forest.locate",
                {
                    "project_root": str(project_root),
                    "task": "恢复购买",
                    "keywords": ["restore"],
                },
            )
        )["report"]
        self.assertIn("src/main.py", [item["path"] for item in located["locations"]])
        validation = self.data(
            await self.call(0, "forest.validate", {"project_root": str(project_root)})
        )["report"]
        self.assertTrue(validation["valid"], validation)
        self.assertFalse((project_root / ".cache").exists())
        self.assertFalse((project_root / ".todos.yml").exists())
        self.assertTrue(any((self.base / "state" / "cache").glob("*.sqlite")))

    @with_clients
    async def test_http_service_restart_recovers_external_state(self):
        task_id = await self.add_task("重启恢复")
        self.assertEqual(await self.claim(0), task_id)
        await self.complete(0, task_id)
        await self.stop_server()
        await self.start_server()
        await self.open_clients()
        tasks = self.data(await self.call(2, "tasks.list", {"project": self.project}))["tasks"]
        self.assertEqual(tasks[0]["id"], task_id)
        self.assertEqual(tasks[0]["status"], "done")
        self.assertTrue(self.database.is_file())
        self.assertNotIn(self.base / "project", self.database.parents)


if __name__ == "__main__":
    unittest.main()
