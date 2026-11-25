import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "lucid-workflow" / "scripts"
sys.path.insert(0, str(SCRIPTS))

from coordination.core import CoordinationError, Coordinator, LeaseLost


class FakeClock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds):
        self.value += seconds


class CoordinationExperiments(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.project_root = self.base / "project"
        self.project_root.mkdir()
        self.database = self.base / "state" / "coordination.sqlite"
        self.clock = FakeClock()
        self.coordinator = Coordinator(self.database, lease_seconds=30, clock=self.clock)
        self.project = "project"
        self.agents = [("A", "session-a"), ("B", "session-b"), ("C", "session-c")]

    def tearDown(self):
        self.temporary.cleanup()

    def task(self, task_id):
        return next(item for item in self.coordinator.tasks(self.project) if item["id"] == task_id)

    def claim(self, agent_index, task_id=None):
        agent, session = self.agents[agent_index]
        if task_id is not None:
            return self.coordinator.claim(self.project, task_id, agent, session)
        return self.coordinator.claim_next(self.project, agent, session)

    def complete(self, agent_index, task_id):
        agent, _ = self.agents[agent_index]
        self.coordinator.complete(self.project, agent, task_id, "模拟验证通过")

    def add_task(self, title, dependencies=None, write_scope=""):
        return self.coordinator.add_task(
            self.project,
            title,
            acceptance_criteria=["模拟验收条件"],
            dependencies=dependencies,
            write_scope=write_scope,
        )

    def test_task_metadata_blocking_and_id_monotonicity(self):
        first = self.add_task("完整任务字段")
        self.assertEqual(self.task(first)["acceptance_criteria"], ["模拟验收条件"])
        self.assertIsNotNone(self.task(first)["created_at"])
        self.assertEqual(self.claim(0), first)
        self.assertIsNotNone(self.task(first)["started_at"])
        self.coordinator.set_blocked(self.project, first, "等待外部事实")
        self.assertEqual(self.task(first)["blocked_reason"], "等待外部事实")
        self.coordinator.set_blocked(self.project, first, None)
        self.assertIsNone(self.task(first)["blocked_reason"])
        self.complete(0, first)
        self.assertIsNotNone(self.task(first)["completed_at"])
        self.coordinator.accept(self.project, [first], "用户明确肯定成果")
        accepted = self.task(first)
        self.assertEqual(accepted["status"], "accepted")
        self.assertIsNotNone(accepted["accepted_at"])
        accepted_event = next(
            event for event in self.coordinator.events(self.project) if event["action"] == "accepted"
        )
        self.assertIsNone(accepted_event["detail"])
        self.assertEqual(self.coordinator.clean_accepted(self.project), [first])
        second = self.add_task("下一编号")
        self.assertEqual(second, "TASK-002")
        with self.assertRaises(CoordinationError):
            self.coordinator.add_task(self.project, "缺少验收条件", [])

    def test_three_agents_race_for_one_task(self):
        task_id = self.add_task("唯一任务")
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(self.claim, range(3)))
        self.assertEqual([value for value in results if value], [task_id])
        task = self.task(task_id)
        self.assertEqual(task["status"], "working")
        self.assertIn(task["executor"], {"A", "B", "C"})
        claims = [event for event in self.coordinator.events(self.project) if event["action"] == "claimed"]
        self.assertEqual(len(claims), 1)

    def test_claim_by_id_skips_earlier_tasks_and_keeps_one_active_task_per_agent(self):
        first = self.add_task("排在前面")
        target = self.add_task("指定任务")

        self.assertIsNone(self.claim(0, "TASK-999"))
        self.assertIsNone(self.coordinator.claim("other-project", target, "A", "session-a"))
        self.assertEqual(self.claim(0, target), target)
        self.assertEqual(self.task(first)["status"], "todo")
        self.assertEqual(self.task(target)["session_id"], "session-a")
        lease = self.task(target)["lease_until"]
        self.clock.advance(1)
        self.assertEqual(self.claim(0, target), target)
        self.assertEqual(self.task(target)["lease_until"], lease)
        self.assertIsNone(self.claim(0, first))
        self.assertEqual(self.claim(0), target)
        self.assertEqual(self.claim(1), first)
        self.complete(0, target)
        self.assertIsNone(self.claim(2, target))
        self.coordinator.accept(self.project, [target], "用户已验收")
        self.assertIsNone(self.claim(2, target))
        claims = [event["task_id"] for event in self.coordinator.events(self.project) if event["action"] == "claimed"]
        self.assertEqual(claims, [target, first])

    def test_claim_by_id_respects_dependencies_and_write_scope(self):
        prerequisite = self.add_task("前置任务")
        conflicting = self.add_task("同范围任务", write_scope="shared")
        target = self.add_task("指定任务", dependencies=[prerequisite], write_scope="shared")

        self.assertIsNone(self.claim(0, target))
        self.assertEqual(self.claim(1, prerequisite), prerequisite)
        self.assertEqual(self.claim(2, conflicting), conflicting)
        self.complete(1, prerequisite)
        self.assertIsNone(self.claim(0, target))
        self.complete(2, conflicting)
        self.assertEqual(self.claim(0, target), target)

    def test_claim_by_id_and_claim_next_race_for_one_task(self):
        target = self.add_task("唯一任务")
        with ThreadPoolExecutor(max_workers=3) as pool:
            attempts = [pool.submit(self.claim, index, task_id) for index, task_id in enumerate((target, target, None))]
            results = [attempt.result() for attempt in attempts]
        self.assertEqual([value for value in results if value], [target])
        self.assertEqual(self.task(target)["executor"], self.agents[results.index(target)][0])
        claims = [event for event in self.coordinator.events(self.project) if event["action"] == "claimed"]
        self.assertEqual(len(claims), 1)

    def test_claim_by_id_takes_over_only_the_requested_expired_task(self):
        first = self.add_task("其他过期任务", write_scope="shared")
        target = self.add_task("指定的过期任务")
        self.assertEqual(self.claim(0, first), first)
        self.assertEqual(self.claim(1, target), target)
        self.assertIsNone(self.claim(2, target))

        self.clock.advance(30)
        self.assertEqual(self.claim(2, target), target)
        with self.assertRaises(LeaseLost):
            self.complete(1, target)
        self.assertEqual(self.task(target)["executor"], "C")
        self.assertEqual(self.task(first)["executor"], "A")
        expired = [event["task_id"] for event in self.coordinator.events(self.project) if event["action"] == "lease_expired"]
        self.assertEqual(expired, [target])
        self.assertEqual(self.claim(0), first)

    def test_three_agents_respect_scope_and_unlock_dependency(self):
        first = self.add_task("支付恢复", write_scope="payment")
        second = self.add_task("支付退款", write_scope="payment")
        third = self.add_task("文档", write_scope="docs")
        dependent = self.add_task(
            "汇总报告", dependencies=[first, third], write_scope="reports"
        )
        self.assertEqual(self.claim(0), first)
        self.assertEqual(self.claim(1), third)
        self.assertIsNone(self.claim(2))
        self.complete(0, first)
        self.assertEqual(self.claim(2), second)
        self.assertIsNone(self.claim(0))
        self.complete(1, third)
        self.assertEqual(self.claim(0), dependent)
        self.complete(2, second)
        self.complete(0, dependent)
        self.assertTrue(all(task["status"] == "done" for task in self.coordinator.tasks(self.project)))

    def test_three_agents_race_for_unlocked_dependency(self):
        roots = [self.add_task(f"前置 {index}") for index in range(3)]
        dependent = self.add_task("汇合任务", dependencies=roots)
        with ThreadPoolExecutor(max_workers=3) as pool:
            claimed = list(pool.map(self.claim, range(3)))
        self.assertCountEqual(claimed, roots)
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(self.complete, index, task_id) for index, task_id in enumerate(claimed)]
            for future in futures:
                future.result()
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(self.claim, range(3)))
        self.assertEqual([value for value in results if value], [dependent])

    def test_lease_heartbeat_takeover_and_stale_writer_rejected(self):
        task_id = self.add_task("可恢复任务")
        self.assertEqual(self.claim(0), task_id)
        self.clock.advance(10)
        self.coordinator.heartbeat(self.project, "A", task_id)
        self.clock.advance(20)
        self.assertIsNone(self.claim(1))
        self.clock.advance(11)
        self.assertEqual(self.claim(1), task_id)
        with self.assertRaises(LeaseLost):
            self.coordinator.complete(self.project, "A", task_id, "过期写入")
        with self.assertRaises(LeaseLost):
            self.coordinator.heartbeat(self.project, "A", task_id)
        self.complete(1, task_id)
        task = self.task(task_id)
        self.assertEqual(task["status"], "done")
        self.assertEqual(task["executor"], "B")
        expired = [event for event in self.coordinator.events(self.project) if event["action"] == "lease_expired"]
        self.assertEqual(len(expired), 1)
        self.assertEqual(expired[0]["agent"], "A")

    def test_project_answer_is_shared_by_three_sessions(self):
        question_id = self.coordinator.ask_question(self.project, "session-a", "采用哪种存储？")
        duplicate = self.coordinator.ask_question(self.project, "session-b", "采用哪种存储？")
        self.assertEqual(duplicate, question_id)
        for _, session in self.agents:
            questions = self.coordinator.questions(self.project, session)
            self.assertEqual(len(questions), 1)
            self.assertEqual(questions[0]["status"], "open")
        self.coordinator.answer_question(self.project, question_id, "项目外 SQLite")
        answered_duplicate = self.coordinator.ask_question(self.project, "session-c", "采用哪种存储？")
        self.assertEqual(answered_duplicate, question_id)
        for _, session in self.agents:
            question = self.coordinator.questions(self.project, session)[0]
            self.assertEqual(question["status"], "answered")
            self.assertEqual(question["answer"], "项目外 SQLite")
        private = self.coordinator.ask_question(
            self.project, "session-b", "当前会话的临时选择", scope="session"
        )
        self.assertFalse(any(q["id"] == private for q in self.coordinator.questions(self.project, "session-a")))
        self.assertTrue(any(q["id"] == private for q in self.coordinator.questions(self.project, "session-b")))
        self.assertFalse(any(q["id"] == private for q in self.coordinator.questions(self.project, "session-c")))

    def test_commit_acceptance_is_scoped_and_audited(self):
        task_ids = [self.add_task(f"成果 {index}") for index in range(3)]
        for index, task_id in enumerate(task_ids):
            self.assertEqual(self.claim(index), task_id)
            self.complete(index, task_id)
        self.coordinator.accept(
            self.project, [task_ids[0], task_ids[2]], "用户准许提交这两个成果"
        )
        self.assertEqual(self.task(task_ids[0])["status"], "accepted")
        self.assertEqual(self.task(task_ids[1])["status"], "done")
        self.assertEqual(self.task(task_ids[2])["status"], "accepted")
        self.assertCountEqual(self.coordinator.clean_accepted(self.project), [task_ids[0], task_ids[2]])
        remaining = self.coordinator.tasks(self.project)
        self.assertEqual([task["id"] for task in remaining], [task_ids[1]])
        self.assertEqual(remaining[0]["status"], "done")
        accepted_events = [event for event in self.coordinator.events(self.project) if event["action"] == "accepted"]
        self.assertCountEqual([event["task_id"] for event in accepted_events], [task_ids[0], task_ids[2]])

    def test_invalid_transition_and_cross_project_write_are_rejected(self):
        task_id = self.add_task("保护状态")
        with self.assertRaises(CoordinationError):
            self.coordinator.complete(self.project, "A", task_id, "提前完成")
        self.assertEqual(self.task(task_id)["status"], "todo")
        self.assertEqual(self.claim(0), task_id)
        with self.assertRaises(CoordinationError):
            self.coordinator.complete("other-project", "A", task_id, "跨项目写入")
        self.assertEqual(self.task(task_id)["status"], "working")
        self.complete(0, task_id)
        with self.assertRaises(CoordinationError):
            self.coordinator.accept(self.project, [task_id], "")
        self.assertEqual(self.task(task_id)["status"], "done")

    def test_external_state_keeps_project_dotfiles_to_forest(self):
        (self.project_root / ".doc-forest").mkdir()
        (self.project_root / ".doc-forest" / "info.yml").write_text("purpose: 测试\n", encoding="utf-8")
        (self.project_root / ".doc-forest.yml").write_text("schema_version: '1.0'\n", encoding="utf-8")
        task_id = self.add_task("无项目状态文件")
        self.assertEqual(self.claim(0), task_id)
        self.complete(0, task_id)
        self.coordinator.accept(self.project, [task_id], "用户肯定成果")
        self.coordinator.clean_accepted(self.project)
        dot_names = {path.name for path in self.project_root.iterdir() if path.name.startswith(".")}
        self.assertEqual(dot_names, {".doc-forest", ".doc-forest.yml"})
        self.assertTrue(self.database.is_file())
        self.assertNotIn(self.project_root, self.database.parents)
        self.assertTrue(self.coordinator.events(self.project))


if __name__ == "__main__":
    unittest.main()
