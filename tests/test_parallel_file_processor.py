import sys
import unittest
from pathlib import Path
from unittest import mock


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from parallel_file_processor import (
    count_unique_file_paths,
    get_parallel_file_threshold,
    get_parallel_file_workers,
    make_file_task,
    run_file_tasks,
    should_parallelize_file_processing,
)


class ParallelFileProcessorTest(unittest.TestCase):
    def test_count_unique_file_paths_supports_dicts_and_lists(self):
        self.assertEqual(
            count_unique_file_paths(
                {"a.md": {}, "b.md": {}},
                ["b.md", "c.md"],
                [],
                None,
            ),
            3,
        )

    def test_should_parallelize_uses_threshold_and_worker_env(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual(get_parallel_file_threshold(), 6)
            self.assertEqual(get_parallel_file_workers(), 4)
            self.assertFalse(should_parallelize_file_processing(6))
            self.assertTrue(should_parallelize_file_processing(7))

        with mock.patch.dict(
            "os.environ",
            {
                "DIFF_PARALLEL_FILE_THRESHOLD": "10",
                "DIFF_PARALLEL_WORKERS": "3",
            },
        ):
            self.assertFalse(should_parallelize_file_processing(10))
            self.assertTrue(should_parallelize_file_processing(11))

        with mock.patch.dict(
            "os.environ",
            {
                "DIFF_PARALLEL_FILE_THRESHOLD": "10",
                "DIFF_PARALLEL_WORKERS": "1",
            },
        ):
            self.assertFalse(should_parallelize_file_processing(11))

    def test_run_file_tasks_preserves_order_and_captures_exceptions(self):
        tasks = [
            make_file_task("a.md", lambda: "a"),
            make_file_task("b.md", lambda: "b"),
            make_file_task("bad.md", lambda: (_ for _ in ()).throw(ValueError("boom"))),
            make_file_task("c.md", lambda: "c"),
        ]

        results = run_file_tasks(tasks, "test files", parallel_enabled=True, max_workers=3)

        self.assertEqual([result["file_path"] for result in results], ["a.md", "b.md", "bad.md", "c.md"])
        self.assertEqual([result["result"] for result in results[:2]], ["a", "b"])
        self.assertFalse(results[2]["ok"])
        self.assertIn("boom", results[2]["error"])
        self.assertEqual(results[3]["result"], "c")

    def test_run_file_tasks_disables_parallel_for_duplicate_resources(self):
        tasks = [
            make_file_task("source/a.md", lambda: "a", resource_key="shared-output"),
            make_file_task("source/b.md", lambda: "b", resource_key="shared-output"),
        ]

        with mock.patch("parallel_file_processor.ThreadPoolExecutor") as executor:
            results = run_file_tasks(tasks, "test files", parallel_enabled=True, max_workers=2)

        executor.assert_not_called()
        self.assertEqual([result["result"] for result in results], ["a", "b"])

    def test_run_file_tasks_converts_chunk_failures_to_task_results(self):
        tasks = [
            make_file_task("a.md", lambda: "a"),
            make_file_task("b.md", lambda: "b"),
            make_file_task("c.md", lambda: "c"),
        ]

        with mock.patch("parallel_file_processor._run_chunk", side_effect=RuntimeError("chunk boom")):
            results = run_file_tasks(tasks, "test files", parallel_enabled=True, max_workers=2)

        self.assertEqual([result["file_path"] for result in results], ["a.md", "b.md", "c.md"])
        self.assertTrue(all(not result["ok"] for result in results))
        self.assertTrue(all("chunk boom" in result["error"] for result in results))


if __name__ == "__main__":
    unittest.main()
