import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from workflow_ignore_config import load_workflow_ignore_config


class WorkflowIgnoreConfigTest(unittest.TestCase):
    def test_load_workflow_ignore_config_normalizes_string_lists(self):
        config = {
            "PR_MODE_IGNORE_FILES": ["TOC-ai.md", "", " /docs/example.md/ "],
            "PR_MODE_IGNORE_FOLDERS": ["tidb-cloud", " /ai/ "],
            "COMMIT_BASED_MODE_IGNORE_FILES": ["docs/skip.md"],
            "COMMIT_BASED_MODE_IGNORE_FOLDERS": [],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir, "workflow_ignore_config.json")
            config_path.write_text(json.dumps(config), encoding="utf-8")

            loaded = load_workflow_ignore_config(config_path)

        self.assertEqual(
            loaded["PR_MODE_IGNORE_FILES"],
            ["TOC-ai.md", "docs/example.md"],
        )
        self.assertEqual(loaded["PR_MODE_IGNORE_FOLDERS"], ["tidb-cloud", "ai"])
        self.assertEqual(loaded["COMMIT_BASED_MODE_IGNORE_FILES"], ["docs/skip.md"])
        self.assertEqual(loaded["COMMIT_BASED_MODE_IGNORE_FOLDERS"], [])

    def test_load_workflow_ignore_config_requires_all_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir, "workflow_ignore_config.json")
            config_path.write_text("{}", encoding="utf-8")

            with self.assertRaises(ValueError) as error:
                load_workflow_ignore_config(config_path)

        self.assertIn("Missing workflow ignore config key", str(error.exception))


if __name__ == "__main__":
    unittest.main()
