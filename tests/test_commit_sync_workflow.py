import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import commit_sync_workflow as workflow


class CommitSyncWorkflowHelpersTest(unittest.TestCase):
    def test_normalize_source_files_prefixes_folder(self):
        normalized = workflow.normalize_source_files("foo.md, ai/bar.md ,baz/qux.md", "ai")
        self.assertEqual(normalized, {"ai/foo.md", "ai/bar.md", "ai/baz/qux.md"})

    def test_filter_changed_files_matches_folder_and_previous_filename(self):
        changed_files = [
            SimpleNamespace(filename="ai/new.md", previous_filename=None),
            SimpleNamespace(filename="other/skip.md", previous_filename=None),
            SimpleNamespace(filename="docs/renamed.md", previous_filename="ai/old.md"),
        ]

        filtered = workflow.filter_changed_files(changed_files, source_folder="ai")
        self.assertEqual([item.filename for item in filtered], ["ai/new.md", "docs/renamed.md"])

        explicitly_filtered = workflow.filter_changed_files(
            changed_files,
            source_folder="ai",
            source_files="old.md",
        )
        self.assertEqual([item.filename for item in explicitly_filtered], ["docs/renamed.md"])

    def test_resolve_compare_range_requires_explicit_refs(self):
        with mock.patch.object(workflow, "SOURCE_BASE_REF", ""), mock.patch.object(
            workflow, "SOURCE_HEAD_REF", ""
        ):
            with self.assertRaises(ValueError):
                workflow.resolve_compare_range()

    def test_resolve_compare_range_returns_explicit_refs(self):
        with mock.patch.object(workflow, "SOURCE_BASE_REF", "abc123"), mock.patch.object(
            workflow, "SOURCE_HEAD_REF", "def456"
        ):
            self.assertEqual(workflow.resolve_compare_range(), ("abc123", "def456"))

    def test_build_exclude_folders_keeps_explicit_commit_target_folder(self):
        repo_config = {"target_language": "Chinese"}

        with mock.patch.object(workflow, "SKIP_TRANSLATING_AI_DOCS_TO_ZH", True), mock.patch.object(
            workflow, "SKIP_TRANSLATING_CLOUD_DOCS_TO_ZH", True
        ), mock.patch.object(workflow, "SOURCE_FOLDER", "ai"), mock.patch.object(
            workflow, "SOURCE_FILES", ""
        ), mock.patch.object(workflow, "AI_DOCS_FOLDER_NAME", "ai"), mock.patch.object(
            workflow, "CLOUD_FOLDER_NAME", "tidb-cloud"
        ):
            exclude_folders = workflow.build_exclude_folders(repo_config)

        self.assertNotIn("ai", exclude_folders)
        self.assertIn("tidb-cloud", exclude_folders)


if __name__ == "__main__":
    unittest.main()
