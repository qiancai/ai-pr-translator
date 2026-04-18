import sys
import tempfile
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

    def test_commit_repo_config_includes_target_ref_and_local_read_preference(self):
        with mock.patch.object(workflow, "SOURCE_REPO", "pingcap/docs"), mock.patch.object(
            workflow, "TARGET_REPO", "pingcap/docs"
        ), mock.patch.object(workflow, "TARGET_REPO_PATH", "/tmp/docs"), mock.patch.object(
            workflow, "TARGET_REF", "i18n-zh-release-8.5"
        ), mock.patch.object(
            workflow, "PREFER_LOCAL_TARGET_FOR_READ", True
        ):
            repo_config = workflow.get_commit_repo_config()

        self.assertEqual(repo_config["target_ref"], "i18n-zh-release-8.5")
        self.assertTrue(repo_config["prefer_local_target_for_read"])
        self.assertEqual(repo_config["target_local_path"], "/tmp/docs")

    def test_commit_ignore_files_allows_explicit_cloud_toc_files(self):
        with mock.patch.object(
            workflow,
            "IGNORE_FILES",
            ["TOC-tidb-cloud.md", "TOC-tidb-cloud-starter.md"],
        ), mock.patch.object(workflow, "SOURCE_FOLDER", ""), mock.patch.object(
            workflow,
            "SOURCE_FILES",
            "TOC-tidb-cloud.md,tidb-cloud/example.md",
        ):
            ignore_files = workflow.get_commit_ignore_files()

        self.assertNotIn("TOC-tidb-cloud.md", ignore_files)
        self.assertIn("TOC-tidb-cloud-starter.md", ignore_files)

    def test_commit_ignore_files_allows_prefixed_explicit_cloud_toc_files(self):
        with mock.patch.object(
            workflow,
            "IGNORE_FILES",
            ["TOC-tidb-cloud.md", "TOC-tidb-cloud-starter.md"],
        ), mock.patch.object(workflow, "SOURCE_FOLDER", "docs"), mock.patch.object(
            workflow,
            "SOURCE_FILES",
            "TOC-tidb-cloud.md",
        ):
            ignore_files = workflow.get_commit_ignore_files()

        self.assertNotIn("TOC-tidb-cloud.md", ignore_files)
        self.assertEqual(
            workflow.determine_file_processing_type(
                "docs/TOC-tidb-cloud.md",
                {},
                workflow.SPECIAL_FILES,
                ignore_files,
            ),
            "special_file_toc",
        )

    def test_process_modified_file_as_added_when_target_file_is_missing(self):
        pr_diff = "\n".join(
            [
                "File: guide.md",
                "@@ -1,1 +1,1 @@",
                "-Old",
                "+New",
                "-" * 80,
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.object(
            workflow,
            "get_source_file_content",
            return_value="# Guide\n\nNew content\n",
        ) as get_source_file_content, mock.patch.object(
            workflow,
            "process_added_files",
            return_value=(True, {}),
        ) as process_added_files:
            result = workflow._process_commit_modified_file(
                "guide.md",
                {},
                pr_diff,
                {"mode": "commit", "source_repo": "acme/docs", "base_ref": "base", "head_ref": "head", "changed_files": []},
                object(),
                object(),
                {
                    "target_repo": "acme/docs",
                    "target_local_path": tmpdir,
                    "prefer_local_target_for_read": True,
                    "source_language": "English",
                    "target_language": "Chinese",
                    "ignore_files": [],
                },
                None,
            )

        self.assertEqual(result["status"], "success")
        get_source_file_content.assert_called_once()
        process_added_files.assert_called_once()
        self.assertEqual(
            process_added_files.call_args.args[0],
            {"guide.md": "# Guide\n\nNew content\n"},
        )

    def test_process_modified_file_reports_regular_file_failure_reason(self):
        pr_diff = "\n".join(
            [
                "File: guide.md",
                "@@ -1,1 +1,1 @@",
                "-Old",
                "+New",
                "-" * 80,
            ]
        )
        failure_reason = "Target file guide.md is missing or could not map 1 modified source section(s)"

        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "guide.md").write_text("# Guide\n", encoding="utf-8")

            with mock.patch.object(
                workflow,
                "process_regular_modified_file",
                return_value=(False, failure_reason),
            ) as process_regular_modified_file:
                result = workflow._process_commit_modified_file(
                    "guide.md",
                    {},
                    pr_diff,
                    {"mode": "commit", "source_repo": "acme/docs", "base_ref": "base", "head_ref": "head", "changed_files": []},
                    object(),
                    object(),
                    {
                        "target_repo": "acme/docs",
                        "target_local_path": tmpdir,
                        "prefer_local_target_for_read": True,
                        "source_language": "English",
                        "target_language": "Chinese",
                        "ignore_files": [],
                    },
                    None,
                )

        self.assertEqual(result["status"], "failure")
        self.assertEqual(result["reason"], failure_reason)
        process_regular_modified_file.assert_called_once()

    def test_translation_stats_writes_failure_report(self):
        stats = workflow.TranslationStats()
        stats.mark_failure("cloud.md", "Matcher failed")

        with tempfile.TemporaryDirectory() as tmpdir:
            stats.write_failure_report(tmpdir)
            report = Path(tmpdir, "translation-failures.md").read_text(encoding="utf-8")

        self.assertIn("### Translation failures", report)
        self.assertIn("- `cloud.md`: Matcher failed", report)

    def test_translation_stats_removes_stale_failure_report_when_no_failures(self):
        stats = workflow.TranslationStats()

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir, "translation-failures.md")
            report_path.write_text("stale", encoding="utf-8")
            stats.write_failure_report(tmpdir)

            self.assertFalse(report_path.exists())


if __name__ == "__main__":
    unittest.main()
