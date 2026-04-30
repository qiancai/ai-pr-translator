import subprocess
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import commit_sync_workflow as workflow
import commit_sync_workflow_local


class CommitSyncWorkflowHelpersTest(unittest.TestCase):
    def test_local_commit_sync_falls_back_to_trans_env_for_azure(self):
        config = {
            "AI_PROVIDER": "azure",
            "SOURCE_BASE_REF": "abc123",
            "SOURCE_HEAD_REF": "def456",
        }

        with mock.patch.dict(
            commit_sync_workflow_local.os.environ,
            {
                "GITHUB_TOKEN": "token",
                "TRANS_KEY": "trans-key",
                "TRANS_URL": "https://trans.example/v1",
            },
            clear=False,
        ), mock.patch.object(
            commit_sync_workflow_local, "build_config", return_value=config
        ), mock.patch.object(
            commit_sync_workflow_local, "resolve_cloud_source_files_for_local", return_value=(config, True)
        ), mock.patch.object(
            workflow, "main", return_value=0
        ):
            self.assertEqual(commit_sync_workflow_local.main(), 0)
            self.assertEqual(
                commit_sync_workflow_local.os.environ["AZURE_OPENAI_KEY"],
                "trans-key",
            )
            self.assertEqual(
                commit_sync_workflow_local.os.environ["OPENAI_BASE_URL"],
                "https://trans.example/v1",
            )

    def test_local_config_exposes_source_files_translation_mode(self):
        config = commit_sync_workflow_local.build_config("ai")

        self.assertEqual(
            config["SOURCE_FILES_TRANSLATION_MODE"],
            commit_sync_workflow_local.SOURCE_FILES_TRANSLATION_MODE,
        )

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

    def test_source_files_translation_mode_defaults_to_incremental(self):
        self.assertEqual(
            workflow.normalize_source_files_translation_mode(""),
            "incremental",
        )
        self.assertEqual(
            workflow.normalize_source_files_translation_mode("diff"),
            "incremental",
        )

    def test_source_files_translation_mode_accepts_full_aliases(self):
        self.assertEqual(
            workflow.normalize_source_files_translation_mode("full"),
            "full",
        )
        self.assertEqual(
            workflow.normalize_source_files_translation_mode("full_translation"),
            "full",
        )

    def test_source_files_translation_mode_rejects_unknown_value(self):
        with self.assertRaises(ValueError):
            workflow.normalize_source_files_translation_mode("partial")

    def test_full_translation_mode_only_applies_with_source_files(self):
        self.assertEqual(
            workflow.get_effective_source_files_translation_mode("full", ""),
            "incremental",
        )
        self.assertEqual(
            workflow.get_effective_source_files_translation_mode("full", "guide.md"),
            "full",
        )

    def test_full_translation_source_content_prefers_local_source_repo_path(self):
        completed = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="# Local head content\n",
            stderr="warning: ignored stderr\n",
        )

        with mock.patch(
            "subprocess.run",
            return_value=completed,
        ) as run, mock.patch.object(
            workflow, "get_source_file_content"
        ) as get_source_file_content:
            content = workflow.get_full_translation_source_content(
                "guide.md",
                {
                    "head_ref": "head-sha",
                    "source_repo_path": "/tmp/source",
                },
                object(),
            )

        self.assertEqual(content, "# Local head content\n")
        run.assert_called_once_with(
            ["git", "-C", "/tmp/source", "show", "head-sha:guide.md"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        get_source_file_content.assert_not_called()

    def test_full_translation_source_content_redacts_local_source_path_on_git_failure(self):
        completed = subprocess.CompletedProcess(
            args=[],
            returncode=128,
            stdout="",
            stderr="fatal: cannot change to '/tmp/private-source': No such file or directory",
        )

        with mock.patch("subprocess.run", return_value=completed):
            with self.assertRaises(RuntimeError) as context:
                workflow.get_full_translation_source_content(
                    "missing.md",
                    {
                        "head_ref": "head-sha",
                        "source_repo_path": "/tmp/private-source",
                    },
                    object(),
                )

        self.assertIn("[SOURCE_REPO_PATH]", str(context.exception))
        self.assertNotIn("/tmp/private-source", str(context.exception))
        self.assertNotIn("git -C", str(context.exception))

    def test_missing_env_message_omits_source_base_ref_in_full_mode(self):
        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(workflow, "SOURCE_REPO", ""))
            stack.enter_context(mock.patch.object(workflow, "TARGET_REPO", "acme/docs-cn"))
            stack.enter_context(mock.patch.object(workflow, "GITHUB_TOKEN", "token"))
            stack.enter_context(mock.patch.object(workflow, "TARGET_REPO_PATH", "/tmp/target"))
            stack.enter_context(mock.patch.object(workflow, "SOURCE_BASE_REF", ""))
            stack.enter_context(mock.patch.object(workflow, "SOURCE_HEAD_REF", "head"))
            stack.enter_context(mock.patch.object(workflow, "SOURCE_FILES", "guide.md"))
            stack.enter_context(mock.patch.object(workflow, "SOURCE_FILES_TRANSLATION_MODE", "full"))
            thread_safe_print = stack.enter_context(mock.patch.object(workflow, "thread_safe_print"))

            result = workflow.main()

        printed = "\n".join(str(call.args[0]) for call in thread_safe_print.call_args_list)
        self.assertEqual(result, 1)
        self.assertNotIn("SOURCE_BASE_REF:", printed)
        self.assertIn("SOURCE_HEAD_REF:", printed)

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

        with mock.patch.object(
            workflow,
            "COMMIT_BASED_MODE_IGNORE_FOLDERS",
            ("ai", "tidb-cloud"),
        ), mock.patch.object(workflow, "SOURCE_FOLDER", "ai"), mock.patch.object(
            workflow, "SOURCE_FILES", ""
        ):
            exclude_folders = workflow.build_exclude_folders(repo_config)

        self.assertNotIn("ai", exclude_folders)
        self.assertIn("tidb-cloud", exclude_folders)

    def test_commit_ignore_folders_do_not_inherit_pr_mode_defaults(self):
        with mock.patch.object(
            workflow,
            "COMMIT_BASED_MODE_IGNORE_FOLDERS",
            ("custom-folder",),
        ):
            self.assertEqual(workflow.get_commit_ignore_folders(), ["custom-folder"])

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

    def test_commit_ignore_files_do_not_inherit_pr_mode_defaults(self):
        with mock.patch.object(
            workflow,
            "COMMIT_BASED_MODE_IGNORE_FILES",
            ("custom.md",),
        ):
            ignore_files = workflow.get_commit_ignore_files()

        self.assertEqual(ignore_files, ["custom.md"])

    def test_commit_mode_treats_pr_ignored_toc_files_as_special_toc_files(self):
        ignore_files = workflow.get_commit_ignore_files()

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

    def test_full_mode_fetches_source_files_even_when_not_in_compare_diff(self):
        fake_github_client = object()
        fake_ai_client = SimpleNamespace(model="fake-model")

        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(workflow, "SOURCE_REPO", "acme/docs"))
            stack.enter_context(mock.patch.object(workflow, "TARGET_REPO", "acme/docs-cn"))
            stack.enter_context(mock.patch.object(workflow, "GITHUB_TOKEN", "token"))
            stack.enter_context(mock.patch.object(workflow, "TARGET_REPO_PATH", "/tmp/target"))
            stack.enter_context(mock.patch.object(workflow, "SOURCE_BASE_REF", ""))
            stack.enter_context(mock.patch.object(workflow, "SOURCE_HEAD_REF", "head"))
            stack.enter_context(mock.patch.object(workflow, "SOURCE_FILES", "guide.md"))
            stack.enter_context(mock.patch.object(workflow, "SOURCE_FOLDER", ""))
            stack.enter_context(mock.patch.object(workflow, "SOURCE_FILES_TRANSLATION_MODE", "full"))
            stack.enter_context(mock.patch.object(workflow, "SOURCE_REPO_PATH", ""))
            stack.enter_context(mock.patch.object(workflow, "TARGET_REF", ""))
            stack.enter_context(mock.patch.object(workflow, "clean_temp_output_dir"))
            stack.enter_context(mock.patch.object(workflow.Auth, "Token", return_value="token-auth"))
            stack.enter_context(mock.patch.object(workflow, "Github", return_value=fake_github_client))
            stack.enter_context(mock.patch.object(workflow, "UnifiedAIClient", return_value=fake_ai_client))
            stack.enter_context(mock.patch.object(workflow, "load_glossary", return_value=[]))
            stack.enter_context(mock.patch.object(workflow, "create_glossary_matcher", return_value=None))
            build_commit_diff_context = stack.enter_context(
                mock.patch.object(
                    workflow,
                    "build_commit_diff_context",
                )
            )
            build_local_commit_diff_context = stack.enter_context(
                mock.patch.object(workflow, "build_local_commit_diff_context")
            )
            analyze_source_changes = stack.enter_context(
                mock.patch.object(workflow, "analyze_source_changes")
            )
            stack.enter_context(mock.patch.object(workflow, "get_source_file_content", return_value="# Guide\n"))
            process_added_files = stack.enter_context(
                mock.patch.object(workflow, "process_added_files", return_value=(True, {}))
            )
            stack.enter_context(mock.patch.object(workflow, "git_add_successful_task_changes"))
            stack.enter_context(mock.patch.object(workflow.TranslationStats, "write_failure_report"))
            result = workflow.main()

        self.assertEqual(result, 0)
        build_commit_diff_context.assert_not_called()
        build_local_commit_diff_context.assert_not_called()
        analyze_source_changes.assert_not_called()
        process_added_files.assert_called_once()
        self.assertEqual(process_added_files.call_args.args[0], {"guide.md": "# Guide\n"})
        self.assertTrue(process_added_files.call_args.kwargs["overwrite_existing"])

    def test_apply_full_translation_mode_uses_head_file_content_and_removes_incremental_work(self):
        changed_files = [
            SimpleNamespace(filename="guide.md", status="modified"),
            SimpleNamespace(filename="TOC.md", status="modified"),
            SimpleNamespace(filename="deleted.md", status="removed"),
            SimpleNamespace(filename="renamed-new.md", previous_filename="renamed-old.md", status="renamed"),
            SimpleNamespace(filename="image.png", status="modified"),
        ]
        added_sections = {"guide.md": {"sections": {}}, "other.md": {"sections": {}}}
        modified_sections = {
            "guide.md": {"sections": {}},
            "TOC.md": {"sections": {}},
            "renamed-new.md": {"sections": {}},
        }
        deleted_sections = {"guide.md": {"1": "# Old"}, "deleted.md": {"1": "# Removed"}}
        added_files = {}
        toc_files = {"TOC.md": {"type": "toc"}}
        keyword_files = {}
        stats = workflow.TranslationStats()
        calls = []

        def fake_get_source_file_content(file_path, diff_context, github_client, ref_name="head_ref"):
            calls.append((file_path, ref_name, diff_context["head_ref"]))
            return f"HEAD content for {file_path}"

        with mock.patch.object(
            workflow,
            "get_source_file_content",
            side_effect=fake_get_source_file_content,
        ):
            full_paths = workflow.apply_source_files_full_translation_mode(
                "guide.md,TOC.md,renamed-old.md,unchanged.md",
                "",
                changed_files,
                {"head_ref": "latest-head"},
                object(),
                [],
                added_sections,
                modified_sections,
                deleted_sections,
                added_files,
                toc_files,
                keyword_files,
                stats,
            )

        self.assertEqual(full_paths, {"guide.md", "TOC.md", "renamed-new.md", "unchanged.md"})
        self.assertCountEqual(
            calls,
            [
                ("guide.md", "head_ref", "latest-head"),
                ("renamed-new.md", "head_ref", "latest-head"),
                ("TOC.md", "head_ref", "latest-head"),
                ("unchanged.md", "head_ref", "latest-head"),
            ],
        )
        self.assertEqual(
            added_files,
            {
                "guide.md": "HEAD content for guide.md",
                "renamed-new.md": "HEAD content for renamed-new.md",
                "TOC.md": "HEAD content for TOC.md",
                "unchanged.md": "HEAD content for unchanged.md",
            },
        )
        self.assertNotIn("guide.md", added_sections)
        self.assertIn("other.md", added_sections)
        self.assertNotIn("guide.md", modified_sections)
        self.assertNotIn("TOC.md", modified_sections)
        self.assertNotIn("renamed-new.md", modified_sections)
        self.assertNotIn("guide.md", deleted_sections)
        self.assertIn("deleted.md", deleted_sections)
        self.assertEqual(toc_files, {})
        self.assertEqual(stats.failed, [])

    def test_collect_toc_scope_added_files_detects_new_cloud_links(self):
        toc_files = {
            "TOC-tidb-cloud.md": {
                "type": "toc",
                "source_base_content": "- [Old](/tidb-cloud/old.md)\n",
                "source_head_content": "\n".join(
                    [
                        "- [Old](/tidb-cloud/old.md)",
                        "- [New](/tidb-cloud/a-b-c.md#overview)",
                        "- [Shared](/shared/a-b-c.md)",
                    ]
                ),
            }
        }

        self.assertEqual(
            workflow.collect_toc_scope_added_files(toc_files),
            {"tidb-cloud/a-b-c.md", "shared/a-b-c.md"},
        )

    def test_collect_toc_scope_added_files_uses_aggregate_toc_scope(self):
        toc_files = {
            "TOC-tidb-cloud.md": {
                "type": "toc",
                "source_base_content": "- [Old](/tidb-cloud/old.md)\n",
                "source_head_content": "\n".join(
                    [
                        "- [Old](/tidb-cloud/old.md)",
                        "- [Shared](/shared/already-linked.md)",
                        "- [New](/shared/newly-linked.md)",
                    ]
                ),
            },
            "TOC-tidb-cloud-starter.md": {
                "type": "toc",
                "source_base_content": "- [Shared](/shared/already-linked.md)\n",
                "source_head_content": "- [Shared](/shared/already-linked.md)\n",
            },
        }

        self.assertEqual(
            workflow.collect_toc_scope_added_files(toc_files),
            {"shared/newly-linked.md"},
        )

    def test_collect_toc_scope_added_files_reads_all_configured_tocs(self):
        contents = {
            ("TOC-tidb-cloud.md", "base_ref"): "- [Old](/tidb-cloud/old.md)\n",
            ("TOC-tidb-cloud.md", "head_ref"): "\n".join(
                [
                    "- [Old](/tidb-cloud/old.md)",
                    "- [Shared](/shared/already-linked.md)",
                    "- [New](/shared/newly-linked.md)",
                ]
            ),
            ("TOC-tidb-cloud-starter.md", "base_ref"): "- [Shared](/shared/already-linked.md)\n",
            ("TOC-tidb-cloud-starter.md", "head_ref"): "- [Shared](/shared/already-linked.md)\n",
        }

        def fake_get_source_ref_content(file_path, diff_context, github_client, ref_name):
            return contents[(file_path, ref_name)]

        with mock.patch.object(
            workflow,
            "get_source_ref_content",
            side_effect=fake_get_source_ref_content,
        ):
            scope_added = workflow.collect_toc_scope_added_files(
                {},
                {"base_ref": "base", "head_ref": "head"},
                object(),
                ["TOC-tidb-cloud.md", "TOC-tidb-cloud-starter.md"],
            )

        self.assertEqual(scope_added, {"shared/newly-linked.md"})

    def test_apply_toc_scope_added_files_queues_full_translation_and_keeps_toc_work(self):
        toc_files = {
            "TOC-tidb-cloud.md": {
                "type": "toc",
                "source_base_content": "- [Old](/tidb-cloud/old.md)\n",
                "source_head_content": "\n".join(
                    [
                        "- [Old](/tidb-cloud/old.md)",
                        "- [New](/shared/a-b-c.md)",
                    ]
                ),
            }
        }
        added_sections = {"shared/a-b-c.md": {"sections": {}}, "other.md": {"sections": {}}}
        modified_sections = {"shared/a-b-c.md": {"sections": {}}}
        deleted_sections = {"shared/a-b-c.md": {"1": "# Old"}}
        added_files = {"shared/a-b-c.md": "diff-added content"}
        keyword_files = {"shared/a-b-c.md": {"type": "keyword"}}
        stats = workflow.TranslationStats()

        with mock.patch.object(
            workflow,
            "get_full_translation_source_content",
            return_value="# HEAD content\n",
        ) as get_full_translation_source_content:
            queued_paths = workflow.apply_toc_scope_added_files(
                toc_files,
                {"head_ref": "head"},
                object(),
                [],
                added_sections,
                modified_sections,
                deleted_sections,
                added_files,
                keyword_files,
                stats,
            )

        self.assertEqual(queued_paths, {"shared/a-b-c.md"})
        get_full_translation_source_content.assert_called_once()
        self.assertEqual(added_files, {"shared/a-b-c.md": "# HEAD content\n"})
        self.assertNotIn("shared/a-b-c.md", added_sections)
        self.assertIn("other.md", added_sections)
        self.assertNotIn("shared/a-b-c.md", modified_sections)
        self.assertNotIn("shared/a-b-c.md", deleted_sections)
        self.assertNotIn("shared/a-b-c.md", keyword_files)
        self.assertIn("TOC-tidb-cloud.md", toc_files)
        self.assertEqual(stats.failed, [])

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
