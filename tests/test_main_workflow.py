import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import main_workflow
import main_workflow_local


class MainWorkflowImageOnlyPrTest(unittest.TestCase):
    def test_local_workflow_infers_target_pr_url(self):
        docs_target = main_workflow_local._target_for_source_pr(
            "https://github.com/pingcap/docs/pull/22655"
        )
        docs_cn_target = main_workflow_local._target_for_source_pr(
            "https://github.com/pingcap/docs-cn/pull/22655"
        )

        self.assertEqual(docs_target["target_pr_url"], "https://github.com/pingcap/docs-cn/pull/0")
        self.assertEqual(docs_target["target_repo_path"], main_workflow_local.DOCS_CN_LOCAL_PATH)
        self.assertEqual(docs_cn_target["target_pr_url"], "https://github.com/pingcap/docs/pull/0")
        self.assertEqual(docs_cn_target["target_repo_path"], main_workflow_local.DOCS_LOCAL_PATH)

    def test_local_workflow_prefers_explicit_azure_env_over_trans_env(self):
        with mock.patch.dict(
            main_workflow_local.os.environ,
            {
                "GITHUB_TOKEN": "token",
                "AZURE_OPENAI_KEY": "azure-key",
                "AZURE_OPENAI_BASE_URL": "https://azure.example/v1",
                "TRANS_KEY": "trans-key",
                "TRANS_URL": "https://trans.example/v1",
            },
            clear=False,
        ), mock.patch.object(main_workflow_local, "AI_PROVIDER", "azure"), mock.patch.object(
            main_workflow_local,
            "_target_for_source_pr",
            return_value={"target_pr_url": "https://github.com/acme/docs/pull/0", "target_repo_path": "/tmp/target"},
        ), mock.patch.object(main_workflow, "main", return_value=0):
            self.assertEqual(main_workflow_local.main(), 0)
            self.assertEqual(main_workflow_local.os.environ["AZURE_OPENAI_KEY"], "azure-key")
            self.assertEqual(
                main_workflow_local.os.environ["OPENAI_BASE_URL"],
                "https://azure.example/v1",
            )

    def test_git_add_changes_can_be_disabled(self):
        with mock.patch.object(main_workflow, "SKIP_GIT_ADD", True), mock.patch.object(
            main_workflow.subprocess, "run"
        ) as subprocess_run:
            main_workflow.git_add_changes("/tmp/target")

        subprocess_run.assert_not_called()

    def test_main_continues_when_pr_has_only_image_changes(self):
        repo_config = {
            "source_repo": "acme/docs",
            "target_repo": "acme/docs-cn",
            "target_local_path": "/tmp/target",
            "prefer_local_target_for_read": False,
            "source_language": "English",
            "target_language": "Chinese",
        }
        fake_github_client = object()

        with mock.patch.object(main_workflow, "SOURCE_PR_URL", "https://github.com/acme/docs/pull/1"), mock.patch.object(
            main_workflow, "TARGET_PR_URL", "https://github.com/acme/docs-cn/pull/2"
        ), mock.patch.object(main_workflow, "GITHUB_TOKEN", "token"), mock.patch.object(
            main_workflow, "TARGET_REPO_PATH", "/tmp/target"
        ), mock.patch.object(main_workflow, "clean_temp_output_dir"), mock.patch.object(
            main_workflow, "get_workflow_repo_configs", return_value={"acme/docs": repo_config}
        ), mock.patch.object(
            main_workflow, "get_workflow_repo_config", return_value=repo_config
        ), mock.patch.object(
            main_workflow.Auth, "Token", return_value="token-auth"
        ), mock.patch.object(
            main_workflow, "Github", return_value=fake_github_client
        ), mock.patch.object(
            main_workflow, "UnifiedAIClient", return_value=SimpleNamespace(model="fake-model")
        ), mock.patch.object(
            main_workflow, "load_glossary", return_value=[]
        ), mock.patch.object(
            main_workflow, "create_glossary_matcher", return_value=None
        ), mock.patch.object(
            main_workflow, "get_pr_diff", return_value=""
        ), mock.patch.object(
            main_workflow,
            "analyze_source_changes",
            return_value=({}, {}, {}, {}, [], {}, {}, ["docs/media/example.png"], [], []),
        ), mock.patch.object(
            main_workflow, "process_all_images"
        ) as process_all_images, mock.patch.object(
            main_workflow, "git_add_changes"
        ):
            main_workflow.main()

        process_all_images.assert_called_once_with(
            ["docs/media/example.png"],
            [],
            [],
            "https://github.com/acme/docs/pull/1",
            fake_github_client,
            repo_config,
        )


class MainWorkflowRegressionTest(unittest.TestCase):
    def test_unmatched_modified_sections_are_formatted_for_failure_report(self):
        source_diff_dict = {
            "modified_1102": {
                "operation": "modified",
                "new_line_number": 1102,
                "new_content": "### tidb_analyze_column_options\n\nNew content\n",
            },
            "added_1200": {
                "operation": "added",
                "new_line_number": 1200,
                "new_content": "### new_section\n",
            },
        }

        missing = main_workflow.get_unmatched_modified_source_sections(
            source_diff_dict,
            {"added_1200": {}},
        )
        reason = main_workflow.format_unmatched_modified_sections_failure(
            "system-variables.md",
            missing,
        )

        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0]["key"], "modified_1102")
        self.assertEqual(missing[0]["title"], "tidb_analyze_column_options")
        self.assertIn("system-variables.md", reason)
        self.assertIn("modified_1102 (source line 1102): tidb_analyze_column_options", reason)
        self.assertIn("translation was skipped", reason)

    def test_regular_modified_file_skips_translation_when_modified_section_unmatched(self):
        source_diff_dict = {
            "modified_10": {
                "operation": "modified",
                "new_line_number": 10,
                "new_content": "### missing_source_section\n\nNew content\n",
                "old_content": "### missing_source_section\n\nOld content\n",
                "original_hierarchy": "## Variable reference > ### missing_source_section",
            },
            "modified_20": {
                "operation": "modified",
                "new_line_number": 20,
                "new_content": "### matched_source_section\n\nNew content\n",
                "old_content": "### matched_source_section\n\nOld content\n",
                "original_hierarchy": "## Variable reference > ### matched_source_section",
            },
        }
        matched_sections = {
            "modified_20": {
                "target_line": "20",
                "target_hierarchy": "## matched_source_section",
                "target_content": "### matched_source_section\n\n旧内容\n",
                "source_operation": "modified",
                "source_old_content": "### matched_source_section\n\nOld content\n",
                "source_new_content": "### matched_source_section\n\nNew content\n",
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            temp_dir = Path(tmpdir)
            source_diff_file = temp_dir / "system-variables-source-diff-dict.json"
            source_diff_file.write_text(
                json.dumps(source_diff_dict),
                encoding="utf-8",
            )

            with mock.patch.object(main_workflow, "ensure_temp_output_dir", return_value=tmpdir), \
                mock.patch.object(main_workflow, "check_source_token_limit", return_value=(True, 10, 50000)), \
                mock.patch("diff_analyzer.get_target_hierarchy_and_content", return_value=({"20": "## matched_source_section"}, ["### matched_source_section"])), \
                mock.patch("section_matcher.match_source_diff_to_target", return_value=matched_sections), \
                mock.patch.object(main_workflow, "process_modified_sections") as process_modified_sections:
                success, reason = main_workflow.process_regular_modified_file(
                    "system-variables.md",
                    {"sections": {"10": "## missing_source_section", "20": "## matched_source_section"}},
                    "File: system-variables.md\n@@ -10,1 +10,1 @@\n-old\n+new",
                    {"mode": "commit"},
                    object(),
                    object(),
                    {
                        "target_repo": "pingcap/docs",
                        "target_local_path": tmpdir,
                        "prefer_local_target_for_read": True,
                        "source_language": "English",
                        "target_language": "Chinese",
                    },
                    120,
                    return_details=True,
                )

        self.assertFalse(success)
        self.assertIn("modified_10", reason)
        self.assertIn("missing_source_section", reason)
        process_modified_sections.assert_not_called()


if __name__ == "__main__":
    unittest.main()
