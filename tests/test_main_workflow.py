import sys
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


if __name__ == "__main__":
    unittest.main()
