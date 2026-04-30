import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from file_adder import preprocess_added_file_batch_for_heading_anchor_stability, process_added_files


class FileAdderRegressionTest(unittest.TestCase):
    def test_preprocess_added_file_batch_adds_anchors_for_non_top_level_headings(self):
        batch = "\n".join(
            [
                "# Title",
                "",
                "## Example test",
                "",
                "### txn-entry-size-limit <span class=\"version-mark\">New in v4.0.10 and v5.0.0</span>",
            ]
        )

        processed = preprocess_added_file_batch_for_heading_anchor_stability(
            batch,
            source_language="English",
            target_language="Chinese",
            source_mode="commit",
        )

        self.assertIn("## Example test {#example-test}", processed)
        self.assertIn(
            "### txn-entry-size-limit <span class=\"version-mark\">New in v4.0.10 and v5.0.0</span> {#txn-entry-size-limit-new-in-v4010-and-v500}",
            processed,
        )
        self.assertIn("# Title", processed)

    def test_preprocess_added_file_batch_skips_pr_mode(self):
        batch = "## Example test"

        processed = preprocess_added_file_batch_for_heading_anchor_stability(
            batch,
            source_language="English",
            target_language="Chinese",
            source_mode="pr",
        )

        self.assertEqual(processed, batch)

    def test_preprocess_added_file_batch_adds_zh_prefix_to_aliases(self):
        batch = "\n".join(
            [
                "---",
                "aliases: ['/tidb/stable/saas-best-practices/','/zh/tidb/dev/saas-best-practices/']",
                "---",
            ]
        )

        processed = preprocess_added_file_batch_for_heading_anchor_stability(
            batch,
            source_language="English",
            target_language="Chinese",
            source_mode="commit",
        )

        self.assertIn(
            "aliases: ['/zh/tidb/stable/saas-best-practices/','/zh/tidb/dev/saas-best-practices/']",
            processed,
        )

    def test_preprocess_added_file_batch_rewrites_tidb_cloud_links_in_pr_mode(self):
        batch = "See [Private Endpoints](/tidb-cloud/test/set-up-private-endpoint-connections-serverless2.md)."

        processed = preprocess_added_file_batch_for_heading_anchor_stability(
            batch,
            source_language="English",
            target_language="Chinese",
            source_mode="pr",
        )

        self.assertEqual(
            processed,
            "See [Private Endpoints](https://docs.pingcap.com/tidbcloud/set-up-private-endpoint-connections-serverless2).",
        )

    def test_preprocess_added_file_batch_rewrites_tidb_cloud_links_only_for_ai_commit_scope(self):
        batch = "See [Private Endpoints](/tidb-cloud/test/set-up-private-endpoint-connections-serverless2.md)."

        with mock.patch.dict(os.environ, {"SOURCE_FOLDER": "docs"}, clear=False):
            processed = preprocess_added_file_batch_for_heading_anchor_stability(
                batch,
                source_language="English",
                target_language="Chinese",
                source_mode="commit",
            )

        self.assertEqual(processed, batch)

    def test_preprocess_added_file_batch_rewrites_tidb_cloud_links_for_ai_commit_scope(self):
        batch = "See [Private Endpoints](/tidb-cloud/test/set-up-private-endpoint-connections-serverless2.md)."

        with mock.patch.dict(os.environ, {"SOURCE_FOLDER": "ai"}, clear=False):
            processed = preprocess_added_file_batch_for_heading_anchor_stability(
                batch,
                source_language="English",
                target_language="Chinese",
                source_mode="commit",
            )

        self.assertEqual(
            processed,
            "See [Private Endpoints](https://docs.pingcap.com/tidbcloud/set-up-private-endpoint-connections-serverless2).",
        )

    def test_process_added_files_can_overwrite_existing_target_file(self):
        repo_config = {
            "target_local_path": "",
            "source_language": "English",
            "target_language": "Chinese",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_config["target_local_path"] = tmpdir
            target_file = Path(tmpdir, "guide.md")
            target_file.write_text("old target", encoding="utf-8")

            with mock.patch("file_adder.translate_file_batch", return_value="translated target"):
                success, failures = process_added_files(
                    {"guide.md": "# Guide\n\nSource content"},
                    {"mode": "commit"},
                    object(),
                    object(),
                    repo_config,
                    return_details=True,
                    overwrite_existing=True,
                )

            self.assertTrue(success)
            self.assertEqual(failures, {})
            self.assertEqual(target_file.read_text(encoding="utf-8"), "translated target")

    def test_process_added_files_still_rejects_existing_target_file_by_default(self):
        repo_config = {
            "target_local_path": "",
            "source_language": "English",
            "target_language": "Chinese",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_config["target_local_path"] = tmpdir
            target_file = Path(tmpdir, "guide.md")
            target_file.write_text("old target", encoding="utf-8")

            success, failures = process_added_files(
                {"guide.md": "# Guide\n\nSource content"},
                {"mode": "commit"},
                object(),
                object(),
                repo_config,
                return_details=True,
            )

            self.assertFalse(success)
            self.assertIn("guide.md", failures)
            self.assertEqual(target_file.read_text(encoding="utf-8"), "old target")


if __name__ == "__main__":
    unittest.main()
