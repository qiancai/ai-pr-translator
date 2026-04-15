import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from file_updater import (
    build_heading_anchor_slug,
    preprocess_diff_for_heading_anchor_stability,
    update_target_document_from_match_data,
)


class FileUpdaterRegressionTest(unittest.TestCase):
    def test_build_heading_anchor_slug_keeps_visible_text_inside_span(self):
        heading = '`txn-entry-size-limit` <span class="version-mark">New in v4.0.10 and v5.0.0</span>'
        slug = build_heading_anchor_slug(heading)
        self.assertEqual(slug, "txn-entry-size-limit-new-in-v4010-and-v500")

    def test_preprocess_diff_adds_anchor_to_changed_non_top_level_heading(self):
        pr_diff = "\n".join(
            [
                "File: ai/example.md",
                "@@ -10,1 +10,1 @@",
                "-## Example tests",
                "+## Example test",
                "-" * 80,
            ]
        )

        processed = preprocess_diff_for_heading_anchor_stability(
            pr_diff,
            source_language="English",
            target_language="Chinese",
            source_mode="commit",
        )

        self.assertIn("+## Example test {#example-test}", processed)
        self.assertNotIn("-## Example tests {#example-tests}", processed)

    def test_preprocess_diff_keeps_existing_explicit_anchor(self):
        pr_diff = "\n".join(
            [
                "File: ai/example.md",
                "@@ -10,1 +10,1 @@",
                "+## {{{ .starter }}} {#starter}",
                "-" * 80,
            ]
        )

        processed = preprocess_diff_for_heading_anchor_stability(
            pr_diff,
            source_language="English",
            target_language="Chinese",
            source_mode="commit",
        )

        self.assertIn("+## {{{ .starter }}} {#starter}", processed)
        self.assertEqual(processed.count("{#starter}"), 1)

    def test_preprocess_diff_is_disabled_for_pr_mode(self):
        pr_diff = "\n".join(
            [
                "File: ai/example.md",
                "@@ -10,1 +10,1 @@",
                "+## Example test",
                "-" * 80,
            ]
        )

        processed = preprocess_diff_for_heading_anchor_stability(
            pr_diff,
            source_language="English",
            target_language="Chinese",
            source_mode="pr",
        )

        self.assertEqual(processed, pr_diff)

    def test_preprocess_diff_does_not_add_anchor_for_heading_level_only_change(self):
        pr_diff = "\n".join(
            [
                "File: ai/example.md",
                "@@ -10,1 +10,1 @@",
                "-## Example test",
                "+### Example test",
                "-" * 80,
            ]
        )

        processed = preprocess_diff_for_heading_anchor_stability(
            pr_diff,
            source_language="English",
            target_language="Chinese",
            source_mode="commit",
        )

        self.assertEqual(processed, pr_diff)

    def test_preprocess_diff_adds_zh_prefix_to_added_aliases(self):
        pr_diff = "\n".join(
            [
                "File: ai/example.md",
                "@@ -1,1 +1,1 @@",
                "+aliases: ['/tidb/stable/saas-best-practices/','/zh/tidb/dev/saas-best-practices/']",
                "-" * 80,
            ]
        )

        processed = preprocess_diff_for_heading_anchor_stability(
            pr_diff,
            source_language="English",
            target_language="Chinese",
            source_mode="commit",
        )

        self.assertIn(
            "+aliases: ['/zh/tidb/stable/saas-best-practices/','/zh/tidb/dev/saas-best-practices/']",
            processed,
        )

    def test_insert_preserves_unmodified_line_endings(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            target_file = tmp_path / "system-variables.md"
            match_file = tmp_path / "system-variables-match_source_diff_to_target.json"

            with open(target_file, "w", encoding="utf-8", newline="") as f:
                f.write("# System Variables\n\n")
                f.write("## Variable reference\n\n")
                f.write("### tidb_enable_tso_follower_proxy <span class=\"version-mark\">New in v5.3.0</span>\n\n")
                f.write("- Scope: GLOBAL\r\n")
                f.write("- Persists to cluster: Yes\r\n")
                f.write("- Type: Boolean\n")

            match_file.write_text(
                json.dumps(
                    {
                        "added_2490": {
                            "source_operation": "added",
                            "insertion_type": "before_reference",
                            "target_line": "5",
                            "target_hierarchy": "## Variable reference > ### tidb_enable_tso_follower_proxy <span class=\"version-mark\">New in v5.3.0</span>",
                            "target_new_content": "### `tidb_enable_ts_validation` <span class=\"version-mark\">New in v9.0.0</span>\n\n- Scope: GLOBAL\n- Persists to cluster: Yes\n",
                        }
                    }
                ),
                encoding="utf-8",
            )

            success = update_target_document_from_match_data(
                str(match_file), str(tmp_path), "system-variables.md"
            )

            self.assertTrue(success)

            with open(target_file, "r", encoding="utf-8", newline="") as f:
                updated_content = f.read()

            self.assertIn("### `tidb_enable_ts_validation`", updated_content)
            self.assertIn(
                "- Scope: GLOBAL\r\n- Persists to cluster: Yes\r\n- Type: Boolean\n",
                updated_content,
            )
            self.assertEqual(updated_content.count("\r\n"), 2)


if __name__ == "__main__":
    unittest.main()
