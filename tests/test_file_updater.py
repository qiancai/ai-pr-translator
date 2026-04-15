import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from file_updater import update_target_document_from_match_data


class FileUpdaterRegressionTest(unittest.TestCase):
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
