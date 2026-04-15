import sys
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from file_adder import preprocess_added_file_batch_for_heading_anchor_stability


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


if __name__ == "__main__":
    unittest.main()
