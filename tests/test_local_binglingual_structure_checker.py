import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

SPEC = importlib.util.spec_from_file_location(
    "local_binglingual_structure_checker",
    SCRIPTS_DIR / "local-binglingual-structure-checker.py",
)
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)


class LocalBinglingualStructureCheckerTest(unittest.TestCase):
    def test_check_file_reports_heading_and_custom_content_issues(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            en_root = root / "en"
            zh_root = root / "zh"
            en_root.mkdir()
            zh_root.mkdir()

            rel_path = "guide.md"
            (en_root / rel_path).write_text(
                '<CustomContent plan="essential">\n\n# Guide\n\n## Section\n\n</CustomContent>\n',
                encoding="utf-8",
            )
            (zh_root / rel_path).write_text(
                "# 指南\n\n### Section\n",
                encoding="utf-8",
            )

            issues = checker.check_file(en_root, zh_root, rel_path)

        self.assertEqual(
            ["heading level sequence differs", "CustomContent tag sequence differs"],
            [issue.reason for issue in issues],
        )

    def test_check_file_reports_missing_target(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            en_root = root / "en"
            zh_root = root / "zh"
            en_root.mkdir()
            zh_root.mkdir()
            (en_root / "guide.md").write_text("# Guide\n", encoding="utf-8")

            issues = checker.check_file(en_root, zh_root, "guide.md")

        self.assertEqual(["target file missing"], [issue.reason for issue in issues])


if __name__ == "__main__":
    unittest.main()
