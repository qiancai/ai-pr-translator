import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from resolve_cloud_source_files import (
    build_allowed_files,
    extract_markdown_doc_links,
    parse_git_name_status,
    resolve_source_files,
)


class ResolveCloudSourceFilesTest(unittest.TestCase):
    def test_build_allowed_files_keeps_links_with_anchors(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "TOC-tidb-cloud.md").write_text(
                "- [Billing](/tidb-cloud/tidb-cloud-billing.md#invoices)\n",
                encoding="utf-8",
            )

            allowed = build_allowed_files(root, ["TOC-tidb-cloud.md"])

        self.assertIn("TOC-tidb-cloud.md", allowed)
        self.assertIn("tidb-cloud/tidb-cloud-billing.md", allowed)

    def test_extract_markdown_doc_links_uses_markdown_parser_edge_cases(self):
        links = extract_markdown_doc_links(
            """
- [![img](x.png)](/tidb-cloud/nested.md)
- [Reference][ref]

```md
[Fake](/tidb-cloud/fake.md)
```

[ref]: /tidb-cloud/reference.md#anchor
"""
        )

        self.assertEqual(
            links,
            [
                "tidb-cloud/nested.md",
                "tidb-cloud/reference.md",
            ],
        )

    def test_manual_file_names_must_be_in_cloud_scope(self):
        allowed = {"TOC-tidb-cloud.md", "tidb-cloud/in-scope.md"}

        with self.assertRaises(ValueError):
            resolve_source_files(allowed, input_file_names="not-cloud.md")

    def test_manual_basename_resolves_when_unique(self):
        allowed = {"TOC-tidb-cloud.md", "tidb-cloud/in-scope.md"}

        resolved = resolve_source_files(allowed, input_file_names="in-scope.md")

        self.assertEqual(resolved, ["tidb-cloud/in-scope.md"])

    def test_auto_mode_intersects_changed_files_with_allowed_files(self):
        allowed = {"TOC-tidb-cloud.md", "tidb-cloud/in-scope.md"}
        changed_rows = [
            {"filename": "tidb-cloud/in-scope.md", "previous_filename": ""},
            {"filename": "other.md", "previous_filename": ""},
        ]

        resolved = resolve_source_files(allowed, changed_rows=changed_rows)

        self.assertEqual(resolved, ["tidb-cloud/in-scope.md"])

    def test_renamed_file_can_match_previous_allowed_path(self):
        allowed = {"tidb-cloud/old.md"}
        changed_rows = [
            {"filename": "tidb-cloud/new-location.md", "previous_filename": "tidb-cloud/old.md"},
        ]

        resolved = resolve_source_files(allowed, changed_rows=changed_rows)

        self.assertEqual(resolved, ["tidb-cloud/old.md"])

    def test_parse_git_name_status_handles_renames(self):
        rows = parse_git_name_status("R100\ttidb-cloud/old.md\ttidb-cloud/new.md\n")

        self.assertEqual(
            rows,
            [{"filename": "tidb-cloud/new.md", "previous_filename": "tidb-cloud/old.md"}],
        )


if __name__ == "__main__":
    unittest.main()
