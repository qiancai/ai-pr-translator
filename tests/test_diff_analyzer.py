import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from diff_analyzer import (
    analyze_source_changes,
    build_commit_diff_context,
    build_diff_text,
    build_pr_diff_context,
    build_hierarchy_dict,
    build_source_diff_dict,
)


class FakeContent:
    def __init__(self, text):
        self.decoded_content = text.encode("utf-8")


class FakePR:
    def __init__(self, files, title, base_sha, head_sha):
        self._files = files
        self.title = title
        self.base = SimpleNamespace(sha=base_sha, ref="master")
        self.head = SimpleNamespace(sha=head_sha, ref="feature/test")

    def get_files(self):
        return self._files


class FakeComparison:
    def __init__(self, files):
        self.files = files


class FakeRepository:
    def __init__(self, source_files, pr, comparison_files):
        self.default_branch = "master"
        self._source_files = source_files
        self._pr = pr
        self._comparison_files = comparison_files

    def get_pull(self, number):
        return self._pr

    def compare(self, base, head):
        return FakeComparison(self._comparison_files[(base, head)])

    def get_contents(self, path, ref):
        return FakeContent(self._source_files[(path, ref)])


class FakeGithub:
    def __init__(self, repositories):
        self._repositories = repositories

    def get_repo(self, repo_name):
        return self._repositories[repo_name]


class DiffAnalyzerContextTest(unittest.TestCase):
    def setUp(self):
        self.temp_output_dir = SCRIPTS_DIR / "temp_output"
        self.generated_file = self.temp_output_dir / "guide-source-diff-dict.json"
        if self.generated_file.exists():
            self.generated_file.unlink()

        patch = "\n".join(
            [
                "@@ -1,4 +1,4 @@",
                " # Title",
                " ",
                " ## Section",
                "-Old text",
                "+New text",
            ]
        )
        self.changed_file = SimpleNamespace(
            filename="guide.md",
            status="modified",
            patch=patch,
            previous_filename=None,
        )
        base_sha = "base123"
        head_sha = "head123"
        base_content = "# Title\n\n## Section\nOld text\n"
        head_content = "# Title\n\n## Section\nNew text\n"
        self.repo_configs = {
            "acme/docs": {
                "target_repo": "acme/docs-cn",
                "target_local_path": "/tmp/target",
                "prefer_local_target_for_read": False,
                "source_language": "English",
                "target_language": "Chinese",
            }
        }
        repository = FakeRepository(
            {
                ("guide.md", base_sha): base_content,
                ("guide.md", head_sha): head_content,
            },
            FakePR([self.changed_file], "Update guide", base_sha, head_sha),
            {(base_sha, head_sha): [self.changed_file]},
        )
        self.github = FakeGithub({"acme/docs": repository})
        self.pr_url = "https://github.com/acme/docs/pull/123"

    def tearDown(self):
        if self.generated_file.exists():
            self.generated_file.unlink()

    def test_pr_and_commit_contexts_produce_same_analysis(self):
        pr_context = build_pr_diff_context(self.pr_url, self.github, self.repo_configs)
        commit_context = build_commit_diff_context(
            "acme/docs",
            "acme/docs-cn",
            "base123",
            "head123",
            self.github,
            self.repo_configs,
        )

        self.assertEqual(
            build_diff_text(pr_context["changed_files"]),
            build_diff_text(commit_context["changed_files"]),
        )

        pr_result = analyze_source_changes(
            pr_context,
            self.github,
            special_files=["TOC.md", "keywords.md"],
            ignore_files=[],
            repo_configs=self.repo_configs,
        )
        commit_result = analyze_source_changes(
            commit_context,
            self.github,
            special_files=["TOC.md", "keywords.md"],
            ignore_files=[],
            repo_configs=self.repo_configs,
        )

        self.assertEqual(pr_result, commit_result)
        self.assertTrue(self.generated_file.exists())
        source_diff = self.generated_file.read_text(encoding="utf-8")
        self.assertIn('"operation": "modified"', source_diff)
        self.assertIn("New text", source_diff)

    def test_bottom_modified_keeps_matching_hierarchy_in_source_diff_dict(self):
        base_content = "# Title\n\n## Section\nOld text\n"
        head_content = "# Title\n\n## Section\nNew text\n"
        base_hierarchy = build_hierarchy_dict(base_content)
        head_hierarchy = build_hierarchy_dict(head_content)

        source_diff_dict = build_source_diff_dict(
            modified_sections={"3": "## Section"},
            added_sections={},
            deleted_sections={},
            all_hierarchy_dict=head_hierarchy,
            base_hierarchy_dict=base_hierarchy,
            operations={"modified_lines": [{"line_number": 4, "is_header": False}]},
            file_content=head_content,
            base_file_content=base_content,
        )

        self.assertEqual(source_diff_dict["modified_3"]["original_hierarchy"], "bottom-modified-3")
        self.assertEqual(source_diff_dict["modified_3"]["matching_hierarchy"], "## Section")

    def test_renamed_markdown_is_treated_as_delete_and_add(self):
        renamed_file = SimpleNamespace(
            filename="new-guide.md",
            status="renamed",
            patch="",
            previous_filename="guide.md",
        )
        head_sha = "head123"
        repository = FakeRepository(
            {
                ("new-guide.md", head_sha): "# Title\n\n## Section\nRenamed text\n",
            },
            FakePR([renamed_file], "Rename guide", "base123", head_sha),
            {("base123", head_sha): [renamed_file]},
        )
        github = FakeGithub({"acme/docs": repository})
        commit_context = build_commit_diff_context(
            "acme/docs",
            "acme/docs-cn",
            "base123",
            head_sha,
            github,
            self.repo_configs,
        )

        (
            added_sections,
            modified_sections,
            deleted_sections,
            added_files,
            deleted_files,
            toc_files,
            keyword_files,
            added_images,
            modified_images,
            deleted_images,
        ) = analyze_source_changes(
            commit_context,
            github,
            special_files=["TOC.md", "keywords.md"],
            ignore_files=[],
            repo_configs=self.repo_configs,
        )

        self.assertEqual(added_sections, {})
        self.assertEqual(modified_sections, {})
        self.assertEqual(deleted_sections, {})
        self.assertEqual(added_files, {"new-guide.md": "# Title\n\n## Section\nRenamed text\n"})
        self.assertEqual(deleted_files, ["guide.md"])
        self.assertEqual(toc_files, {})
        self.assertEqual(keyword_files, {})
        self.assertEqual(added_images, [])
        self.assertEqual(modified_images, [])
        self.assertEqual(deleted_images, [])


if __name__ == "__main__":
    unittest.main()
