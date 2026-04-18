import sys
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from diff_analyzer import (
    analyze_diff_operations,
    analyze_source_changes,
    build_commit_diff_context,
    build_local_commit_diff_context,
    build_diff_text,
    build_pr_diff_context,
    build_hierarchy_dict,
    build_source_diff_dict,
    get_target_file_content,
    get_target_hierarchy_and_content,
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

    def test_numbered_heading_rename_is_treated_as_modified(self):
        patch = "\n".join(
            [
                "@@ -12,9 +12,9 @@",
                " Intro text",
                "-## Step 1: Create a TiDB cluster",
                "+## Step 1: Create a {{{ .starter }}} instance {#step-1-create-a-starter-instance}",
                " ",
                "-Old body",
                "+New body",
                " ## Step 2: Try AI-assisted SQL Editor",
            ]
        )
        changed_file = SimpleNamespace(filename="quickstart.md", status="modified", patch=patch)

        operations = analyze_diff_operations(changed_file)

        self.assertEqual([], [line["content"] for line in operations["added_lines"] if line["is_header"]])
        self.assertEqual([], [line["content"] for line in operations["deleted_lines"] if line["is_header"]])
        self.assertEqual(
            ["## Step 1: Create a {{{ .starter }}} instance {#step-1-create-a-starter-instance}"],
            [line["content"] for line in operations["modified_lines"] if line["is_header"]],
        )
        self.assertEqual(
            "## Step 1: Create a TiDB cluster",
            next(line for line in operations["modified_lines"] if line["is_header"])["original_content"],
        )

    def test_keyword_overlapping_heading_rename_is_treated_as_modified(self):
        patch = "\n".join(
            [
                "@@ -144,8 +198,8 @@",
                " Some context",
                "-### Invite an organization member",
                "+### Invite a user to your organization",
                " ",
                "-Old body",
                "+New body",
                " ### Remove an organization member",
            ]
        )
        changed_file = SimpleNamespace(filename="manage-user-access.md", status="modified", patch=patch)

        operations = analyze_diff_operations(changed_file)

        self.assertEqual([], [line["content"] for line in operations["added_lines"] if line["is_header"]])
        self.assertEqual([], [line["content"] for line in operations["deleted_lines"] if line["is_header"]])
        self.assertEqual(
            ["### Invite a user to your organization"],
            [line["content"] for line in operations["modified_lines"] if line["is_header"]],
        )
        self.assertEqual(
            "### Invite an organization member",
            next(line for line in operations["modified_lines"] if line["is_header"])["original_content"],
        )

    def test_short_heading_rename_with_shared_leading_keyword_is_modified(self):
        patch = "\n".join(
            [
                "@@ -43,9 +43,9 @@",
                " ### External storage",
                " ",
                "-### Downstream cluster",
                "+### Downstream TiDB Cloud",
                " ",
                " The sharded schemas and tables are merged into the table `store.sales`.",
            ]
        )
        changed_file = SimpleNamespace(filename="migrate-sql-shards.md", status="modified", patch=patch)

        operations = analyze_diff_operations(changed_file)

        self.assertEqual([], [line["content"] for line in operations["added_lines"] if line["is_header"]])
        self.assertEqual([], [line["content"] for line in operations["deleted_lines"] if line["is_header"]])
        self.assertEqual(
            ["### Downstream TiDB Cloud"],
            [line["content"] for line in operations["modified_lines"] if line["is_header"]],
        )
        self.assertEqual(
            "### Downstream cluster",
            next(line for line in operations["modified_lines"] if line["is_header"])["original_content"],
        )

    def test_adjacent_same_level_heading_rename_without_keyword_overlap_is_modified(self):
        patch = "\n".join(
            [
                "@@ -240,8 +240,8 @@",
                " ## Incremental replication",
                " ",
                "-### Prerequisites",
                "+### Before you begin",
                " ",
                "-Old body",
                "+New body",
            ]
        )
        changed_file = SimpleNamespace(filename="guide.md", status="modified", patch=patch)

        operations = analyze_diff_operations(changed_file)

        self.assertEqual([], [line["content"] for line in operations["added_lines"] if line["is_header"]])
        self.assertEqual([], [line["content"] for line in operations["deleted_lines"] if line["is_header"]])
        self.assertEqual(
            ["### Before you begin"],
            [line["content"] for line in operations["modified_lines"] if line["is_header"]],
        )
        self.assertEqual(
            "### Prerequisites",
            next(line for line in operations["modified_lines"] if line["is_header"])["original_content"],
        )

    def test_adjacent_delete_add_heading_blocks_are_not_paired_as_renames(self):
        patch = "\n".join(
            [
                "@@ -20,8 +20,8 @@",
                " ## Parent",
                "-### Old one",
                "-### Old two",
                "+### New three",
                "+### New four",
                " ## Next",
            ]
        )
        changed_file = SimpleNamespace(filename="guide.md", status="modified", patch=patch)

        operations = analyze_diff_operations(changed_file)

        self.assertEqual(
            ["### New three", "### New four"],
            [line["content"] for line in operations["added_lines"] if line["is_header"]],
        )
        self.assertEqual(
            ["### Old one", "### Old two"],
            [line["content"] for line in operations["deleted_lines"] if line["is_header"]],
        )
        self.assertEqual([], [line["content"] for line in operations["modified_lines"] if line["is_header"]])

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

    def test_target_hierarchy_prefers_local_target_checkout(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target_file = Path(tmpdir) / "guide.md"
            target_file.write_text("# Local title\n\n## Local section\n", encoding="utf-8")

            repository = FakeRepository(
                {("guide.md", "i18n-zh-release-8.5"): "# Remote title\n"},
                FakePR([], "Empty", "base123", "head123"),
                {},
            )
            github = FakeGithub({"acme/docs-cn": repository})

            hierarchy, lines = get_target_hierarchy_and_content(
                "guide.md",
                github,
                "acme/docs-cn",
                target_local_path=tmpdir,
                prefer_local_target_for_read=True,
                target_ref="i18n-zh-release-8.5",
            )

        self.assertIn(1, hierarchy)
        self.assertEqual(hierarchy[1], "# Local title")
        self.assertEqual(lines[0], "# Local title")

    def test_target_hierarchy_uses_target_ref_before_default_branch(self):
        repository = FakeRepository(
            {
                ("guide.md", "master"): "# Master title\n",
                ("guide.md", "i18n-zh-release-8.5"): "# Target title\n",
            },
            FakePR([], "Empty", "base123", "head123"),
            {},
        )
        github = FakeGithub({"acme/docs-cn": repository})

        hierarchy, lines = get_target_hierarchy_and_content(
            "guide.md",
            github,
            "acme/docs-cn",
            target_ref="i18n-zh-release-8.5",
        )

        self.assertEqual(hierarchy[1], "# Target title")
        self.assertEqual(lines[0], "# Target title")

    def test_target_hierarchy_does_not_fallback_to_default_branch_with_explicit_target_ref(self):
        repository = FakeRepository(
            {
                ("guide.md", "master"): "# Master title\n",
            },
            FakePR([], "Empty", "base123", "head123"),
            {},
        )
        github = FakeGithub({"acme/docs-cn": repository})

        hierarchy, lines = get_target_hierarchy_and_content(
            "guide.md",
            github,
            "acme/docs-cn",
            target_ref="i18n-zh-release-8.5",
        )

        self.assertEqual(hierarchy, {})
        self.assertEqual(lines, [])

    def test_target_file_content_uses_default_branch_without_explicit_target_ref(self):
        repository = FakeRepository(
            {
                ("guide.md", "master"): "# Master title\n",
            },
            FakePR([], "Empty", "base123", "head123"),
            {},
        )
        github = FakeGithub({"acme/docs-cn": repository})

        content, source = get_target_file_content("guide.md", github, "acme/docs-cn")

        self.assertEqual(content, "# Master title\n")
        self.assertEqual(source, "remote:acme/docs-cn@master")

    def test_keyword_tabs_fallbacks_to_remote_when_local_blocks_are_empty(self):
        patch = "\n".join(
            [
                "@@ -5,1 +5,1 @@",
                "- - apple",
                "+ - apple updated",
            ]
        )
        changed_file = SimpleNamespace(
            filename="keywords.md",
            status="modified",
            patch=patch,
            previous_filename=None,
        )
        base_content = "# Keywords\n\n<TabsPanel>\n<a id=\"A\" class=\"letter\" href=\"#A\">A</a>\n- apple\n"
        head_content = "# Keywords\n\n<TabsPanel>\n<a id=\"A\" class=\"letter\" href=\"#A\">A</a>\n- apple updated\n"
        target_content = "# 关键字\n\n<TabsPanel>\n<a id=\"A\" class=\"letter\" href=\"#A\">A</a>\n- 苹果\n"

        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "keywords.md").write_text("# 关键字\n\nNo tabs yet\n", encoding="utf-8")
            repo_configs = {
                "acme/docs": {
                    "target_repo": "acme/docs-cn",
                    "target_local_path": tmpdir,
                    "prefer_local_target_for_read": True,
                    "target_ref": "i18n",
                    "source_language": "English",
                    "target_language": "Chinese",
                }
            }
            source_repo = FakeRepository(
                {
                    ("keywords.md", "base123"): base_content,
                    ("keywords.md", "head123"): head_content,
                },
                FakePR([changed_file], "Update keywords", "base123", "head123"),
                {("base123", "head123"): [changed_file]},
            )
            target_repo = FakeRepository(
                {
                    ("keywords.md", "i18n"): target_content,
                },
                FakePR([], "Empty", "base123", "head123"),
                {},
            )
            github = FakeGithub({"acme/docs": source_repo, "acme/docs-cn": target_repo})
            commit_context = build_commit_diff_context(
                "acme/docs",
                "acme/docs-cn",
                "base123",
                "head123",
                github,
                repo_configs,
            )

            result = analyze_source_changes(
                commit_context,
                github,
                special_files=["keywords.md"],
                ignore_files=[],
                repo_configs=repo_configs,
            )

        keyword_files = result[6]
        self.assertIn("keywords.md", keyword_files)
        self.assertIn("- 苹果", keyword_files["keywords.md"]["tabs_changes"]["A"]["target_old_block"])

    def test_local_commit_context_reads_all_changed_files_from_git_diff(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            subprocess.run(["git", "init"], cwd=repo, check=True, stdout=subprocess.DEVNULL)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=repo, check=True)
            subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True)

            (repo / "guide.md").write_text("# Guide\n\nOld\n", encoding="utf-8")
            (repo / "old-name.md").write_text("# Old\n", encoding="utf-8")
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-m", "base"], cwd=repo, check=True, stdout=subprocess.DEVNULL)
            base_ref = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()

            (repo / "guide.md").write_text("# Guide\n\nNew\n", encoding="utf-8")
            (repo / "new-name.md").write_text((repo / "old-name.md").read_text(encoding="utf-8"), encoding="utf-8")
            (repo / "old-name.md").unlink()
            subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
            subprocess.run(["git", "commit", "-m", "head"], cwd=repo, check=True, stdout=subprocess.DEVNULL)
            head_ref = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()

            context = build_local_commit_diff_context(
                "acme/docs",
                "acme/docs-cn",
                base_ref,
                head_ref,
                str(repo),
                self.repo_configs,
            )

        files = {file.filename: file for file in context["changed_files"]}
        self.assertEqual(files["guide.md"].status, "modified")
        self.assertIn("+New", files["guide.md"].patch)
        self.assertEqual(files["new-name.md"].status, "renamed")
        self.assertEqual(files["new-name.md"].previous_filename, "old-name.md")


if __name__ == "__main__":
    unittest.main()
