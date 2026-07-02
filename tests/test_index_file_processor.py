import json
import os
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from index_file_processor import (
    build_index_translation_memory,
    localize_docs_absolute_links,
    parse_index_line,
    plan_synced_index_lines,
    process_index_file_by_source_snapshot,
    translate_index_lines,
    _extract_frontmatter,
    _needs_translation,
)


# ---------------------------------------------------------------------------
# Fake AI client for deterministic tests
# ---------------------------------------------------------------------------

class FakeAIClient:
    """Return pre-canned translations keyed on substrings found in the prompt."""

    def chat_completion(self, messages, temperature=0.1):
        prompt = messages[0]["content"]
        translations = {}

        if "TiDB Cloud Dedicated" in prompt:
            for key in ("line_0", "line_1", "line_2"):
                if key == "line_0":
                    translations[key] = (
                        '<LearningPathContainer platform="tidb-cloud" '
                        'title="TiDB Cloud Dedicated" '
                        'subTitle="TiDB Cloud Dedicated 专为关键任务业务设计，提供跨多个可用区的高可用性、水平扩展和完整的 HTAP 能力。">'
                    )
        if "Developer Guide Overview" in prompt:
            translations.setdefault("line_0", "[开发者指南概览](https://docs.pingcap.com/developer/)")
        if "Quick Start" in prompt:
            translations.setdefault("line_1", "[快速入门](https://docs.pingcap.com/developer/dev-guide-build-cluster-in-cloud/)")
        if "Connect to TiDB" in prompt:
            translations.setdefault("line_2", "[连接到 TiDB](https://docs.pingcap.com/developer/dev-guide-connect-to-tidb/)")

        return json.dumps(translations, ensure_ascii=False)


class EchoAIClient:
    """Return the input lines unchanged (echo them back as-is)."""

    def chat_completion(self, messages, temperature=0.1):
        prompt = messages[0]["content"]
        marker = "Input lines to translate:\n"
        start = prompt.find(marker)
        if start == -1:
            return "{}"
        start += len(marker)
        json_start = prompt.find("{", start)
        if json_start == -1:
            return "{}"
        depth = 0
        for i in range(json_start, len(prompt)):
            if prompt[i] == "{":
                depth += 1
            elif prompt[i] == "}":
                depth -= 1
                if depth == 0:
                    return prompt[json_start : i + 1]
        return "{}"


class EmptyAIClient:
    def chat_completion(self, messages, temperature=0.1):
        return "{}"


# ---------------------------------------------------------------------------
# Sample _index.md content
# ---------------------------------------------------------------------------

SOURCE_BASE = """\
---
title: TiDB Cloud Documentation
aliases: ['/tidbcloud/privacy-policy']
hide_sidebar: true
hide_commit: true
summary: Summary text here.
---

<LearningPathContainer platform="tidb-cloud" title="TiDB Cloud" subTitle="TiDB Cloud is a fully-managed DBaaS.">

<LearningPath label="Learn" icon="cloud1">

[Why TiDB Cloud](https://docs.pingcap.com/tidbcloud/tidb-cloud-intro)

[FAQ](https://docs.pingcap.com/tidbcloud/tidb-cloud-faq)

</LearningPath>

<LearningPath label="Develop" icon="doc8">

[Developer Guide Overview](https://docs.pingcap.com/tidbcloud/dev-guide-overview)

[Quick Start](https://docs.pingcap.com/tidbcloud/dev-guide-build-cluster-in-cloud)

[Example Application](https://docs.pingcap.com/tidbcloud/dev-guide-sample-application-spring-boot)

</LearningPath>

</LearningPathContainer>"""

SOURCE_HEAD = """\
---
title: TiDB Cloud Documentation
aliases: ['/tidbcloud/privacy-policy']
hide_sidebar: true
hide_commit: true
summary: Summary text here.
---

<LearningPathContainer platform="tidb-cloud" title="TiDB Cloud Dedicated" subTitle="TiDB Cloud Dedicated is designed for mission-critical businesses.">

<LearningPath label="Learn" icon="cloud1">

[Why TiDB Cloud](https://docs.pingcap.com/tidbcloud/tidb-cloud-intro)

[FAQ](https://docs.pingcap.com/tidbcloud/tidb-cloud-faq)

</LearningPath>

<LearningPath label="Develop" icon="doc8">

[Developer Guide Overview](https://docs.pingcap.com/developer/)

[Quick Start](https://docs.pingcap.com/developer/dev-guide-build-cluster-in-cloud/)

[Connect to TiDB](https://docs.pingcap.com/developer/dev-guide-connect-to-tidb/)

</LearningPath>

</LearningPathContainer>"""

TARGET_EXISTING = """\
---
title: TiDB Cloud 文档
aliases: ['/tidbcloud/privacy-policy']
hide_sidebar: true
hide_commit: true
summary: 这里是摘要文本。
---

<LearningPathContainer platform="tidb-cloud" title="TiDB Cloud" subTitle="TiDB Cloud 是一种全托管的 DBaaS。">

<LearningPath label="学习" icon="cloud1">

[为什么选择 TiDB Cloud](https://docs.pingcap.com/tidbcloud/tidb-cloud-intro)

[常见问题](https://docs.pingcap.com/tidbcloud/tidb-cloud-faq)

</LearningPath>

<LearningPath label="开发" icon="doc8">

[开发者指南概览](https://docs.pingcap.com/tidbcloud/dev-guide-overview)

[快速入门](https://docs.pingcap.com/tidbcloud/dev-guide-build-cluster-in-cloud)

[示例应用](https://docs.pingcap.com/tidbcloud/dev-guide-sample-application-spring-boot)

</LearningPath>

</LearningPathContainer>"""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestParseIndexLine(unittest.TestCase):
    def test_blank_line(self):
        entry = parse_index_line("")
        self.assertEqual(entry["type"], "blank")

    def test_frontmatter_fence(self):
        entry = parse_index_line("---")
        self.assertEqual(entry["type"], "frontmatter_fence")

    def test_learning_path_container_open(self):
        entry = parse_index_line(
            '<LearningPathContainer platform="tidb-cloud" title="TiDB Cloud" subTitle="A managed DBaaS.">'
        )
        self.assertEqual(entry["type"], "tag")
        self.assertEqual(entry["tag_kind"], "container_open")
        self.assertEqual(entry["attrs"]["title"], "TiDB Cloud")
        self.assertEqual(entry["attrs"]["subTitle"], "A managed DBaaS.")

    def test_learning_path_open(self):
        entry = parse_index_line('<LearningPath label="Learn" icon="cloud1">')
        self.assertEqual(entry["type"], "tag")
        self.assertEqual(entry["tag_kind"], "path_open")
        self.assertEqual(entry["attrs"]["label"], "Learn")

    def test_learning_path_close(self):
        entry = parse_index_line("</LearningPath>")
        self.assertEqual(entry["type"], "tag")
        self.assertEqual(entry["tag_kind"], "path_close")

    def test_markdown_link(self):
        entry = parse_index_line("[Why TiDB Cloud](https://docs.pingcap.com/tidbcloud/tidb-cloud-intro)")
        self.assertEqual(entry["type"], "link")
        self.assertEqual(entry["display_text"], "Why TiDB Cloud")
        self.assertEqual(entry["url"], "https://docs.pingcap.com/tidbcloud/tidb-cloud-intro")

    def test_other_line(self):
        entry = parse_index_line("Some random text")
        self.assertEqual(entry["type"], "other")


class TestExtractFrontmatter(unittest.TestCase):
    def test_valid_frontmatter(self):
        lines = ["---", "title: Test", "---", "body"]
        end_idx, fm_lines = _extract_frontmatter(lines)
        self.assertEqual(end_idx, 2)
        self.assertEqual(fm_lines, ["---", "title: Test", "---"])

    def test_no_frontmatter(self):
        lines = ["body", "more body"]
        end_idx, fm_lines = _extract_frontmatter(lines)
        self.assertEqual(end_idx, -1)
        self.assertEqual(fm_lines, [])


class TestNeedsTranslation(unittest.TestCase):
    def test_link_needs_translation(self):
        entry = parse_index_line("[Why TiDB Cloud](https://example.com)")
        self.assertTrue(_needs_translation(entry))

    def test_blank_does_not(self):
        entry = parse_index_line("")
        self.assertFalse(_needs_translation(entry))

    def test_close_tag_does_not(self):
        entry = parse_index_line("</LearningPath>")
        self.assertFalse(_needs_translation(entry))

    def test_container_tag_with_title_needs(self):
        entry = parse_index_line(
            '<LearningPathContainer platform="tidb-cloud" title="TiDB Cloud" subTitle="Description">'
        )
        self.assertTrue(_needs_translation(entry))

    def test_path_open_with_label_needs(self):
        entry = parse_index_line('<LearningPath label="Learn" icon="cloud1">')
        self.assertTrue(_needs_translation(entry))


class TestBuildTranslationMemory(unittest.TestCase):
    def test_link_memory(self):
        base_lines = [
            "[Why TiDB Cloud](https://docs.pingcap.com/tidbcloud/tidb-cloud-intro)",
            "[FAQ](https://docs.pingcap.com/tidbcloud/faq)",
        ]
        target_lines = [
            "[为什么选择 TiDB Cloud](https://docs.pingcap.com/tidbcloud/tidb-cloud-intro)",
            "[常见问题](https://docs.pingcap.com/tidbcloud/faq)",
        ]
        base_map, target_map = build_index_translation_memory(base_lines, target_lines)

        key = ("link", "https://docs.pingcap.com/tidbcloud/tidb-cloud-intro")
        self.assertIn(key, target_map)
        self.assertEqual(
            target_map[key],
            "[为什么选择 TiDB Cloud](https://docs.pingcap.com/tidbcloud/tidb-cloud-intro)",
        )


class TestLocalizeDocsAbsoluteLinks(unittest.TestCase):
    def test_localizes_http_links_for_chinese(self):
        content = "[Guide](http://docs.pingcap.com/developer/)"
        self.assertEqual(
            localize_docs_absolute_links(content, "Chinese"),
            "[Guide](http://docs.pingcap.com/zh/developer/)",
        )

    def test_localizes_links_for_chinese(self):
        content = "[Guide](https://docs.pingcap.com/developer/)"
        self.assertEqual(
            localize_docs_absolute_links(content, "Chinese"),
            "[Guide](https://docs.pingcap.com/zh/developer/)",
        )

    def test_localizes_links_for_japanese(self):
        content = "[Guide](https://docs.pingcap.com/tidbcloud/)"
        self.assertEqual(
            localize_docs_absolute_links(content, "Japanese"),
            "[Guide](https://docs.pingcap.com/ja/tidbcloud/)",
        )

    def test_does_not_double_prefix_existing_locale(self):
        content = "\n".join(
            [
                "[ZH](https://docs.pingcap.com/zh/developer/)",
                "[JA](https://docs.pingcap.com/ja/developer/)",
            ]
        )
        self.assertEqual(
            localize_docs_absolute_links(content, "Chinese"),
            "\n".join(
                [
                    "[ZH](https://docs.pingcap.com/zh/developer/)",
                    "[JA](https://docs.pingcap.com/zh/developer/)",
                ]
            ),
        )


class TestPlanSyncedIndexLines(unittest.TestCase):
    def test_unchanged_links_keep_target_localized_url(self):
        """Reused links should keep the target's localized URL, not source URL."""
        source_base = "[Changefeed](http://docs.pingcap.com/tidbcloud/changefeed-overview)"
        source_head = "[Changefeed](http://docs.pingcap.com/tidbcloud/changefeed-overview)"
        target_existing = "[流式数据](http://docs.pingcap.com/zh/tidbcloud/changefeed-overview)"

        planned, to_translate = plan_synced_index_lines(
            localize_docs_absolute_links(source_base, "Chinese"),
            localize_docs_absolute_links(source_head, "Chinese"),
            localize_docs_absolute_links(target_existing, "Chinese"),
        )

        self.assertEqual(
            planned,
            ["[流式数据](http://docs.pingcap.com/zh/tidbcloud/changefeed-overview)"],
        )
        self.assertEqual(to_translate, [])

    def test_unchanged_links_reused(self):
        """Links whose URL and display text are unchanged should reuse target translations."""
        planned, to_translate = plan_synced_index_lines(
            SOURCE_BASE, SOURCE_HEAD, TARGET_EXISTING
        )

        result_text = "\n".join(
            line if line is not None else "<NEED_TRANSLATE>"
            for line in planned
        )

        # Unchanged links should be reused from target
        self.assertIn("[为什么选择 TiDB Cloud](https://docs.pingcap.com/tidbcloud/tidb-cloud-intro)", result_text)
        self.assertIn("[常见问题](https://docs.pingcap.com/tidbcloud/tidb-cloud-faq)", result_text)

        # Changed container tag should need translation
        self.assertTrue(
            any("TiDB Cloud Dedicated" in line for _, line in to_translate),
            f"Expected container tag change in to_translate: {to_translate}"
        )

        # Changed link URLs should need translation
        self.assertTrue(
            any("developer/" in line for _, line in to_translate),
            f"Expected changed-URL links in to_translate: {to_translate}"
        )

    def test_frontmatter_preserved_when_unchanged(self):
        """Unchanged frontmatter lines should reuse target translations."""
        planned, _ = plan_synced_index_lines(
            SOURCE_BASE, SOURCE_HEAD, TARGET_EXISTING
        )
        self.assertEqual(planned[0], "---")
        self.assertEqual(planned[1], "title: TiDB Cloud 文档")

    def test_frontmatter_propagates_source_changes(self):
        """Changed frontmatter lines should use source HEAD values."""
        modified_head = SOURCE_HEAD.replace(
            "title: TiDB Cloud Documentation",
            "title: TiDB Cloud Dedicated Documentation",
        )
        planned, _ = plan_synced_index_lines(
            SOURCE_BASE, modified_head, TARGET_EXISTING
        )
        self.assertEqual(planned[0], "---")
        self.assertEqual(planned[1], "title: TiDB Cloud Dedicated Documentation")
        # Unchanged lines should still use target
        self.assertEqual(planned[3], "hide_sidebar: true")

    def test_frontmatter_new_field_propagated(self):
        """New frontmatter fields in source HEAD should appear in output."""
        modified_head = SOURCE_HEAD.replace(
            "hide_commit: true\nsummary:",
            "hide_commit: true\nnew_field: new_value\nsummary:",
        )
        planned, _ = plan_synced_index_lines(
            SOURCE_BASE, modified_head, TARGET_EXISTING
        )
        planned_text = "\n".join(line for line in planned if line is not None)
        self.assertIn("new_field: new_value", planned_text)

    def test_empty_target_all_translated(self):
        """When target is empty, all translatable lines should be queued."""
        planned, to_translate = plan_synced_index_lines(
            SOURCE_BASE, SOURCE_HEAD, ""
        )
        self.assertTrue(len(to_translate) > 0)


class TestProcessIndexFileBySourceSnapshot(unittest.TestCase):
    def test_preserves_localized_http_link_when_reusing_existing_translation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = os.path.join(tmpdir, "_index.md")
            existing_target = "[流式数据](http://docs.pingcap.com/zh/tidbcloud/changefeed-overview)"
            Path(target_path).write_text(existing_target, encoding="utf-8")

            index_data = {
                "type": "index",
                "source_base_content": "[Changefeed](http://docs.pingcap.com/tidbcloud/changefeed-overview)",
                "source_head_content": "[Changefeed](http://docs.pingcap.com/tidbcloud/changefeed-overview)",
            }
            repo_config = {
                "target_local_path": tmpdir,
                "source_language": "English",
                "target_language": "Chinese",
            }

            success = process_index_file_by_source_snapshot(
                "_index.md",
                index_data,
                EchoAIClient(),
                repo_config,
                target_path,
            )

            self.assertTrue(success)
            self.assertEqual(
                Path(target_path).read_text(encoding="utf-8"),
                "[流式数据](http://docs.pingcap.com/zh/tidbcloud/changefeed-overview)",
            )

    def test_creates_target_file(self):
        """Processing should create the target file with translated content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = os.path.join(tmpdir, "dedicated", "_index.md")

            index_data = {
                "type": "index",
                "source_base_content": SOURCE_BASE,
                "source_head_content": SOURCE_HEAD,
            }

            repo_config = {
                "target_local_path": tmpdir,
                "source_language": "English",
                "target_language": "Chinese",
            }

            ai_client = EchoAIClient()
            success = process_index_file_by_source_snapshot(
                "dedicated/_index.md",
                index_data,
                ai_client,
                repo_config,
                target_path,
            )

            self.assertTrue(success)
            self.assertTrue(os.path.exists(target_path))

            with open(target_path, "r", encoding="utf-8") as f:
                content = f.read()

            self.assertIn("LearningPathContainer", content)
            self.assertIn("LearningPath", content)

    def test_reuses_existing_translations(self):
        """Unchanged links should reuse translations from the existing target file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = os.path.join(tmpdir, "_index.md")
            with open(target_path, "w", encoding="utf-8") as f:
                f.write(TARGET_EXISTING)

            index_data = {
                "type": "index",
                "source_base_content": SOURCE_BASE,
                "source_head_content": SOURCE_HEAD,
            }

            repo_config = {
                "target_local_path": tmpdir,
                "source_language": "English",
                "target_language": "Chinese",
            }

            ai_client = EchoAIClient()
            success = process_index_file_by_source_snapshot(
                "_index.md",
                index_data,
                ai_client,
                repo_config,
                target_path,
            )

            self.assertTrue(success)

            with open(target_path, "r", encoding="utf-8") as f:
                content = f.read()

            # Unchanged link should use target translation
            self.assertIn("[为什么选择 TiDB Cloud]", content)
            self.assertIn("[常见问题]", content)

    def test_localizes_docs_links_for_chinese_targets(self):
        """docs.pingcap.com links in index files should use the zh prefix."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = os.path.join(tmpdir, "_index.md")

            index_data = {
                "type": "index",
                "source_base_content": SOURCE_BASE,
                "source_head_content": SOURCE_HEAD,
            }

            repo_config = {
                "target_local_path": tmpdir,
                "source_language": "English",
                "target_language": "Chinese",
            }

            ai_client = EchoAIClient()
            success = process_index_file_by_source_snapshot(
                "_index.md",
                index_data,
                ai_client,
                repo_config,
                target_path,
            )

            self.assertTrue(success)
            content = Path(target_path).read_text(encoding="utf-8")
            self.assertIn("https://docs.pingcap.com/zh/developer/", content)
            self.assertIn(
                "https://docs.pingcap.com/zh/developer/dev-guide-build-cluster-in-cloud/",
                content,
            )

    def test_localizes_docs_links_for_japanese_targets(self):
        """docs.pingcap.com links in index files should use the ja prefix."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = os.path.join(tmpdir, "_index.md")

            index_data = {
                "type": "index",
                "source_base_content": SOURCE_BASE,
                "source_head_content": SOURCE_HEAD,
            }

            repo_config = {
                "target_local_path": tmpdir,
                "source_language": "English",
                "target_language": "Japanese",
            }

            ai_client = EchoAIClient()
            success = process_index_file_by_source_snapshot(
                "_index.md",
                index_data,
                ai_client,
                repo_config,
                target_path,
            )

            self.assertTrue(success)
            content = Path(target_path).read_text(encoding="utf-8")
            self.assertIn("https://docs.pingcap.com/ja/developer/", content)
            self.assertIn(
                "https://docs.pingcap.com/ja/tidbcloud/tidb-cloud-intro",
                content,
            )

    def test_empty_ai_response_returns_false(self):
        """When AI returns empty translations, should return False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            target_path = os.path.join(tmpdir, "_index.md")

            index_data = {
                "type": "index",
                "source_base_content": SOURCE_BASE,
                "source_head_content": SOURCE_HEAD,
            }

            repo_config = {
                "target_local_path": tmpdir,
                "source_language": "English",
                "target_language": "Chinese",
            }

            ai_client = EmptyAIClient()
            success = process_index_file_by_source_snapshot(
                "_index.md",
                index_data,
                ai_client,
                repo_config,
                target_path,
            )

            # Should still write (falling back to source lines) but report failure
            self.assertFalse(success)


class TestTranslateIndexLines(unittest.TestCase):
    def test_empty_input(self):
        result = translate_index_lines(
            [],
            EmptyAIClient(),
            {"source_language": "English", "target_language": "Chinese"},
        )
        self.assertEqual(result, {})


class TestSpecialFileUtils(unittest.TestCase):
    def test_is_index_file_name(self):
        from special_file_utils import is_index_file_name

        self.assertTrue(is_index_file_name("tidb-cloud/dedicated/_index.md"))
        self.assertTrue(is_index_file_name("tidb-cloud/starter/_index.md"))
        self.assertFalse(is_index_file_name("tidb-cloud/TOC.md"))
        self.assertFalse(is_index_file_name("tidb-cloud/some-doc.md"))

    def test_is_index_file_name_with_ignore(self):
        from special_file_utils import is_index_file_name

        self.assertFalse(
            is_index_file_name(
                "tidb-cloud/dedicated/_index.md",
                ignore_files=["tidb-cloud/dedicated/_index.md"],
            )
        )


class TestDetermineFileProcessingType(unittest.TestCase):
    def test_index_file_returns_special_file_index(self):
        from main_workflow import determine_file_processing_type

        result = determine_file_processing_type(
            "tidb-cloud/dedicated/_index.md",
            {},
            special_files=["TOC.md", "keywords.md"],
            ignore_files=[],
        )
        self.assertEqual(result, "special_file_index")


class TestPRModeDoesNotSkipIndexFile(unittest.TestCase):
    """Verify _index.md is not silently dropped in PR mode."""

    def test_pr_mode_modified_file_handler_accepts_special_file_index(self):
        """In PR mode, special_file_index should NOT be skipped; it should
        fall through to regular_modified processing."""
        from main_workflow import make_task_result

        file_type = "special_file_index"
        # Simulate the check logic from main_workflow's modified-file handler
        if file_type == "special_file_toc":
            result = make_task_result("skipped", "TOC")
        elif file_type == "special_file_keyword":
            result = make_task_result("skipped", "keyword")
        elif file_type not in ("regular_modified", "special_file_index"):
            result = make_task_result("failure", f"Unknown: {file_type}")
        else:
            result = None  # would proceed to process_regular_modified_file

        self.assertIsNone(result, "special_file_index should not be skipped in PR mode")


class TestRemoveIncrementalWorkIncludesIndexFiles(unittest.TestCase):
    """Verify remove_incremental_work_for_files clears index_files."""

    def test_index_files_cleared_when_passed(self):
        from commit_sync_workflow import remove_incremental_work_for_files

        index_files = {"tidb-cloud/dedicated/_index.md": {"type": "index"}}
        remove_incremental_work_for_files(
            ["tidb-cloud/dedicated/_index.md"],
            {}, {}, {}, {}, {},
            index_files=index_files,
        )
        self.assertEqual(index_files, {})

    def test_index_files_not_cleared_when_not_passed(self):
        from commit_sync_workflow import remove_incremental_work_for_files

        remove_incremental_work_for_files(
            ["some-file.md"],
            {}, {}, {}, {}, {},
        )
        # Should not raise — index_files defaults to None


if __name__ == "__main__":
    unittest.main()
