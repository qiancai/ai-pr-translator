import json
import os
import re
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from file_updater import (
    TranslationResult,
    build_heading_anchor_slug,
    build_translation_chunks,
    get_updated_sections_from_ai,
    preprocess_diff_for_heading_anchor_stability,
    update_target_document_from_match_data,
)


class FileUpdaterRegressionTest(unittest.TestCase):
    def _cleanup_chunk_test_outputs(self, prefix):
        temp_dir = SCRIPTS_DIR / "temp_output"
        for path in temp_dir.glob(f"{prefix}_*"):
            path.unlink()

    def _build_system_sections(self, count, term_prefix=""):
        source_sections = {}
        target_sections = {}
        for index in range(1, count + 1):
            key = f"modified_{index}"
            name = f"tidb_chunk_test_{index:03d}"
            term = f" {term_prefix}{index}" if term_prefix else ""
            source_sections[key] = f"### `{name}`\n\nOld English content{term}.\n"
            target_sections[key] = f"### `{name}`\n\n旧中文内容{index}。\n"
        return source_sections, target_sections

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

    def test_preprocess_diff_rewrites_tidb_cloud_links_in_pr_mode(self):
        pr_diff = "\n".join(
            [
                "File: ai/example.md",
                "@@ -10,1 +10,1 @@",
                "+See [Private Endpoints](/tidb-cloud/test/set-up-private-endpoint-connections-serverless3.md#examples).",
                "-" * 80,
            ]
        )

        processed = preprocess_diff_for_heading_anchor_stability(
            pr_diff,
            source_language="English",
            target_language="Chinese",
            source_mode="pr",
        )

        self.assertIn(
            "+See [Private Endpoints](https://docs.pingcap.com/tidbcloud/set-up-private-endpoint-connections-serverless3#examples).",
            processed,
        )

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

    def test_preprocess_diff_rewrites_tidb_cloud_links_only_for_ai_commit_scope(self):
        pr_diff = "\n".join(
            [
                "File: docs/example.md",
                "@@ -1,1 +1,1 @@",
                "+See [Private Endpoints](/tidb-cloud/test/set-up-private-endpoint-connections-serverless2.md).",
                "-" * 80,
            ]
        )

        with mock.patch.dict(os.environ, {"SOURCE_FOLDER": "docs"}, clear=False):
            processed = preprocess_diff_for_heading_anchor_stability(
                pr_diff,
                source_language="English",
                target_language="Chinese",
                source_mode="commit",
            )

        self.assertEqual(processed, pr_diff)

    def test_preprocess_diff_does_not_rewrite_tidb_cloud_links_for_cloud_commit_scope(self):
        pr_diff = "\n".join(
            [
                "File: tidb-cloud/example.md",
                "@@ -1,1 +1,1 @@",
                "+See [Private Endpoints](/tidb-cloud/test/set-up-private-endpoint-connections-serverless2.md).",
                "-" * 80,
            ]
        )

        with mock.patch.dict(
            os.environ,
            {"SOURCE_FOLDER": "", "SOURCE_FILES": "tidb-cloud/example.md"},
            clear=False,
        ):
            processed = preprocess_diff_for_heading_anchor_stability(
                pr_diff,
                source_language="English",
                target_language="Chinese",
                source_mode="commit",
            )

        self.assertEqual(processed, pr_diff)

    def test_preprocess_diff_rewrites_tidb_cloud_links_for_ai_commit_scope(self):
        pr_diff = "\n".join(
            [
                "File: ai/example.md",
                "@@ -1,1 +1,1 @@",
                "+See [Private Endpoints](/tidb-cloud/test/set-up-private-endpoint-connections-serverless2.md).",
                "-" * 80,
            ]
        )

        with mock.patch.dict(os.environ, {"SOURCE_FOLDER": "ai"}, clear=False):
            processed = preprocess_diff_for_heading_anchor_stability(
                pr_diff,
                source_language="English",
                target_language="Chinese",
                source_mode="commit",
            )

        self.assertIn(
            "+See [Private Endpoints](https://docs.pingcap.com/tidbcloud/set-up-private-endpoint-connections-serverless2).",
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

    def test_update_trims_extra_blank_lines_at_eof(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            target_file = tmp_path / "example.md"
            match_file = tmp_path / "example-match_source_diff_to_target.json"

            target_file.write_text("# Example\n\n## Last section\n\nOld content\n", encoding="utf-8")
            match_file.write_text(
                json.dumps(
                    {
                        "modified_3": {
                            "source_operation": "modified",
                            "target_line": "3",
                            "target_hierarchy": "## Last section",
                            "target_new_content": "## Last section\n\nNew content\n\n",
                        }
                    }
                ),
                encoding="utf-8",
            )

            success = update_target_document_from_match_data(
                str(match_file), str(tmp_path), "example.md"
            )

            self.assertTrue(success)
            updated_content = target_file.read_text(encoding="utf-8")
            self.assertTrue(updated_content.endswith("New content\n"))
            self.assertFalse(updated_content.endswith("New content\n\n"))

    def test_large_system_sections_are_translated_in_chunks_and_merged(self):
        class FakeAIClient:
            def __init__(self):
                self.prompts = []

            def chat_completion(self, messages, temperature=0.1):
                prompt = messages[0]["content"]
                self.prompts.append(prompt)
                keys = list(dict.fromkeys(re.findall(r'"(modified_\d+)"\s*:', prompt)))
                return json.dumps({key: f"translated {key}" for key in keys})

        prefix = "chunk-test-unit"
        self._cleanup_chunk_test_outputs(prefix)
        try:
            source_sections, target_sections = self._build_system_sections(25)
            ai_client = FakeAIClient()

            result = get_updated_sections_from_ai(
                "File: system-variables.md\n@@ -1,1 +1,1 @@",
                target_sections,
                source_sections,
                ai_client,
                "English",
                "Chinese",
                "chunk-test-unit.md",
            )

            self.assertIsInstance(result, TranslationResult)
            self.assertEqual(len(ai_client.prompts), 2)
            self.assertEqual(set(result.keys()), set(source_sections.keys()))
            self.assertFalse(result.failures)

            temp_dir = SCRIPTS_DIR / "temp_output"
            self.assertTrue((temp_dir / f"{prefix}_updated_sections_from_ai.part-001.json").exists())
            self.assertTrue((temp_dir / f"{prefix}_updated_sections_from_ai.part-002.json").exists())
            merged_file = temp_dir / f"{prefix}_updated_sections_from_ai.json"
            self.assertTrue(merged_file.exists())
            merged = json.loads(merged_file.read_text(encoding="utf-8"))
            self.assertEqual(set(merged.keys()), set(source_sections.keys()))
        finally:
            self._cleanup_chunk_test_outputs(prefix)

    def test_translation_chunks_use_character_budget(self):
        source_sections = {
            "modified_1": "x" * 60,
            "modified_2": "x" * 60,
            "modified_3": "x" * 20,
        }

        with mock.patch("file_updater.TRANSLATION_CHUNK_MAX_SECTIONS", 20), mock.patch(
            "file_updater.TRANSLATION_CHUNK_CHAR_LIMIT",
            100,
        ):
            chunks = build_translation_chunks(source_sections)

        self.assertEqual([chunk["keys"] for chunk in chunks], [["modified_1"], ["modified_2", "modified_3"]])

    def test_translation_chunks_include_target_content_in_character_budget(self):
        source_sections = {
            "modified_1": "x" * 40,
            "modified_2": "x" * 40,
            "modified_3": "x" * 20,
        }
        target_sections = {
            "modified_1": "y" * 20,
            "modified_2": "y" * 20,
            "modified_3": "y" * 10,
        }

        with mock.patch("file_updater.TRANSLATION_CHUNK_MAX_SECTIONS", 20), mock.patch(
            "file_updater.TRANSLATION_CHUNK_CHAR_LIMIT",
            100,
        ):
            chunks = build_translation_chunks(source_sections, target_sections)

        self.assertEqual([chunk["keys"] for chunk in chunks], [["modified_1"], ["modified_2", "modified_3"]])

    def test_chunk_glossary_matching_uses_chunk_filtered_diff(self):
        class FakeAIClient:
            def __init__(self):
                self.prompts = []

            def chat_completion(self, messages, temperature=0.1):
                prompt = messages[0]["content"]
                self.prompts.append(prompt)
                keys = list(dict.fromkeys(re.findall(r'"(modified_\d+)"\s*:', prompt)))
                return json.dumps({key: f"translated {key}" for key in keys})

        prefix = "chunk-glossary-unit"
        self._cleanup_chunk_test_outputs(prefix)
        try:
            source_sections, target_sections = self._build_system_sections(25)
            seen_glossary_inputs = []

            def glossary_matcher(text, source_language=None):
                seen_glossary_inputs.append(text)
                return []

            get_updated_sections_from_ai(
                "\n".join([
                    "File: system-variables.md",
                    "@@ -1,3 +1,3 @@",
                    "-old content 1",
                    "+ChunkDiffOnlyTerm1",
                    " context",
                    "@@ -25,3 +25,3 @@",
                    "-old content 25",
                    "+ChunkDiffOnlyTerm25",
                    " context",
                ]),
                target_sections,
                source_sections,
                FakeAIClient(),
                "English",
                "Chinese",
                "chunk-glossary-unit.md",
                glossary_matcher=glossary_matcher,
            )

            self.assertEqual(len(seen_glossary_inputs), 2)
            self.assertIn("ChunkDiffOnlyTerm1", seen_glossary_inputs[0])
            self.assertNotIn("ChunkDiffOnlyTerm25", seen_glossary_inputs[0])
            self.assertIn("ChunkDiffOnlyTerm25", seen_glossary_inputs[1])
            self.assertNotIn("ChunkDiffOnlyTerm1", seen_glossary_inputs[1])
        finally:
            self._cleanup_chunk_test_outputs(prefix)

    def test_modified_glossary_matching_ignores_source_and_target_sections(self):
        class FakeAIClient:
            def chat_completion(self, messages, temperature=0.1):
                return json.dumps({"modified_1": "translated modified_1"})

        prefix = "modified-glossary-unit"
        self._cleanup_chunk_test_outputs(prefix)
        seen_glossary_inputs = []

        def glossary_matcher(text, source_language=None):
            seen_glossary_inputs.append(text)
            return []

        try:
            get_updated_sections_from_ai(
                "\n".join([
                    "File: example.md",
                    "@@ -1,3 +1,3 @@",
                    "-old content",
                    "+DiffOnlyTerm",
                    " context",
                ]),
                {
                    "modified_1": "### TargetOnlyTerm\n\n旧中文内容。",
                },
                {
                    "modified_1": "### SourceOnlyTerm\n\nOld English content.",
                },
                FakeAIClient(),
                "English",
                "Chinese",
                "modified-glossary-unit.md",
                glossary_matcher=glossary_matcher,
            )

            self.assertEqual(len(seen_glossary_inputs), 1)
            self.assertIn("DiffOnlyTerm", seen_glossary_inputs[0])
            self.assertNotIn("SourceOnlyTerm", seen_glossary_inputs[0])
            self.assertNotIn("TargetOnlyTerm", seen_glossary_inputs[0])
        finally:
            self._cleanup_chunk_test_outputs(prefix)

    def test_chunk_glossary_matching_ignores_chunk_source_and_target_sections(self):
        class FakeAIClient:
            def chat_completion(self, messages, temperature=0.1):
                prompt = messages[0]["content"]
                keys = list(dict.fromkeys(re.findall(r'"(modified_\d+)"\s*:', prompt)))
                return json.dumps({key: f"translated {key}" for key in keys})

        prefix = "chunk-glossary-ignore-unit"
        self._cleanup_chunk_test_outputs(prefix)
        try:
            source_sections, target_sections = self._build_system_sections(25)
            source_sections["modified_1"] += "\nSourceOnlyTerm1\n"
            target_sections["modified_1"] += "\nTargetOnlyTerm1\n"
            source_sections["modified_25"] += "\nSourceOnlyTerm25\n"
            target_sections["modified_25"] += "\nTargetOnlyTerm25\n"
            seen_glossary_inputs = []

            def glossary_matcher(text, source_language=None):
                seen_glossary_inputs.append(text)
                return []

            get_updated_sections_from_ai(
                "\n".join([
                    "File: system-variables.md",
                    "@@ -1,3 +1,3 @@",
                    "-old content 1",
                    "+DiffOnlyTerm1",
                    " context",
                    "@@ -25,3 +25,3 @@",
                    "-old content 25",
                    "+DiffOnlyTerm25",
                    " context",
                ]),
                target_sections,
                source_sections,
                FakeAIClient(),
                "English",
                "Chinese",
                "chunk-glossary-ignore-unit.md",
                glossary_matcher=glossary_matcher,
            )

            self.assertEqual(len(seen_glossary_inputs), 2)
            self.assertIn("DiffOnlyTerm1", seen_glossary_inputs[0])
            self.assertNotIn("DiffOnlyTerm25", seen_glossary_inputs[0])
            self.assertNotIn("SourceOnlyTerm1", seen_glossary_inputs[0])
            self.assertNotIn("TargetOnlyTerm1", seen_glossary_inputs[0])
            self.assertIn("DiffOnlyTerm25", seen_glossary_inputs[1])
            self.assertNotIn("DiffOnlyTerm1", seen_glossary_inputs[1])
            self.assertNotIn("SourceOnlyTerm25", seen_glossary_inputs[1])
            self.assertNotIn("TargetOnlyTerm25", seen_glossary_inputs[1])
        finally:
            self._cleanup_chunk_test_outputs(prefix)

    def test_chunk_failure_is_attached_to_translation_result(self):
        class FakeAIClient:
            def __init__(self):
                self.prompts = []

            def chat_completion(self, messages, temperature=0.1):
                prompt = messages[0]["content"]
                self.prompts.append(prompt)
                if len(self.prompts) == 2:
                    return "not json"
                keys = list(dict.fromkeys(re.findall(r'"(modified_\d+)"\s*:', prompt)))
                return json.dumps({key: f"translated {key}" for key in keys})

        prefix = "chunk-failure-unit"
        self._cleanup_chunk_test_outputs(prefix)
        try:
            source_sections, target_sections = self._build_system_sections(25)

            result = get_updated_sections_from_ai(
                "File: system-variables.md\n@@ -1,1 +1,1 @@",
                target_sections,
                source_sections,
                FakeAIClient(),
                "English",
                "Chinese",
                "chunk-failure-unit.md",
            )

            self.assertIsInstance(result, TranslationResult)
            self.assertEqual(len(result), 20)
            self.assertEqual(len(result.failures), 1)
            self.assertIn("failed to translate chunk 2/2", result.failures[0])
            self.assertIn("tidb_chunk_test_021", result.failures[0])
        finally:
            self._cleanup_chunk_test_outputs(prefix)


if __name__ == "__main__":
    unittest.main()
