import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from toc_processor import (
    is_toc_translation_needed,
    parse_toc_line,
    plan_synced_toc_lines,
    process_toc_file,
    process_toc_operations,
)


class FakeAIClient:
    def chat_completion(self, messages, temperature=0.1):
        prompt = messages[0]["content"]
        translations = {}
        if "Select Your Plan" in prompt:
            translations["line_0"] = "  - [选择套餐](/tidb-cloud/select-cluster-tier.md)"
        if "Manage TiDB Cloud Resources and Projects" in prompt:
            translations["line_1"] = "- [管理 TiDB Cloud 资源和项目](/tidb-cloud/manage-projects-and-resources.md)"
        if "Manage {{{ .dedicated }}} Clusters" in prompt:
            translations["line_2"] = "- 管理 {{{ .dedicated }}} 集群"
        if "Use TiFlash for HTAP" in prompt:
            translations["line_3"] = "  - 使用 TiFlash 实现 HTAP"
        if "Events" in prompt:
            translations["line_4"] = "    - [事件](/tidb-cloud/tidb-cloud-events.md)"
        if "Upgrade the TiDB Version" in prompt:
            translations["line_5"] = "  - [升级 TiDB 版本](/tidb-cloud/upgrade-tidb-cluster.md)"
        if "Delete a {{{ .dedicated }}} Cluster" in prompt:
            translations["line_6"] = "  - [删除 {{{ .dedicated }}} 集群](/tidb-cloud/delete-tidb-cluster.md)"
        if "Connect AWS DMS to TiDB Cloud]" in prompt:
            translations["line_7"] = "    - [连接 AWS DMS 到 TiDB Cloud](/tidb-cloud/tidb-cloud-connect-aws-dms.md)"
        return json.dumps(translations, ensure_ascii=False)


class EmptyAIClient:
    def chat_completion(self, messages, temperature=0.1):
        return "{}"


class RecordingAIClient:
    def __init__(self, response):
        self.response = response
        self.prompts = []

    def chat_completion(self, messages, temperature=0.1):
        self.prompts.append(messages[0]["content"])
        return json.dumps(self.response, ensure_ascii=False)


class TocProcessorSnapshotSyncTest(unittest.TestCase):
    def test_parse_toc_line_handles_nested_brackets_and_suffix_images(self):
        release_entry = parse_toc_line(
            "- [[2024-09-15] TiDB Cloud Console Maintenance Notification]"
            "(/tidb-cloud/releases/notification-2024-09-15-console-maintenance.md)"
        )
        sql_entry = parse_toc_line(
            "    - [`ADMIN CHECK [TABLE|INDEX]`](/sql-statements/sql-statement-admin-check-table-index.md)"
        )
        suffix_entry = parse_toc_line(
            "  - [Data Service](/tidb-cloud/data-service-concepts.md) "
            "![BETA](/media/tidb-cloud/blank_transparent_placeholder.png)"
        )

        self.assertEqual(release_entry["type"], "link")
        self.assertEqual(
            release_entry["text"],
            "[2024-09-15] TiDB Cloud Console Maintenance Notification",
        )
        self.assertEqual(
            release_entry["link"],
            "/tidb-cloud/releases/notification-2024-09-15-console-maintenance.md",
        )
        self.assertEqual(sql_entry["type"], "link")
        self.assertEqual(sql_entry["text"], "`ADMIN CHECK [TABLE|INDEX]`")
        self.assertEqual(suffix_entry["type"], "link")
        self.assertEqual(
            suffix_entry["suffix"],
            " ![BETA](/media/tidb-cloud/blank_transparent_placeholder.png)",
        )

    def test_single_word_english_toc_link_still_requires_translation(self):
        self.assertTrue(is_toc_translation_needed("- [Events](/events.md)"))
        self.assertTrue(is_toc_translation_needed("- [Overview](/overview.md)"))
        self.assertFalse(is_toc_translation_needed("- [2024-09-15](/release.md)"))

    def test_added_plain_toc_line_is_retranslated_even_when_text_exists_in_base(self):
        planned_lines, lines_to_translate = plan_synced_toc_lines(
            "\n".join(
                [
                    "- A",
                    "- B",
                ]
            ),
            "\n".join(
                [
                    "- B",
                    "- A",
                ]
            ),
            "\n".join(
                [
                    "- 甲",
                    "- 乙",
                ]
            ),
            source_added_line_numbers=[1],
        )

        self.assertIsNone(planned_lines[0])
        self.assertEqual(lines_to_translate, [(0, "- B")])
        self.assertEqual(planned_lines[1], "- 甲")

    def test_missing_toc_translation_fails_without_writing_source_text(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            toc_path = Path(tmpdir) / "TOC-tidb-cloud.md"
            toc_path.write_text("- 旧行", encoding="utf-8")

            success = process_toc_file(
                "TOC-tidb-cloud.md",
                {
                    "type": "toc",
                    "operations": [],
                    "source_base_content": "- Old",
                    "source_head_content": "- New",
                    "source_added_line_numbers": [1],
                },
                {},
                None,
                EmptyAIClient(),
                {
                    "target_local_path": tmpdir,
                    "source_language": "English",
                    "target_language": "Chinese",
                },
            )

            result = toc_path.read_text(encoding="utf-8")

        self.assertFalse(success)
        self.assertEqual(result, "- 旧行")

    def test_snapshot_sync_preserves_source_structure_for_unlinked_toc_groups(self):
        source_base = "\n".join(
            [
                "## GUIDES",
                "",
                "- Manage Cluster",
                "  - Plan Your Cluster",
                "    - [Select Your Cluster Plan](/tidb-cloud/select-cluster-tier.md)",
                "    - [Determine Your TiDB Size](/tidb-cloud/size-your-cluster.md)",
                "  - [Configure Maintenance Window](/tidb-cloud/configure-maintenance-window.md)",
                "  - Use an HTAP Cluster with TiFlash",
                "    - [TiFlash Overview](/tiflash/tiflash-overview.md)",
                "    - [Create TiFlash Replicas](/tiflash/create-tiflash-replicas.md)",
                "    - [Read Data from TiFlash](/tiflash/use-tidb-to-read-tiflash.md)",
                "    - [Cluster Events](/tidb-cloud/tidb-cloud-events.md)",
                "  - [Upgrade a TiDB Cluster](/tidb-cloud/upgrade-tidb-cluster.md)",
                "  - [Delete a TiDB Cluster](/tidb-cloud/delete-tidb-cluster.md)",
                "- Migrate or Import Data",
                "    - [Connect AWS DMS to TiDB Cloud clusters](/tidb-cloud/tidb-cloud-connect-aws-dms.md)",
            ]
        )
        source_head = "\n".join(
            [
                "## GUIDES",
                "",
                "- Plan Your Cluster",
                "  - [Select Your Plan](/tidb-cloud/select-cluster-tier.md)",
                "  - [Determine Your TiDB Size](/tidb-cloud/size-your-cluster.md)",
                "- [Manage TiDB Cloud Resources and Projects](/tidb-cloud/manage-projects-and-resources.md)",
                "- Manage {{{ .dedicated }}} Clusters",
                "  - [Configure Maintenance Window](/tidb-cloud/configure-maintenance-window.md)",
                "  - Use TiFlash for HTAP",
                "    - [TiFlash Overview](/tiflash/tiflash-overview.md)",
                "    - [Create TiFlash Replicas](/tiflash/create-tiflash-replicas.md)",
                "    - [Read Data from TiFlash](/tiflash/use-tidb-to-read-tiflash.md)",
                "    - [Events](/tidb-cloud/tidb-cloud-events.md)",
                "  - [Upgrade the TiDB Version](/tidb-cloud/upgrade-tidb-cluster.md)",
                "  - [Delete a {{{ .dedicated }}} Cluster](/tidb-cloud/delete-tidb-cluster.md)",
                "- Migrate or Import Data",
                "    - [Connect AWS DMS to TiDB Cloud](/tidb-cloud/tidb-cloud-connect-aws-dms.md)",
            ]
        )
        target = "\n".join(
            [
                "## 指南",
                "",
                "- 管理集群",
                "  - 规划集群",
                "    - [选择集群套餐](/tidb-cloud/select-cluster-tier.md)",
                "    - [确定 TiDB 的大小](/tidb-cloud/size-your-cluster.md)",
                "  - [配置维护窗口](/tidb-cloud/configure-maintenance-window.md)",
                "  - 使用带有 TiFlash 的 HTAP 集群",
                "    - [TiFlash 简介](/tiflash/tiflash-overview.md)",
                "    - [构建 TiFlash 副本](/tiflash/create-tiflash-replicas.md)",
                "    - [使用 TiDB 读取 TiFlash](/tiflash/use-tidb-to-read-tiflash.md)",
                "    - [集群事件](/tidb-cloud/tidb-cloud-events.md)",
                "  - [升级 TiDB 集群](/tidb-cloud/upgrade-tidb-cluster.md)",
                "  - [删除 TiDB 集群](/tidb-cloud/delete-tidb-cluster.md)",
                "- 迁移或导入数据",
                "    - [将 AWS DMS 连接到 TiDB Cloud 集群](/tidb-cloud/tidb-cloud-connect-aws-dms.md)",
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            toc_path = Path(tmpdir) / "TOC-tidb-cloud.md"
            toc_path.write_text(target, encoding="utf-8")

            success = process_toc_file(
                "TOC-tidb-cloud.md",
                {
                    "type": "toc",
                    "operations": [],
                    "source_base_content": source_base,
                    "source_head_content": source_head,
                },
                {},
                None,
                FakeAIClient(),
                {
                    "target_local_path": tmpdir,
                    "source_language": "English",
                    "target_language": "Chinese",
                },
            )

            result = toc_path.read_text(encoding="utf-8")

        self.assertTrue(success)
        self.assertIn("- 规划集群", result)
        self.assertIn("- 管理 {{{ .dedicated }}} 集群", result)
        self.assertIn("  - 使用 TiFlash 实现 HTAP", result)
        self.assertIn("    - [事件](/tidb-cloud/tidb-cloud-events.md)", result)
        self.assertIn("  - [升级 TiDB 版本](/tidb-cloud/upgrade-tidb-cluster.md)", result)
        self.assertIn("  - [删除 {{{ .dedicated }}} 集群](/tidb-cloud/delete-tidb-cluster.md)", result)
        self.assertIn("    - [连接 AWS DMS 到 TiDB Cloud](/tidb-cloud/tidb-cloud-connect-aws-dms.md)", result)
        self.assertNotIn("- 管理集群", result)
        self.assertNotIn("使用带有 TiFlash 的 HTAP 集群", result)

    def test_snapshot_glossary_matches_only_changed_toc_lines(self):
        seen_glossary_inputs = []

        def glossary_matcher(text, source_language=None):
            seen_glossary_inputs.append((text, source_language))
            if "Events" in text:
                return [{"en": "Events", "zh": "事件", "comment": "TOC term"}]
            return []

        ai_client = RecordingAIClient({"line_0": "- [事件](/events.md)"})

        with tempfile.TemporaryDirectory() as tmpdir:
            toc_path = Path(tmpdir) / "TOC-test.md"
            toc_path.write_text(
                "\n".join(
                    [
                        "- [旧标题](/events.md)",
                        "- [稳定项](/stable.md)",
                    ]
                ),
                encoding="utf-8",
            )

            success = process_toc_file(
                "TOC-test.md",
                {
                    "type": "toc",
                    "operations": [],
                    "source_base_content": "\n".join(
                        [
                            "- [Old Title](/events.md)",
                            "- [Stable](/stable.md)",
                        ]
                    ),
                    "source_head_content": "\n".join(
                        [
                            "- [Events](/events.md)",
                            "- [Stable](/stable.md)",
                        ]
                    ),
                },
                {},
                None,
                ai_client,
                {
                    "target_local_path": tmpdir,
                    "source_language": "English",
                    "target_language": "Chinese",
                },
                glossary_matcher=glossary_matcher,
            )

        self.assertTrue(success)
        self.assertEqual([("- [Events](/events.md)", "English")], seen_glossary_inputs)
        self.assertEqual(1, len(ai_client.prompts))
        self.assertIn("| Events | 事件 | TOC term |", ai_client.prompts[0])
        self.assertNotIn("Stable", ai_client.prompts[0])


class TocProcessorOperationLevelTest(unittest.TestCase):
    def test_added_group_preserves_source_order(self):
        source_lines = [
            "- [Old](/old.md)",
            "- A",
            "- B",
        ]
        target_lines = [
            "- [旧](/old.md)",
        ]
        toc_results = process_toc_operations(
            "TOC-test.md",
            {
                "added_lines": [
                    {"line_number": 2, "content": "- A", "is_header": False},
                    {"line_number": 3, "content": "- B", "is_header": False},
                ],
                "modified_lines": [],
                "deleted_lines": [],
            },
            source_lines,
            target_lines,
            "",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            toc_path = Path(tmpdir) / "TOC-test.md"
            toc_path.write_text("\n".join(target_lines), encoding="utf-8")

            success = process_toc_file(
                "TOC-test.md",
                {
                    "type": "toc",
                    "operations": toc_results["added"],
                },
                {"mode": "pr"},
                None,
                EmptyAIClient(),
                {
                    "target_local_path": tmpdir,
                    "source_language": "English",
                    "target_language": "Chinese",
                },
            )

            result = toc_path.read_text(encoding="utf-8").splitlines()

        self.assertTrue(success)
        self.assertEqual(["- [旧](/old.md)", "- A", "- B"], result)

    def test_modified_group_updates_each_line_once(self):
        source_lines = [
            "- [Anchor](/anchor.md)",
            "- new1",
            "- new2",
        ]
        target_lines = [
            "- [锚点](/anchor.md)",
            "- old1",
            "- old2",
        ]
        toc_results = process_toc_operations(
            "TOC-test.md",
            {
                "added_lines": [],
                "modified_lines": [
                    {"line_number": 2, "content": "- new1", "is_header": False},
                    {"line_number": 3, "content": "- new2", "is_header": False},
                ],
                "deleted_lines": [],
            },
            source_lines,
            target_lines,
            "",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            toc_path = Path(tmpdir) / "TOC-test.md"
            toc_path.write_text("\n".join(target_lines), encoding="utf-8")

            success = process_toc_file(
                "TOC-test.md",
                {
                    "type": "toc",
                    "operations": toc_results["modified"],
                },
                {"mode": "pr"},
                None,
                EmptyAIClient(),
                {
                    "target_local_path": tmpdir,
                    "source_language": "English",
                    "target_language": "Chinese",
                },
            )

            result = toc_path.read_text(encoding="utf-8").splitlines()

        self.assertTrue(success)
        self.assertEqual(["- [锚点](/anchor.md)", "- new1", "- new2"], result)

    def test_added_group_can_use_next_link_when_immediate_previous_line_has_no_link(self):
        source_lines = [
            "## GUIDES",
            "- Parent Group",
            "  - [Child](/child.md)",
            "- [Existing](/existing.md)",
        ]
        target_lines = [
            "## 指南",
            "- 父分组",
            "- [已有](/existing.md)",
        ]
        toc_results = process_toc_operations(
            "TOC-test.md",
            {
                "added_lines": [
                    {"line_number": 3, "content": "  - [Child](/child.md)", "is_header": False},
                ],
                "modified_lines": [],
                "deleted_lines": [],
            },
            source_lines,
            target_lines,
            "",
        )

        self.assertEqual(1, len(toc_results["added"]))

        with tempfile.TemporaryDirectory() as tmpdir:
            toc_path = Path(tmpdir) / "TOC-test.md"
            toc_path.write_text("\n".join(target_lines), encoding="utf-8")

            success = process_toc_file(
                "TOC-test.md",
                {
                    "type": "toc",
                    "operations": toc_results["added"],
                },
                {"mode": "pr"},
                None,
                EmptyAIClient(),
                {
                    "target_local_path": tmpdir,
                    "source_language": "English",
                    "target_language": "Chinese",
                },
            )

            result = toc_path.read_text(encoding="utf-8").splitlines()

        self.assertTrue(success)
        self.assertEqual(
            ["## 指南", "- 父分组", "  - [Child](/child.md)", "- [已有](/existing.md)"],
            result,
        )

    def test_deleted_plain_group_title_is_removed_using_next_anchor(self):
        source_base_lines = [
            "## GUIDES",
            "- Parent Group",
            "  - [Child](/child.md)",
            "- [Existing](/existing.md)",
        ]
        source_head_lines = [
            "## GUIDES",
            "  - [Child](/child.md)",
            "- [Existing](/existing.md)",
        ]
        target_lines = [
            "## 指南",
            "- 父分组",
            "  - [子项](/child.md)",
            "- [已有](/existing.md)",
        ]
        toc_results = process_toc_operations(
            "TOC-test.md",
            {
                "added_lines": [],
                "modified_lines": [],
                "deleted_lines": [
                    {"line_number": 2, "content": "- Parent Group", "is_header": False},
                ],
            },
            source_head_lines,
            target_lines,
            "",
            source_base_lines=source_base_lines,
        )

        self.assertEqual([2], [op["target_line"] for op in toc_results["deleted"]])

        with tempfile.TemporaryDirectory() as tmpdir:
            toc_path = Path(tmpdir) / "TOC-test.md"
            toc_path.write_text("\n".join(target_lines), encoding="utf-8")

            success = process_toc_file(
                "TOC-test.md",
                {
                    "type": "toc",
                    "operations": toc_results["deleted"],
                },
                {"mode": "pr"},
                None,
                EmptyAIClient(),
                {
                    "target_local_path": tmpdir,
                    "source_language": "English",
                    "target_language": "Chinese",
                },
            )

            result = toc_path.read_text(encoding="utf-8").splitlines()

        self.assertTrue(success)
        self.assertEqual(["## 指南", "  - [子项](/child.md)", "- [已有](/existing.md)"], result)

    def test_deleted_plain_group_is_skipped_when_target_range_is_ambiguous(self):
        source_base_lines = [
            "- [A](/a.md)",
            "- Parent Group",
            "- [B](/b.md)",
        ]
        source_head_lines = [
            "- [A](/a.md)",
            "- [B](/b.md)",
        ]
        target_lines = [
            "- [甲](/a.md)",
            "- 目标独有章节",
            "- 父分组",
            "- [乙](/b.md)",
        ]

        toc_results = process_toc_operations(
            "TOC-test.md",
            {
                "added_lines": [],
                "modified_lines": [],
                "deleted_lines": [
                    {"line_number": 2, "content": "- Parent Group", "is_header": False},
                ],
            },
            source_head_lines,
            target_lines,
            "",
            source_base_lines=source_base_lines,
        )

        self.assertEqual([], toc_results["deleted"])

    def test_modified_group_is_skipped_when_target_range_is_ambiguous(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            toc_path = Path(tmpdir) / "TOC-test.md"
            toc_path.write_text(
                "\n".join(
                    [
                        "- [锚点](/anchor.md)",
                        "- 目标独有章节",
                        "- old1",
                        "- old2",
                    ]
                ),
                encoding="utf-8",
            )

            success = process_toc_file(
                "TOC-test.md",
                {
                    "type": "toc",
                    "operations": [
                        {
                            "group_id": "modified:2",
                            "group_offset": 0,
                            "source_line": 2,
                            "content": "- new1",
                            "needs_translation": False,
                            "anchor_previous_link": "(/anchor.md)",
                            "anchor_previous_source_line": 1,
                        },
                        {
                            "group_id": "modified:2",
                            "group_offset": 1,
                            "source_line": 3,
                            "content": "- new2",
                            "needs_translation": False,
                            "anchor_previous_link": "(/anchor.md)",
                            "anchor_previous_source_line": 1,
                        },
                    ],
                },
                {"mode": "pr"},
                None,
                EmptyAIClient(),
                {
                    "target_local_path": tmpdir,
                    "source_language": "English",
                    "target_language": "Chinese",
                },
            )

            result = toc_path.read_text(encoding="utf-8").splitlines()

        self.assertTrue(success)
        self.assertEqual(
            ["- [锚点](/anchor.md)", "- 目标独有章节", "- old1", "- old2"],
            result,
        )

    def test_modified_plain_text_group_with_original_content_updates_when_anchor_window_is_unique(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            toc_path = Path(tmpdir) / "TOC-test.md"
            toc_path.write_text(
                "\n".join(
                    [
                        "- [锚点](/anchor.md)",
                        "- 父分组",
                        "- [后续](/next.md)",
                    ]
                ),
                encoding="utf-8",
            )

            ai_client = RecordingAIClient({"line_0": "- 新父分组"})
            success = process_toc_file(
                "TOC-test.md",
                {
                    "type": "toc",
                    "operations": [
                        {
                            "group_id": "modified:2",
                            "group_offset": 0,
                            "source_line": 2,
                            "content": "- New Parent Group",
                            "original_content": "- Parent Group",
                            "needs_translation": True,
                            "anchor_previous_link": "(/anchor.md)",
                            "anchor_previous_source_line": 1,
                            "anchor_next_link": "(/next.md)",
                            "anchor_next_source_line": 3,
                        },
                    ],
                },
                {"mode": "pr"},
                None,
                ai_client,
                {
                    "target_local_path": tmpdir,
                    "source_language": "English",
                    "target_language": "Chinese",
                },
            )

            result = toc_path.read_text(encoding="utf-8").splitlines()

        self.assertTrue(success)
        self.assertEqual(
            ["- [锚点](/anchor.md)", "- 新父分组", "- [后续](/next.md)"],
            result,
        )

    def test_operation_level_glossary_matches_only_changed_toc_lines(self):
        source_lines = [
            "- [Anchor](/anchor.md)",
            "- [Events](/events.md)",
            "- [KeepMe](/keep.md)",
        ]
        target_lines = [
            "- [锚点](/anchor.md)",
            "- [保留项](/keep.md)",
        ]
        toc_results = process_toc_operations(
            "TOC-test.md",
            {
                "added_lines": [
                    {"line_number": 2, "content": "- [Events](/events.md)", "is_header": False},
                ],
                "modified_lines": [],
                "deleted_lines": [],
            },
            source_lines,
            target_lines,
            "",
        )

        seen_glossary_inputs = []

        def glossary_matcher(text, source_language=None):
            seen_glossary_inputs.append((text, source_language))
            if "Events" in text:
                return [{"en": "Events", "zh": "事件", "comment": "TOC term"}]
            return []

        ai_client = RecordingAIClient({"line_0": "- [事件](/events.md)"})

        with tempfile.TemporaryDirectory() as tmpdir:
            toc_path = Path(tmpdir) / "TOC-test.md"
            toc_path.write_text("\n".join(target_lines), encoding="utf-8")

            success = process_toc_file(
                "TOC-test.md",
                {
                    "type": "toc",
                    "operations": toc_results["added"],
                },
                {"mode": "pr"},
                None,
                ai_client,
                {
                    "target_local_path": tmpdir,
                    "source_language": "English",
                    "target_language": "Chinese",
                },
                glossary_matcher=glossary_matcher,
            )

        self.assertTrue(success)
        self.assertEqual([("- [Events](/events.md)", "English")], seen_glossary_inputs)
        self.assertEqual(1, len(ai_client.prompts))
        self.assertIn("| Events | 事件 | TOC term |", ai_client.prompts[0])
        self.assertNotIn("KeepMe", ai_client.prompts[0])


if __name__ == "__main__":
    unittest.main()
