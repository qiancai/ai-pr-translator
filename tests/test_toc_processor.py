import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from toc_processor import parse_toc_line, plan_synced_toc_lines, process_toc_file


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
        class EmptyAIClient:
            def chat_completion(self, messages, temperature=0.1):
                return "{}"

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
                object(),
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
                object(),
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


if __name__ == "__main__":
    unittest.main()
