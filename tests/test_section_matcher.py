import sys
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from section_matcher import (
    batch_match_sections_with_ai,
    is_direct_match_candidate,
    is_system_variable_or_config,
    match_source_diff_to_target,
    parse_ai_response,
    process_modified_or_deleted_section,
)


class FakeAIClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.prompts = []

    def chat_completion(self, messages, temperature, max_tokens):
        self.prompts.append(messages[0]["content"])
        if not self.responses:
            raise AssertionError("No fake AI responses left")
        return self.responses.pop(0)


class SectionMatcherRegressionTest(unittest.TestCase):
    def build_target_fixture(self):
        target_lines = [
            "# 集成 TiDB 向量搜索与 Jina AI Embeddings API",
            "",
            "## 前置条件",
            "",
            "## 运行示例应用",
            "",
            "### 步骤 1. 克隆仓库",
            "",
            "### 步骤 2. 创建虚拟环境",
            "",
            "### 步骤 3. 安装依赖",
            "",
            "### 步骤 4. 配置环境变量",
            "旧内容",
            "",
            "### 步骤 5. 运行示例",
            "",
            "## 示例代码片段",
            "",
            "### 从 Jina AI 获取 embedding",
            "",
            "### 连接 TiDB 集群",
            "旧连接内容",
            "",
            "### 定义向量表结构",
            "旧结构内容",
            "",
            "### 使用 Jina AI 生成 embedding 并存储到 TiDB",
            "",
            "### 在 TiDB 中基于 Jina AI embedding 进行语义搜索",
            "",
            "## 另请参阅",
        ]
        target_hierarchy = {
            "1": "# 集成 TiDB 向量搜索与 Jina AI Embeddings API",
            "3": "## 前置条件",
            "5": "## 运行示例应用",
            "7": "## 运行示例应用 > ### 步骤 1. 克隆仓库",
            "9": "## 运行示例应用 > ### 步骤 2. 创建虚拟环境",
            "11": "## 运行示例应用 > ### 步骤 3. 安装依赖",
            "13": "## 运行示例应用 > ### 步骤 4. 配置环境变量",
            "16": "## 运行示例应用 > ### 步骤 5. 运行示例",
            "18": "## 示例代码片段",
            "20": "## 示例代码片段 > ### 从 Jina AI 获取 embedding",
            "22": "## 示例代码片段 > ### 连接 TiDB 集群",
            "25": "## 示例代码片段 > ### 定义向量表结构",
            "28": "## 示例代码片段 > ### 使用 Jina AI 生成 embedding 并存储到 TiDB",
            "30": "## 示例代码片段 > ### 在 TiDB 中基于 Jina AI embedding 进行语义搜索",
            "32": "## 另请参阅",
        }
        return target_lines, target_hierarchy

    def test_step_heading_falls_back_by_level_and_step_number(self):
        repo_config = {
            "source_language": "English",
            "target_language": "Chinese",
        }
        target_hierarchy = {
            "29": "# 集成 TiDB 向量搜索与 Jina AI Embeddings API > ## 运行示例应用",
            "45": "# 集成 TiDB 向量搜索与 Jina AI Embeddings API > ## 运行示例应用 > ### 步骤 3. 连接到 TiDB",
            "59": "# 集成 TiDB 向量搜索与 Jina AI Embeddings API > ## 运行示例应用 > ### 步骤 4. 配置环境变量",
            "127": "# 集成 TiDB 向量搜索与 Jina AI Embeddings API > ## 运行示例应用 > ### 步骤 5. 运行示例",
        }

        result = process_modified_or_deleted_section(
            "modified_59",
            "## Run the sample app > ### Step 4. Configure the environment variables",
            target_hierarchy,
            [],
            ai_client=None,
            repo_config=repo_config,
            max_non_system_sections=120,
        )

        self.assertEqual(
            result,
            {
                "target_line": "59",
                "target_hierarchy": "# 集成 TiDB 向量搜索与 Jina AI Embeddings API > ## 运行示例应用 > ### 步骤 4. 配置环境变量",
            },
        )

    def test_match_source_diff_to_target_batches_non_direct_sections_once(self):
        repo_config = {
            "source_language": "English",
            "target_language": "Chinese",
        }
        target_lines, target_hierarchy = self.build_target_fixture()
        source_diff_dict = {
            "modified_16": {
                "operation": "modified",
                "original_hierarchy": "## Prerequisites",
                "old_content": "old",
                "new_content": "new",
            },
            "modified_59": {
                "operation": "modified",
                "original_hierarchy": "## Run the sample app > ### Step 4. Configure the environment variables",
                "old_content": "old",
                "new_content": "new",
            },
            "added_178": {
                "operation": "added",
                "original_hierarchy": "## Sample code snippets > ### Define the vector table schema",
                "old_content": "",
                "new_content": "new",
            },
            "deleted_178": {
                "operation": "deleted",
                "original_hierarchy": "## Sample code snippets > ### Connect to the TiDB cluster",
                "old_content": "old",
                "new_content": "",
            },
        }
        ai_client = FakeAIClient(
            [
                "\n".join(
                    [
                        "```",
                        "## 前置条件",
                        "运行示例应用 > ### 步骤 4. 配置环境变量",
                        "## 示例代码片段 > ### 定义向量表结构",
                        "## 示例代码片段 > ### 连接 TiDB 集群",
                        "```",
                    ]
                )
            ]
        )

        result = match_source_diff_to_target(
            source_diff_dict,
            target_hierarchy,
            target_lines,
            ai_client,
            repo_config,
            max_non_system_sections=120,
        )

        self.assertEqual(len(ai_client.prompts), 1)
        prompt = ai_client.prompts[0]
        self.assertIn("## Prerequisites", prompt)
        self.assertIn("## Run the sample app > ### Step 4. Configure the environment variables", prompt)
        self.assertIn("## Sample code snippets > ### Define the vector table schema", prompt)
        self.assertIn("## Sample code snippets > ### Connect to the TiDB cluster", prompt)
        self.assertEqual(set(result.keys()), set(source_diff_dict.keys()))
        self.assertEqual(result["modified_16"]["target_line"], "3")
        self.assertEqual(result["modified_59"]["target_line"], "13")
        self.assertEqual(result["added_178"]["target_line"], "25")
        self.assertEqual(result["added_178"]["insertion_type"], "before_reference")
        self.assertEqual(result["deleted_178"]["target_line"], "22")

    def test_match_source_diff_to_target_falls_back_when_batch_result_is_incomplete(self):
        repo_config = {
            "source_language": "English",
            "target_language": "Chinese",
        }
        target_lines, target_hierarchy = self.build_target_fixture()
        source_diff_dict = {
            "modified_16": {
                "operation": "modified",
                "original_hierarchy": "## Prerequisites",
                "old_content": "old",
                "new_content": "new",
            },
            "modified_59": {
                "operation": "modified",
                "original_hierarchy": "## Run the sample app > ### Step 4. Configure the environment variables",
                "old_content": "old",
                "new_content": "new",
            },
        }
        ai_client = FakeAIClient(
            [
                "```\n## 前置条件\n```",
                "```\n## 前置条件\n```",
                "```\n运行示例应用 > ### 步骤 4. 配置环境变量\n```",
            ]
        )

        result = match_source_diff_to_target(
            source_diff_dict,
            target_hierarchy,
            target_lines,
            ai_client,
            repo_config,
            max_non_system_sections=120,
        )

        self.assertEqual(len(ai_client.prompts), 3)
        self.assertEqual(set(result.keys()), {"modified_16", "modified_59"})
        self.assertEqual(result["modified_16"]["target_line"], "3")
        self.assertEqual(result["modified_59"]["target_line"], "13")

    def test_bottom_modified_marker_uses_source_dict_hierarchy_for_ai_matching(self):
        repo_config = {
            "source_language": "English",
            "target_language": "Chinese",
        }
        target_lines = [
            "# TiDB 向量搜索集成概览",
            "",
            "## 对象关系映射 (ORM) 库",
            "",
            "旧内容",
        ]
        target_hierarchy = {
            "1": "# TiDB 向量搜索集成概览",
            "3": "## 对象关系映射 (ORM) 库",
        }
        source_diff_dict = {
            "modified_39": {
                "operation": "modified",
                "original_hierarchy": "bottom-modified-39",
                "matching_hierarchy": "## Object Relational Mapping (ORM) libraries",
                "old_content": "old body without heading",
                "new_content": "new body without heading",
            },
        }
        ai_client = FakeAIClient(["```\n## 对象关系映射 (ORM) 库\n```"])

        result = match_source_diff_to_target(
            source_diff_dict,
            target_hierarchy,
            target_lines,
            ai_client,
            repo_config,
            max_non_system_sections=120,
        )

        self.assertFalse(is_direct_match_candidate("bottom-modified-39"))
        combined_prompts = "\n\n".join(ai_client.prompts)
        self.assertIn("## Object Relational Mapping (ORM) libraries", combined_prompts)
        self.assertNotIn("bottom-modified-39", combined_prompts)
        self.assertEqual(result["modified_39"]["target_line"], "3")
        self.assertEqual(result["modified_39"]["source_original_hierarchy"], "bottom-modified-39")

    def test_bottom_modified_source_dict_direct_candidates_use_direct_matching(self):
        repo_config = {
            "source_language": "English",
            "target_language": "Chinese",
        }
        cases = [
            ("# English document title", "# 中文文档标题"),
            ("## `tidb_enable_example`", "## `tidb_enable_example`"),
        ]

        for source_heading, target_heading in cases:
            with self.subTest(source_heading=source_heading):
                target_lines = [target_heading, "", "旧内容"]
                target_hierarchy = {"1": target_heading}
                source_diff_dict = {
                    "modified_39": {
                        "operation": "modified",
                        "original_hierarchy": "bottom-modified-39",
                        "matching_hierarchy": source_heading,
                        "old_content": "old body without heading",
                        "new_content": "new body without heading",
                    },
                }
                ai_client = FakeAIClient([])

                result = match_source_diff_to_target(
                    source_diff_dict,
                    target_hierarchy,
                    target_lines,
                    ai_client,
                    repo_config,
                    max_non_system_sections=120,
                )

                self.assertEqual(ai_client.prompts, [])
                self.assertEqual(result["modified_39"]["target_line"], "1")

    def test_bottom_modified_marker_without_source_dict_hierarchy_falls_back_to_content(self):
        repo_config = {
            "source_language": "English",
            "target_language": "Chinese",
        }
        target_lines = [
            "# TiDB 向量搜索集成概览",
            "",
            "## 对象关系映射 (ORM) 库",
            "",
            "旧内容",
        ]
        target_hierarchy = {
            "1": "# TiDB 向量搜索集成概览",
            "3": "## 对象关系映射 (ORM) 库",
        }
        source_diff_dict = {
            "modified_39": {
                "operation": "modified",
                "original_hierarchy": "bottom-modified-39",
                "old_content": "## Object Relational Mapping (ORM) libraries\n\nold",
                "new_content": "## Object Relational Mapping (ORM) libraries\n\nnew",
            },
        }
        ai_client = FakeAIClient(["```\n## 对象关系映射 (ORM) 库\n```"])

        result = match_source_diff_to_target(
            source_diff_dict,
            target_hierarchy,
            target_lines,
            ai_client,
            repo_config,
            max_non_system_sections=120,
        )

        combined_prompts = "\n\n".join(ai_client.prompts)
        self.assertIn("## Object Relational Mapping (ORM) libraries", combined_prompts)
        self.assertNotIn("bottom-modified-39", combined_prompts)
        self.assertEqual(result["modified_39"]["target_line"], "3")

    def test_bottom_modified_marker_without_source_dict_or_content_heading_does_not_direct_match(self):
        repo_config = {
            "source_language": "English",
            "target_language": "Chinese",
        }
        target_lines = ["# 文档标题", "", "## bottom-modified-39", "", "旧内容"]
        target_hierarchy = {
            "1": "# 文档标题",
            "3": "## bottom-modified-39",
        }
        source_diff_dict = {
            "modified_39": {
                "operation": "modified",
                "original_hierarchy": "bottom-modified-39",
                "old_content": "old body without a heading",
                "new_content": "new body without a heading",
            },
        }
        ai_client = FakeAIClient(["", ""])

        result = match_source_diff_to_target(
            source_diff_dict,
            target_hierarchy,
            target_lines,
            ai_client,
            repo_config,
            max_non_system_sections=120,
        )

        self.assertFalse(is_direct_match_candidate("bottom-modified-39"))
        self.assertFalse(is_system_variable_or_config("bottom-modified-39"))
        self.assertEqual(result, {})
        self.assertEqual(len(ai_client.prompts), 2)

    def test_parse_ai_response_prefers_code_block_content(self):
        response = "\n".join(
            [
                "Here are the matching sections:",
                "```",
                "## 前置条件",
                "运行示例应用 > ### 步骤 4. 配置环境变量",
                "```",
                "Hope this helps.",
            ]
        )

        parsed = parse_ai_response(response)

        self.assertEqual(
            parsed,
            ["## 前置条件", "运行示例应用 > ### 步骤 4. 配置环境变量"],
        )

    def test_batch_match_sections_with_ai_keeps_list_shape_on_failure(self):
        repo_config = {
            "source_language": "English",
            "target_language": "Chinese",
        }

        ai_sections, failed_keys = batch_match_sections_with_ai(
            {"modified_16": "## Prerequisites"},
            {"3": "## 前置条件"},
            ai_client=FakeAIClient([""]),
            repo_config=repo_config,
        )

        self.assertEqual(ai_sections, [])
        self.assertEqual(failed_keys, ["modified_16"])


if __name__ == "__main__":
    unittest.main()
