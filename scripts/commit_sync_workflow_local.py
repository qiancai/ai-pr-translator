"""
Local entry point for commit-based sync verification.

Fill in the configuration below, then run:

    python commit_sync_workflow_local.py
"""

import os
import sys


SOURCE_REPO = "pingcap/docs"
TARGET_REPO = "pingcap/docs-cn"
SOURCE_BRANCH = "release-8.5"

# Set these two refs explicitly for local verification.
SOURCE_BASE_REF = "b7469123c65aa8409bf754e4a7909e16d8ed3082"
SOURCE_HEAD_REF = "7a75928211563b1c5632d8e2a7ece92608832cac"

# Optional scope filters.
SOURCE_FOLDER = "ai"
SOURCE_FILES = "ai/integrations/vector-search-integrate-with-jinaai-embedding.md" #Specify this field if you only want to translate specific files. Separate multiple files with commas.

AI_PROVIDER = "deepseek"  # Options: "deepseek", "gemini", "openai", "azure"
TARGET_REPO_PATH = "/Users/grcai/Documents/GitHub/docs-cn"
TIDB_CLOUD_ABSOLUTE_LINK_PREFIX = "https://docs.pingcap.com/tidbcloud/"

# Optional glossary path.
TERMS_PATH = "/Users/grcai/Documents/GitHub/docs/resources/terms.md"

# Keep existing PR-mode defaults unless you explicitly want to translate excluded folders.
SKIP_TRANSLATING_CLOUD_DOCS_TO_ZH = True
SKIP_TRANSLATING_AI_DOCS_TO_ZH = False


def main():
    required = {
        "SOURCE_REPO": SOURCE_REPO,
        "TARGET_REPO": TARGET_REPO,
        "SOURCE_BRANCH": SOURCE_BRANCH,
        "SOURCE_BASE_REF": SOURCE_BASE_REF,
        "SOURCE_HEAD_REF": SOURCE_HEAD_REF,
        "SOURCE_FOLDER": SOURCE_FOLDER,
        "SOURCE_FILES": SOURCE_FILES,
        "TARGET_REPO_PATH": TARGET_REPO_PATH,
        "AI_PROVIDER": AI_PROVIDER,
        "TERMS_PATH": TERMS_PATH,
        "TIDB_CLOUD_ABSOLUTE_LINK_PREFIX": TIDB_CLOUD_ABSOLUTE_LINK_PREFIX,
        "SKIP_TRANSLATING_CLOUD_DOCS_TO_ZH": str(SKIP_TRANSLATING_CLOUD_DOCS_TO_ZH).lower(),
        "SKIP_TRANSLATING_AI_DOCS_TO_ZH": str(SKIP_TRANSLATING_AI_DOCS_TO_ZH).lower(),
    }

    missing_refs = [name for name in ("SOURCE_BASE_REF", "SOURCE_HEAD_REF") if not required[name].strip()]
    if missing_refs:
        print("❌ Please set explicit commit refs before local verification:")
        for name in missing_refs:
            print(f"   {name} is empty")
        return 1

    if not os.getenv("GITHUB_TOKEN"):
        print("❌ GITHUB_TOKEN is not set in the environment.")
        return 1

    for key, value in required.items():
        os.environ[key] = value

    from commit_sync_workflow import main as commit_main

    return commit_main()


if __name__ == "__main__":
    raise SystemExit(main())
