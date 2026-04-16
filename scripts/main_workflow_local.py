"""
Local entry point for PR-based sync verification.

Fill in SOURCE_PR_URL and local paths, then run:

    python main_workflow_local.py
"""

import os


SOURCE_PR_URL = "https://github.com/pingcap/docs-cn/pull/21434"
AI_PROVIDER = "deepseek"  # Options: "deepseek", "gemini", "openai", "azure"

DOCS_CN_LOCAL_PATH = "/Users/grcai/Documents/GitHub/docs-cn"
DOCS_LOCAL_PATH = "/Users/grcai/Documents/GitHub/docs"
TERMS_PATH = "/Users/grcai/Documents/GitHub/docs/resources/terms.md"
TIDB_CLOUD_ABSOLUTE_LINK_PREFIX = "https://docs.pingcap.com/tidbcloud/"

SKIP_TRANSLATING_CLOUD_DOCS_TO_ZH = True
CLOUD_FOLDER_NAME = "tidb-cloud"

SKIP_TRANSLATING_AI_DOCS_TO_ZH = True
AI_DOCS_FOLDER_NAME = "ai"

# Local verification should leave translated files unstaged for easy review.
SKIP_GIT_ADD = True


def _parse_github_pr_url(pr_url):
    parts = pr_url.rstrip("/").split("/")
    if len(parts) < 7 or parts[-2] != "pull":
        raise ValueError(f"Invalid GitHub PR URL: {pr_url}")
    return parts[-4], parts[-3], parts[-1]


def _target_for_source_pr(source_pr_url):
    owner, source_repo, _ = _parse_github_pr_url(source_pr_url)
    if source_repo == "docs":
        return {
            "target_pr_url": f"https://github.com/{owner}/docs-cn/pull/0",
            "target_repo_path": DOCS_CN_LOCAL_PATH,
        }
    if source_repo == "docs-cn":
        return {
            "target_pr_url": f"https://github.com/{owner}/docs/pull/0",
            "target_repo_path": DOCS_LOCAL_PATH,
        }

    raise ValueError(
        f"Unsupported source repository '{owner}/{source_repo}'. "
        "Expected 'docs' or 'docs-cn'."
    )


def main():
    try:
        target = _target_for_source_pr(SOURCE_PR_URL)
    except ValueError as e:
        print(f"❌ {e}")
        return 1

    if not os.getenv("GITHUB_TOKEN"):
        print("❌ GITHUB_TOKEN is not set in the environment.")
        return 1

    required = {
        "SOURCE_PR_URL": SOURCE_PR_URL,
        "TARGET_PR_URL": target["target_pr_url"],
        "TARGET_REPO_PATH": target["target_repo_path"],
        "AI_PROVIDER": AI_PROVIDER,
        "TERMS_PATH": TERMS_PATH,
        "TIDB_CLOUD_ABSOLUTE_LINK_PREFIX": TIDB_CLOUD_ABSOLUTE_LINK_PREFIX,
        "SKIP_TRANSLATING_CLOUD_DOCS_TO_ZH": str(SKIP_TRANSLATING_CLOUD_DOCS_TO_ZH).lower(),
        "CLOUD_FOLDER_NAME": CLOUD_FOLDER_NAME,
        "SKIP_TRANSLATING_AI_DOCS_TO_ZH": str(SKIP_TRANSLATING_AI_DOCS_TO_ZH).lower(),
        "AI_DOCS_FOLDER_NAME": AI_DOCS_FOLDER_NAME,
        "SKIP_GIT_ADD": str(SKIP_GIT_ADD).lower(),
    }

    for key, value in required.items():
        os.environ[key] = value

    from main_workflow import main as workflow_main

    return workflow_main()


if __name__ == "__main__":
    raise SystemExit(main())
