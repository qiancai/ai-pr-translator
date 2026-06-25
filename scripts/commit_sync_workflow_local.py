"""
Local entry point for commit-based sync verification.

Fill in the configuration below, then run:

    python commit_sync_workflow_local.py
"""

import os
import sys


TEST_OPTION = "cloud"  # Options: "ai", "cloud", "ja"

# Options: "incremental" for commit diff translation, or "full" for complete
# SOURCE_FILES translation from SOURCE_HEAD_REF using the file-added flow.
SOURCE_FILES_TRANSLATION_MODE = "incremental"
# Options: "manual" to add/update per-file Corresponding EN commit markers,
# or "scheduled" to remove existing markers after successful translation.
COMMIT_SYNC_RUN_TYPE = "scheduled"
# Options: "Yes" to ignore RelatedResources sections containing ResourceCard,
# or "No" to keep those sections in the normal commit-mode translation flow.
IGNORE_RESOURCE_CARD_SECTION = "Yes"


COMMON_CONFIG = {
    "SOURCE_BRANCH": "release-8.5",
    "AI_PROVIDER": "azure",  # Options: "deepseek", "gemini", "openai", "azure"
    "SOURCE_FILES_TRANSLATION_MODE": SOURCE_FILES_TRANSLATION_MODE,
    "COMMIT_SYNC_RUN_TYPE": COMMIT_SYNC_RUN_TYPE,
    "IGNORE_RESOURCE_CARD_SECTION": IGNORE_RESOURCE_CARD_SECTION,
    "TIDB_CLOUD_ABSOLUTE_LINK_PREFIX": "https://docs.pingcap.com/tidbcloud/",
    # Keep local verification going after per-file failures so successful outputs can be reviewed.
    "FAIL_ON_TRANSLATION_ERROR": False,
    # Local verification should leave translated files unstaged for easy review.
    "SKIP_GIT_ADD": True,
}


TEST_CONFIGS = {
    "ai": {
        "SOURCE_REPO": "pingcap/docs",
        "TARGET_REPO": "pingcap/docs-cn",
        # Set these two refs explicitly for local verification.
        "SOURCE_BASE_REF": "2eaf0b7cd9c870d6f25c0dea7c7e1bb64ba2572b",
        "SOURCE_HEAD_REF": "6376b9086871957f4809711c83e021a01b0a8f5d",
        # Optional scope filters.
        "SOURCE_FOLDER": "ai",
        # Specify this field if you only want to translate specific files. Separate multiple files with commas.
        "SOURCE_FILES": "ai/integrations/vector-search-integration-overview.md",
        "TARGET_REPO_PATH": "/Users/grcai/Documents/GitHub/docs-cn",
        "SOURCE_REPO_PATH": "",
        "TARGET_REF": "",
        "PREFER_LOCAL_TARGET_FOR_READ": False,
        "TERMS_PATH": "/Users/grcai/Documents/GitHub/terms.md",
    },
    "cloud": {
        "SOURCE_REPO": "pingcap/docs",
        "TARGET_REPO": "pingcap/docs",
        # Set SOURCE_BASE_REF from latest_translation_commit.json and SOURCE_HEAD_REF from release-8.5 HEAD.
        "SOURCE_BASE_REF": "6376b9086871957f4809711c83e021a01b0a8f5d",
        "SOURCE_HEAD_REF": "6a3301ed41e2d6edcc5f6d76f06fffdfe029acd5",#ecd31cc2c25ad6715af68d05794da89265cfe4d8
        # Keep SOURCE_FOLDER empty. Put resolved Cloud TOC-scoped files here.
        "SOURCE_FOLDER": "", ## Leave this field empty when translating cloud docs.
        "SOURCE_FILES": "coprocessor-cache.md,tiflash/tiflash-results-materialization.md",
        "AUTO_RESOLVE_TOC_SOURCE_FILES": True,
        "CLOUD_TOC_FILES": (
            "TOC-tidb-cloud.md,"
            "TOC-tidb-cloud-starter.md,"
            "TOC-tidb-cloud-essential.md,"
            "TOC-tidb-cloud-releases.md,"
            "TOC-tidb-cloud-premium.md"
        ),
        "CLOUD_INDEX_FILES": (
            "tidb-cloud/dedicated/_index.md,"
            "tidb-cloud/essential/_index.md,"
            "tidb-cloud/premium/_index.md,"
            "tidb-cloud/releases/_index.md,"
            "tidb-cloud/starter/_index.md"
        ),
        # SOURCE_REPO_PATH should be a local checkout/worktree of pingcap/docs release-8.5.
        "SOURCE_REPO_PATH": "/Users/grcai/Documents/GitHub/docs",
        # TARGET_REPO_PATH should be a local checkout/worktree of pingcap/docs i18n-zh-release-8.5.
        "TARGET_REPO_PATH": "/Users/grcai/Documents/GitHub/docs",
        "TARGET_REF": "i18n-zh-release-8.5",
        "PREFER_LOCAL_TARGET_FOR_READ": True,
        "TERMS_PATH": "/Users/grcai/Documents/GitHub/terms.md",
    },
    "ja": {
        "SOURCE_REPO": "pingcap/docs",
        "TARGET_REPO": "pingcap/docs",
        "SOURCE_LANGUAGE": "English",
        "TARGET_LANGUAGE": "Japanese",
        # Set SOURCE_BASE_REF from latest_translation_commit.json on i18n-ja-release-8.5
        # and SOURCE_HEAD_REF from release-8.5 HEAD.
        "SOURCE_BASE_REF": "2eaf0b7cd9c870d6f25c0dea7c7e1bb64ba2572b",
        "SOURCE_HEAD_REF": "6376b9086871957f4809711c83e021a01b0a8f5d",
        "SOURCE_FOLDER": "",
        "SOURCE_FILES": "ai/integrations/vector-search-integration-overview.md",
        "AUTO_RESOLVE_TOC_SOURCE_FILES": True,
        "TOC_FILES": (
            "TOC.md,"
            "TOC-ai.md,"
            "TOC-api.md,"
            "TOC-best-practices.md,"
            "TOC-develop.md,"
            "TOC-tidb-releases.md,"
            "TOC-tidb-cloud.md,"
            "TOC-tidb-cloud-starter.md,"
            "TOC-tidb-cloud-essential.md,"
            "TOC-tidb-cloud-releases.md,"
            "TOC-tidb-cloud-premium.md"
        ),
        # SOURCE_REPO_PATH should be a local checkout/worktree of pingcap/docs release-8.5.
        "SOURCE_REPO_PATH": "/Users/grcai/Documents/GitHub/docs",
        # TARGET_REPO_PATH should be a local checkout/worktree of pingcap/docs i18n-ja-release-8.5.
        "TARGET_REPO_PATH": "/Users/grcai/Documents/GitHub/docs",
        "TARGET_REF": "i18n-ja-release-8.5",
        "PREFER_LOCAL_TARGET_FOR_READ": True,
        "TERMS_PATH": "/Users/grcai/Documents/GitHub/terms-en-ja.md",
    },
}


LOCAL_ONLY_CONFIG_KEYS = {
    "AUTO_RESOLVE_TOC_SOURCE_FILES",
    "CLOUD_INDEX_FILES",
}


def build_config(test_option):
    if test_option not in TEST_CONFIGS:
        print(f"❌ Unknown TEST_OPTION: {test_option}")
        print(f"   Options: {', '.join(sorted(TEST_CONFIGS))}")
        return None
    return {**COMMON_CONFIG, **TEST_CONFIGS[test_option]}


def resolve_toc_source_files_for_local(config):
    if not config.get("AUTO_RESOLVE_TOC_SOURCE_FILES"):
        return config, True

    from resolve_cloud_source_files import (
        build_allowed_files,
        collect_toc_scope_added_files,
        list_changed_files,
        parse_list,
        resolve_source_files,
    )

    toc_files = parse_list(config.get("TOC_FILES") or config.get("CLOUD_TOC_FILES", ""))
    if not toc_files:
        print("❌ TOC_FILES / CLOUD_TOC_FILES is empty.")
        return config, None

    input_file_names = config.get("SOURCE_FILES", "")
    cloud_index_files = parse_list(config.get("INDEX_FILES") or config.get("CLOUD_INDEX_FILES", ""))
    source_repo_path = config.get("SOURCE_REPO_PATH", "")
    base_ref = config.get("SOURCE_BASE_REF", "")
    head_ref = config.get("SOURCE_HEAD_REF", "")

    try:
        allowed = build_allowed_files(source_repo_path, toc_files, extra_files=cloud_index_files)
        if input_file_names.strip():
            changed_rows = []
            toc_scope_added_files = set()
        else:
            changed_rows = list_changed_files(source_repo_path, base_ref, head_ref)
            toc_scope_added_files = collect_toc_scope_added_files(
                source_repo_path,
                toc_files,
                base_ref,
                head_ref,
            )
        resolved = resolve_source_files(
            allowed,
            input_file_names=input_file_names,
            changed_rows=changed_rows,
            toc_scope_added_files=toc_scope_added_files,
        )
    except Exception as e:
        print(f"❌ Failed to resolve TOC-scoped source files: {e}")
        return config, None

    if not resolved:
        print("ℹ️  No TOC-scoped source changes detected. Nothing to translate.")
        return config, False

    updated = dict(config)
    updated["SOURCE_FILES"] = ",".join(resolved)
    print(f"📄 Resolved TOC-scoped source files: {updated['SOURCE_FILES']}")
    return updated, True


def stringify_env_config(config):
    return {
        key: str(value).lower() if isinstance(value, bool) else str(value)
        for key, value in config.items()
        if key not in LOCAL_ONLY_CONFIG_KEYS
    }


def main():
    config = build_config(TEST_OPTION)
    if config is None:
        return 1

    print(f"🧪 Local commit sync test option: {TEST_OPTION}")

    missing_refs = [name for name in ("SOURCE_BASE_REF", "SOURCE_HEAD_REF") if not str(config[name]).strip()]
    if missing_refs:
        print("❌ Please set explicit commit refs before local verification:")
        for name in missing_refs:
            print(f"   {name} is empty")
        return 1

    config, should_run = resolve_toc_source_files_for_local(config)
    if should_run is None:
        return 1
    if not should_run:
        return 0

    if not os.getenv("GITHUB_TOKEN"):
        print("❌ GITHUB_TOKEN is not set in the environment.")
        return 1

    if config["AI_PROVIDER"] == "azure":
        local_azure_openai_key = os.getenv("AZURE_OPENAI_KEY") or os.getenv("TRANS_KEY", "")
        local_azure_openai_base_url = (
            os.getenv("AZURE_OPENAI_BASE_URL") or os.getenv("TRANS_URL", "")
        )

        if not local_azure_openai_key:
            print("❌ Neither AZURE_OPENAI_KEY nor TRANS_KEY is set for local Azure runs.")
            return 1
        if not local_azure_openai_base_url:
            print(
                "❌ Neither AZURE_OPENAI_BASE_URL nor TRANS_URL is set for local Azure runs."
            )
            return 1
        os.environ["AZURE_OPENAI_KEY"] = local_azure_openai_key
        os.environ["AZURE_OPENAI_BASE_URL"] = local_azure_openai_base_url
        os.environ["OPENAI_BASE_URL"] = local_azure_openai_base_url

    required = stringify_env_config(config)

    for key, value in required.items():
        os.environ[key] = value

    from commit_sync_workflow import main as commit_main

    return commit_main()


if __name__ == "__main__":
    raise SystemExit(main())
