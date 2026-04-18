"""
Local entry point for commit-based sync verification.

Fill in the configuration below, then run:

    python commit_sync_workflow_local.py
"""

import os
import sys


TEST_OPTION = "cloud"  # Options: "ai", "cloud"


COMMON_CONFIG = {
    "SOURCE_BRANCH": "release-8.5",
    "AI_PROVIDER": "deepseek",  # Options: "deepseek", "gemini", "openai", "azure"
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
        "SOURCE_BASE_REF": "b7469123c65aa8409bf754e4a7909e16d8ed3082",
        "SOURCE_HEAD_REF": "9c3dec15e4e65344c0e60ec3e6752f11e8036d34",
        # Optional scope filters.
        "SOURCE_FOLDER": "ai",
        # Specify this field if you only want to translate specific files. Separate multiple files with commas.
        "SOURCE_FILES": "ai/integrations/vector-search-integrate-with-django-orm.md",
        "TARGET_REPO_PATH": "/Users/grcai/Documents/GitHub/docs-cn",
        "SOURCE_REPO_PATH": "",
        "TARGET_REF": "",
        "PREFER_LOCAL_TARGET_FOR_READ": False,
        "TERMS_PATH": "/Users/grcai/Documents/GitHub/docs/resources/terms.md",
        "SKIP_TRANSLATING_CLOUD_DOCS_TO_ZH": True,
        "SKIP_TRANSLATING_AI_DOCS_TO_ZH": False,
    },
    "cloud": {
        "SOURCE_REPO": "pingcap/docs",
        "TARGET_REPO": "pingcap/docs",
        # Set SOURCE_BASE_REF from latest_translation_commit.json and SOURCE_HEAD_REF from release-8.5 HEAD.
        "SOURCE_BASE_REF": "0205ededf901476ea31dcd603f21ab9c9bed3f0d",
        "SOURCE_HEAD_REF": "68fed3b6508cb56dc77800008c1d13ef40b1a8a6", # #ecd31cc2c25ad6715af68d05794da89265cfe4d8
        # Keep SOURCE_FOLDER empty. Put resolved Cloud TOC-scoped files here.
        "SOURCE_FOLDER": "", ## Leave this field empty when translating cloud docs.
        "SOURCE_FILES": "system-variables.md",
        "AUTO_RESOLVE_CLOUD_SOURCE_FILES": True,
        "CLOUD_TOC_FILES": (
            "TOC-tidb-cloud.md,"
            "TOC-tidb-cloud-starter.md,"
            "TOC-tidb-cloud-essential.md,"
            "TOC-tidb-cloud-releases.md"
        ),
        # SOURCE_REPO_PATH should be a local checkout/worktree of pingcap/docs release-8.5.
        "SOURCE_REPO_PATH": "/Users/grcai/Documents/GitHub/docs",
        # TARGET_REPO_PATH should be a local checkout/worktree of pingcap/docs i18n-zh-release-8.5.
        "TARGET_REPO_PATH": "/Users/grcai/Documents/GitHub/docs",
        "TARGET_REF": "i18n-zh-release-8.5",
        "PREFER_LOCAL_TARGET_FOR_READ": True,
        "TERMS_PATH": "/Users/grcai/Documents/GitHub/terms.md",
        "SKIP_TRANSLATING_CLOUD_DOCS_TO_ZH": False,
        "SKIP_TRANSLATING_AI_DOCS_TO_ZH": True,
    },
}


LOCAL_ONLY_CONFIG_KEYS = {
    "AUTO_RESOLVE_CLOUD_SOURCE_FILES",
    "CLOUD_TOC_FILES",
}


def build_config(test_option):
    if test_option not in TEST_CONFIGS:
        print(f"❌ Unknown TEST_OPTION: {test_option}")
        print(f"   Options: {', '.join(sorted(TEST_CONFIGS))}")
        return None
    return {**COMMON_CONFIG, **TEST_CONFIGS[test_option]}


def resolve_cloud_source_files_for_local(config):
    if TEST_OPTION != "cloud" or not config.get("AUTO_RESOLVE_CLOUD_SOURCE_FILES"):
        return config, True

    from resolve_cloud_source_files import (
        build_allowed_files,
        list_changed_files,
        parse_list,
        resolve_source_files,
    )

    toc_files = parse_list(config.get("CLOUD_TOC_FILES", ""))
    if not toc_files:
        print("❌ CLOUD_TOC_FILES is empty.")
        return config, None

    input_file_names = config.get("SOURCE_FILES", "")
    source_repo_path = config.get("SOURCE_REPO_PATH", "")
    base_ref = config.get("SOURCE_BASE_REF", "")
    head_ref = config.get("SOURCE_HEAD_REF", "")

    try:
        allowed = build_allowed_files(source_repo_path, toc_files)
        changed_rows = [] if input_file_names.strip() else list_changed_files(source_repo_path, base_ref, head_ref)
        resolved = resolve_source_files(
            allowed,
            input_file_names=input_file_names,
            changed_rows=changed_rows,
        )
    except Exception as e:
        print(f"❌ Failed to resolve Cloud source files: {e}")
        return config, None

    if not resolved:
        print("ℹ️  No Cloud TOC-scoped source changes detected. Nothing to translate.")
        return config, False

    updated = dict(config)
    updated["SOURCE_FILES"] = ",".join(resolved)
    print(f"📄 Resolved Cloud source files: {updated['SOURCE_FILES']}")
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

    config, should_run = resolve_cloud_source_files_for_local(config)
    if should_run is None:
        return 1
    if not should_run:
        return 0

    if not os.getenv("GITHUB_TOKEN"):
        print("❌ GITHUB_TOKEN is not set in the environment.")
        return 1

    required = stringify_env_config(config)

    for key, value in required.items():
        os.environ[key] = value

    from commit_sync_workflow import main as commit_main

    return commit_main()


if __name__ == "__main__":
    raise SystemExit(main())
