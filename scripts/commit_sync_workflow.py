"""
Main entry point for commit-diff-based sync in GitHub Actions.

This workflow keeps the existing PR-based pipeline untouched and adds a
parallel entrypoint for translation based on an explicit commit compare range.
"""

import os

from github import Auth, Github

SOURCE_REPO = os.getenv("SOURCE_REPO")
TARGET_REPO = os.getenv("TARGET_REPO")
SOURCE_BRANCH = os.getenv("SOURCE_BRANCH", "master")
SOURCE_BASE_REF = os.getenv("SOURCE_BASE_REF", "")
SOURCE_HEAD_REF = os.getenv("SOURCE_HEAD_REF", "")
SOURCE_FOLDER = os.getenv("SOURCE_FOLDER", "")
SOURCE_FILES = os.getenv("SOURCE_FILES", "")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
AI_PROVIDER = os.getenv("AI_PROVIDER", "deepseek")
TARGET_REPO_PATH = os.getenv("TARGET_REPO_PATH")
TERMS_PATH = os.getenv("TERMS_PATH", "")
TIDB_CLOUD_ABSOLUTE_LINK_PREFIX = os.getenv(
    "TIDB_CLOUD_ABSOLUTE_LINK_PREFIX",
    "https://docs.pingcap.com/tidbcloud/",
)
os.environ.setdefault(
    "TIDB_CLOUD_ABSOLUTE_LINK_PREFIX",
    TIDB_CLOUD_ABSOLUTE_LINK_PREFIX,
)

from ai_client import UnifiedAIClient, thread_safe_print
from diff_analyzer import (
    build_commit_diff_context,
    build_diff_text,
    infer_language_direction,
    analyze_source_changes,
)
from file_adder import process_added_files
from file_deleter import process_deleted_files
from glossary import create_glossary_matcher, load_glossary
from image_processor import process_all_images
from keword_processor import process_keyword_files
from log_sanitizer import sanitize_exception_message
from main_workflow import (
    AI_DOCS_FOLDER_NAME,
    CLOUD_FOLDER_NAME,
    IGNORE_FILES,
    MAX_NON_SYSTEM_SECTIONS_FOR_AI,
    SKIP_TRANSLATING_AI_DOCS_TO_ZH,
    SKIP_TRANSLATING_CLOUD_DOCS_TO_ZH,
    SPECIAL_FILES,
    clean_temp_output_dir,
    determine_file_processing_type,
    extract_file_diff_from_pr,
    git_add_changes,
    process_regular_modified_file,
)
from toc_processor import process_toc_files


def get_commit_repo_config():
    """Build a minimal repo config for commit-driven sync."""
    source_language, target_language = infer_language_direction(SOURCE_REPO, TARGET_REPO)
    return {
        "source_repo": SOURCE_REPO,
        "target_repo": TARGET_REPO,
        "target_local_path": TARGET_REPO_PATH,
        "prefer_local_target_for_read": False,
        "source_language": source_language,
        "target_language": target_language,
    }


def get_commit_repo_configs():
    """Return repo configs keyed by source repo for analyzer compatibility."""
    repo_config = get_commit_repo_config()
    return {
        SOURCE_REPO: repo_config.copy(),
    }


def normalize_source_files(source_files, source_folder):
    """Normalize comma-separated source file filters relative to the optional folder.

    When SOURCE_FOLDER is set, bare file paths are treated as relative to that folder.
    """
    normalized = set()
    folder_prefix = source_folder.strip("/").strip()

    for item in source_files.split(","):
        rel = item.strip()
        if not rel:
            continue
        rel = rel.lstrip("/")
        if folder_prefix and not rel.startswith(folder_prefix + "/"):
            rel = f"{folder_prefix}/{rel}"
        normalized.add(rel)

    return normalized


def filter_changed_files(changed_files, source_folder="", source_files=""):
    """Filter changed files by folder and/or explicit file list.

    Renames are matched against both the current filename and previous_filename so
    moved files stay in scope for translation and cleanup.
    """
    folder_prefix = source_folder.strip("/").strip()
    folder_prefix = f"{folder_prefix}/" if folder_prefix else ""
    normalized_files = normalize_source_files(source_files, source_folder)

    def candidate_paths(file):
        paths = [file.filename]
        if getattr(file, "previous_filename", None):
            paths.append(file.previous_filename)
        return [path for path in paths if path]

    def matches(file):
        paths = candidate_paths(file)

        if folder_prefix and not any(
            path.startswith(folder_prefix) or path == folder_prefix.rstrip("/")
            for path in paths
        ):
            return False

        if normalized_files and not any(path in normalized_files for path in paths):
            return False

        return True

    return [file for file in changed_files if matches(file)]


def source_filters_explicitly_include(folder_name):
    """Return True when commit-mode filters explicitly target a folder."""
    normalized_folder = folder_name.strip("/").strip()
    source_folder = SOURCE_FOLDER.strip("/").strip()

    if source_folder == normalized_folder:
        return True

    normalized_files = normalize_source_files(SOURCE_FILES, SOURCE_FOLDER)
    return any(
        path == normalized_folder or path.startswith(normalized_folder + "/")
        for path in normalized_files
    )


def build_exclude_folders(repo_config):
    """Build the early-exclusion folder list using existing env-driven behavior."""
    exclude_folders = []
    if SKIP_TRANSLATING_CLOUD_DOCS_TO_ZH and repo_config.get("target_language") == "Chinese":
        if source_filters_explicitly_include(CLOUD_FOLDER_NAME):
            print(f"ℹ️  Explicit source filters include '{CLOUD_FOLDER_NAME}', skipping the default exclusion for commit sync.")
        else:
            exclude_folders.append(CLOUD_FOLDER_NAME)
    if SKIP_TRANSLATING_AI_DOCS_TO_ZH and repo_config.get("target_language") == "Chinese":
        if source_filters_explicitly_include(AI_DOCS_FOLDER_NAME):
            print(f"ℹ️  Explicit source filters include '{AI_DOCS_FOLDER_NAME}', skipping the default exclusion for commit sync.")
        else:
            exclude_folders.append(AI_DOCS_FOLDER_NAME)
    return exclude_folders


def resolve_compare_range():
    """Resolve the explicit source compare range from environment variables."""
    base_ref = SOURCE_BASE_REF.strip()
    head_ref = SOURCE_HEAD_REF.strip()

    if not base_ref or not head_ref:
        raise ValueError(
            "SOURCE_BASE_REF and SOURCE_HEAD_REF must both be set for commit-based sync."
        )

    return base_ref, head_ref


def main():
    """Run the commit-based translation workflow."""
    if not all([SOURCE_REPO, TARGET_REPO, GITHUB_TOKEN, TARGET_REPO_PATH, SOURCE_BASE_REF.strip(), SOURCE_HEAD_REF.strip()]):
        print("❌ Missing required environment variables:")
        print(f"   SOURCE_REPO: {SOURCE_REPO}")
        print(f"   TARGET_REPO: {TARGET_REPO}")
        print(f"   GITHUB_TOKEN: {'Set' if GITHUB_TOKEN else 'Not set'}")
        print(f"   TARGET_REPO_PATH: {TARGET_REPO_PATH}")
        print(f"   SOURCE_BASE_REF: {SOURCE_BASE_REF or '(empty)'}")
        print(f"   SOURCE_HEAD_REF: {SOURCE_HEAD_REF or '(empty)'}")
        return 1

    print("🔧 Auto Commit Sync Tool")
    print(f"📍 Source Repo: {SOURCE_REPO}")
    print(f"📍 Target Repo: {TARGET_REPO}")
    print(f"🌿 Source Branch: {SOURCE_BRANCH}")
    print(f"🧭 Compare Range: {SOURCE_BASE_REF}...{SOURCE_HEAD_REF}")
    print(f"📁 Source Folder Filter: {SOURCE_FOLDER or '(none)'}")
    print(f"📄 Source File Filter: {SOURCE_FILES or '(none)'}")
    print(f"📁 Target Repo Path: {TARGET_REPO_PATH}")
    print(f"🤖 AI Provider: {AI_PROVIDER}")

    clean_temp_output_dir()

    auth = Auth.Token(GITHUB_TOKEN)
    github_client = Github(auth=auth)
    repo_config = get_commit_repo_config()
    repo_configs = get_commit_repo_configs()

    try:
        ai_client = UnifiedAIClient(provider=AI_PROVIDER)
        thread_safe_print(f"🤖 AI Provider: {AI_PROVIDER.upper()} ({ai_client.model})")
    except Exception as e:
        thread_safe_print(f"❌ Failed to initialize AI client: {sanitize_exception_message(e)}")
        return 1

    terms_path = TERMS_PATH
    if not terms_path and TARGET_REPO_PATH:
        candidate = os.path.join(TARGET_REPO_PATH, "resources", "terms.md")
        if os.path.exists(candidate):
            terms_path = candidate
    print(f"\n📚 Loading glossary from: {terms_path or '(not configured)'}")
    glossary = load_glossary(terms_path) if terms_path else []
    glossary_matcher = create_glossary_matcher(glossary)

    try:
        base_ref, head_ref = resolve_compare_range()
    except Exception as e:
        print(f"❌ Failed to resolve compare range: {sanitize_exception_message(e)}")
        return 1

    print(f"\n🚀 Starting commit-based sync for: {base_ref}...{head_ref}")

    try:
        diff_context = build_commit_diff_context(
            SOURCE_REPO,
            TARGET_REPO,
            base_ref,
            head_ref,
            github_client,
            repo_configs,
        )
        diff_context["repo_config"] = repo_config
    except Exception as e:
        print(f"❌ Failed to build commit diff context: {sanitize_exception_message(e)}")
        return 1

    filtered_changed_files = filter_changed_files(
        diff_context["changed_files"],
        source_folder=SOURCE_FOLDER,
        source_files=SOURCE_FILES,
    )
    diff_context["changed_files"] = filtered_changed_files

    print(f"📊 Filtered changed files: {len(filtered_changed_files)}")
    if not filtered_changed_files:
        print("ℹ️  No matching source changes found after folder/file filtering.")
        return 0

    pr_diff = build_diff_text(filtered_changed_files)
    if pr_diff:
        print(f"✅ Built diff text: {len(pr_diff)} characters")
    else:
        print("ℹ️  No markdown patch text found; continuing in case there are file-level or image changes.")

    exclude_folders = build_exclude_folders(repo_config)

    try:
        added_sections, modified_sections, deleted_sections, added_files, deleted_files, toc_files, keyword_files, added_images, modified_images, deleted_images = analyze_source_changes(
            diff_context,
            github_client,
            special_files=SPECIAL_FILES,
            ignore_files=IGNORE_FILES,
            repo_configs=repo_configs,
            max_non_system_sections=MAX_NON_SYSTEM_SECTIONS_FOR_AI,
            pr_diff=pr_diff,
            exclude_folders=exclude_folders,
        )
    except Exception as e:
        print(f"❌ Source diff analysis failed: {sanitize_exception_message(e)}")
        return 1

    if deleted_files:
        print(f"\n🗑️  Processing {len(deleted_files)} deleted files...")
        process_deleted_files(deleted_files, github_client, repo_config)
        git_add_changes(TARGET_REPO_PATH)

    if added_files:
        print(f"\n📄 Processing {len(added_files)} added files...")
        for file_path, file_content in added_files.items():
            process_added_files(
                {file_path: file_content},
                diff_context,
                github_client,
                ai_client,
                repo_config,
                glossary_matcher=glossary_matcher,
            )
            git_add_changes(TARGET_REPO_PATH)

    if toc_files:
        print(f"\n📋 Processing {len(toc_files)} TOC files...")
        process_toc_files(toc_files, diff_context, github_client, ai_client, repo_config)
        git_add_changes(TARGET_REPO_PATH)

    if keyword_files:
        print(f"\n📋 Processing {len(keyword_files)} keyword files...")
        keyword_success = process_keyword_files(keyword_files, diff_context, github_client, ai_client, repo_config)
        if not keyword_success:
            print("   ❌ Keyword files processing failed, exiting workflow")
            return 1
        git_add_changes(TARGET_REPO_PATH)

    if modified_sections:
        print(f"\n📝 Processing {len(modified_sections)} modified files...")
        for source_file_path, file_sections in modified_sections.items():
            print(f"\n📄 Processing modified file: {source_file_path}")

            file_specific_diff = extract_file_diff_from_pr(pr_diff, source_file_path) if pr_diff else ""
            if not file_specific_diff:
                if pr_diff:
                    print(f"   ⚠️  No diff found for {source_file_path}, skipping...")
                else:
                    print(f"   ⚠️  No markdown patch text available for {source_file_path}, skipping section-level translation.")
                continue
            print(f"   📊 File-specific diff: {len(file_specific_diff)} chars")

            file_type = determine_file_processing_type(source_file_path, file_sections, SPECIAL_FILES, IGNORE_FILES)
            print(f"   🔍 File processing type: {file_type}")

            if file_type == "special_file_toc":
                print("   ⏭️  Special file already processed in TOC step, skipping...")
                continue
            if file_type == "special_file_keyword":
                print("   ⏭️  Keyword file already processed in keyword step, skipping...")
                continue
            if file_type != "regular_modified":
                print(f"   ⚠️  Unknown file processing type: {file_type}, skipping...")
                continue

            success = process_regular_modified_file(
                source_file_path,
                file_sections,
                file_specific_diff,
                diff_context,
                github_client,
                ai_client,
                repo_config,
                MAX_NON_SYSTEM_SECTIONS_FOR_AI,
                glossary_matcher=glossary_matcher,
            )
            if success:
                print(f"   ✅ Successfully processed {source_file_path}")
                git_add_changes(TARGET_REPO_PATH)
            else:
                print(f"   ❌ Failed to process {source_file_path}")
                return 1

    if added_images or modified_images or deleted_images:
        print("\n🖼️  Processing images...")
        process_all_images(
            added_images,
            modified_images,
            deleted_images,
            diff_context,
            github_client,
            repo_config,
        )
        git_add_changes(TARGET_REPO_PATH)

    print("\n" + "=" * 80)
    print("📊 Final Summary:")
    print("=" * 80)
    print(f"   📄 Added files: {len(added_files)} processed")
    print(f"   🗑️  Deleted files: {len(deleted_files)} processed")
    print(f"   📋 TOC files: {len(toc_files)} processed")
    print(f"   📋 Keyword files: {len(keyword_files)} processed")
    print(f"   📝 Modified files: {len(modified_sections)} processed")
    print(f"   🖼️  Added images: {len(added_images)} processed")
    print(f"   🖼️  Modified images: {len(modified_images)} processed")
    print(f"   🖼️  Deleted images: {len(deleted_images)} processed")
    print("=" * 80)
    print("🎉 The commit-based sync workflow completed successfully!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
