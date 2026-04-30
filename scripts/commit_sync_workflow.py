"""
Main entry point for commit-diff-based sync in GitHub Actions.

This workflow keeps the existing PR-based pipeline untouched and adds a
parallel entrypoint for translation based on an explicit commit compare range.
"""

import json
import os

from github import Auth, Github

SOURCE_REPO = os.getenv("SOURCE_REPO")
TARGET_REPO = os.getenv("TARGET_REPO")
SOURCE_BRANCH = os.getenv("SOURCE_BRANCH", "master")
SOURCE_BASE_REF = os.getenv("SOURCE_BASE_REF", "")
SOURCE_HEAD_REF = os.getenv("SOURCE_HEAD_REF", "")
SOURCE_FOLDER = os.getenv("SOURCE_FOLDER", "")
SOURCE_FILES = os.getenv("SOURCE_FILES", "")
SOURCE_FILES_TRANSLATION_MODE = os.getenv("SOURCE_FILES_TRANSLATION_MODE", "incremental")
CLOUD_TOC_FILES = os.getenv("CLOUD_TOC_FILES", "")
SOURCE_REPO_PATH = os.getenv("SOURCE_REPO_PATH", "")
TARGET_REF = os.getenv("TARGET_REF", "")
PREFER_LOCAL_TARGET_FOR_READ = os.getenv("PREFER_LOCAL_TARGET_FOR_READ", "false").lower() == "true"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
AI_PROVIDER = os.getenv("AI_PROVIDER", "deepseek")
TARGET_REPO_PATH = os.getenv("TARGET_REPO_PATH")
TERMS_PATH = os.getenv("TERMS_PATH", "")
FAIL_ON_TRANSLATION_ERROR = os.getenv("FAIL_ON_TRANSLATION_ERROR", "false").lower() == "true"
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
    build_local_commit_diff_context,
    build_diff_text,
    get_source_file_content,
    infer_language_direction,
    analyze_source_changes,
)
from file_adder import process_added_files
from file_deleter import process_deleted_files
from glossary import create_glossary_matcher, load_glossary
from image_processor import process_all_images
from keword_processor import process_keyword_file
from log_sanitizer import sanitize_exception_message
from parallel_file_processor import (
    make_file_task,
    make_task_result,
    run_file_tasks,
    should_parallelize_file_processing,
)
from resolve_cloud_source_files import extract_markdown_doc_links
from main_workflow import (
    MAX_NON_SYSTEM_SECTIONS_FOR_AI,
    SPECIAL_FILES,
    clean_temp_output_dir,
    determine_file_processing_type,
    extract_file_diff_from_pr,
    git_add_changes,
    git_add_successful_task_changes,
    process_regular_modified_file,
)
from toc_processor import process_toc_file
from workflow_ignore_config import load_workflow_ignore_config

WORKFLOW_IGNORE_CONFIG = load_workflow_ignore_config()
COMMIT_BASED_MODE_IGNORE_FILES = WORKFLOW_IGNORE_CONFIG["COMMIT_BASED_MODE_IGNORE_FILES"]
COMMIT_BASED_MODE_IGNORE_FOLDERS = WORKFLOW_IGNORE_CONFIG["COMMIT_BASED_MODE_IGNORE_FOLDERS"]
SOURCE_FILES_TRANSLATION_MODE_ALIASES = {
    "incremental": "incremental",
    "diff": "incremental",
    "commit-diff": "incremental",
    "commit_diff": "incremental",
    "full": "full",
    "all": "full",
    "full-translation": "full",
    "full_translation": "full",
}


class TranslationStats:
    """Track per-file translation outcomes without stopping later files."""

    def __init__(self):
        self.total = 0
        self.succeeded = []
        self.failed = []
        self.skipped = []

    def mark_success(self, file_path):
        self.total += 1
        self.succeeded.append(file_path)

    def mark_failure(self, file_path, reason):
        self.total += 1
        self.failed.append((file_path, reason))

    def mark_skipped(self, file_path, reason):
        self.skipped.append((file_path, reason))

    def print_summary(self):
        thread_safe_print("\n📚 Translation attempt summary:")
        thread_safe_print(f"   📄 Files attempted for translation: {self.total}")
        thread_safe_print(f"   ✅ Successfully translated: {len(self.succeeded)}")
        thread_safe_print(f"   ❌ Failed to translate: {len(self.failed)}")

        if self.failed:
            thread_safe_print("   ❌ Failed files:")
            for file_path, reason in self.failed:
                thread_safe_print(f"      - {file_path}: {reason}")

        if self.skipped:
            thread_safe_print(f"   ⏭️  Skipped files: {len(self.skipped)}")
            for file_path, reason in self.skipped:
                thread_safe_print(f"      - {file_path}: {reason}")

    def write_failure_report(self, output_dir):
        """Write per-file failures for workflow PR descriptions."""
        os.makedirs(output_dir, exist_ok=True)
        markdown_path = os.path.join(output_dir, "translation-failures.md")
        json_path = os.path.join(output_dir, "translation-failures.json")

        if not self.failed:
            for path in (markdown_path, json_path):
                if os.path.exists(path):
                    os.remove(path)
            return

        with open(markdown_path, "w", encoding="utf-8") as f:
            f.write("### Translation failures\n\n")
            f.write(
                "The following files were not translated automatically and must be handled manually before merging.\n\n"
            )
            for file_path, reason in self.failed:
                f.write(f"- `{file_path}`: {reason}\n")

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(
                [
                    {
                        "file_path": file_path,
                        "reason": reason,
                    }
                    for file_path, reason in self.failed
                ],
                f,
                ensure_ascii=False,
                indent=2,
            )
            f.write("\n")


def _record_translation_task_result(task_result, translation_stats):
    file_path = task_result["file_path"]
    if not task_result["ok"]:
        thread_safe_print(f"   ❌ Failed to process {file_path}: {task_result['error']}")
        translation_stats.mark_failure(file_path, task_result["error"])
        return False

    result = task_result["result"] or {}
    status = result.get("status", "failure")
    reason = result.get("reason", "")

    if status == "success":
        thread_safe_print(f"   ✅ Successfully processed {file_path}")
        translation_stats.mark_success(file_path)
        return True
    if status == "skipped":
        thread_safe_print(f"   ⏭️  Skipped {file_path}: {reason}")
        translation_stats.mark_skipped(file_path, reason)
        return False

    thread_safe_print(f"   ❌ Failed to process {file_path}: {reason}")
    translation_stats.mark_failure(file_path, reason)
    return False


def _process_commit_modified_file(
    source_file_path,
    file_sections,
    pr_diff,
    diff_context,
    github_client,
    ai_client,
    repo_config,
    glossary_matcher,
):
    thread_safe_print(f"\n📄 Processing modified file: {source_file_path}")

    file_specific_diff = extract_file_diff_from_pr(pr_diff, source_file_path) if pr_diff else ""
    if not file_specific_diff:
        if pr_diff:
            return make_task_result("failure", "No file-specific diff found")
        return make_task_result("failure", "No markdown patch text available")
    thread_safe_print(f"   📊 File-specific diff: {len(file_specific_diff)} chars")

    if should_process_modified_file_as_added(source_file_path, repo_config, github_client):
        thread_safe_print(
            f"   🆕 Target file is missing; processing {source_file_path} as a newly added file"
        )
        try:
            source_content = get_source_file_content(
                source_file_path,
                diff_context,
                github_client,
                ref_name="head_ref",
            )
        except Exception as e:
            return make_task_result(
                "failure",
                f"Could not get source file content for added-file fallback: {sanitize_exception_message(e)}",
            )

        success, failure_reasons = process_added_files(
            {source_file_path: source_content},
            diff_context,
            github_client,
            ai_client,
            repo_config,
            glossary_matcher=glossary_matcher,
            return_details=True,
        )
        if success:
            return make_task_result("success")
        return make_task_result(
            "failure",
            failure_reasons.get(source_file_path, "Added-file fallback returned failure"),
        )

    ignore_files = repo_config.get("ignore_files", COMMIT_BASED_MODE_IGNORE_FILES)
    file_type = determine_file_processing_type(source_file_path, file_sections, SPECIAL_FILES, ignore_files)
    thread_safe_print(f"   🔍 File processing type: {file_type}")

    if file_type == "special_file_toc":
        return make_task_result("skipped", "Already handled in TOC step")
    if file_type == "special_file_keyword":
        return make_task_result("skipped", "Already handled in keyword step")
    if file_type != "regular_modified":
        return make_task_result("failure", f"Unknown file processing type: {file_type}")

    success, failure_reason = process_regular_modified_file(
        source_file_path,
        file_sections,
        file_specific_diff,
        diff_context,
        github_client,
        ai_client,
        repo_config,
        MAX_NON_SYSTEM_SECTIONS_FOR_AI,
        glossary_matcher=glossary_matcher,
        return_details=True,
    )
    if success:
        return make_task_result("success")
    return make_task_result(
        "failure",
        failure_reason or "Regular modified file processor returned failure",
    )


def get_commit_repo_config():
    """Build a minimal repo config for commit-driven sync."""
    source_language, target_language = infer_language_direction(SOURCE_REPO, TARGET_REPO)
    return {
        "source_repo": SOURCE_REPO,
        "target_repo": TARGET_REPO,
        "target_local_path": TARGET_REPO_PATH,
        "target_ref": TARGET_REF,
        "prefer_local_target_for_read": PREFER_LOCAL_TARGET_FOR_READ,
        "source_mode": "commit",
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


def parse_comma_separated_list(value):
    return [item.strip() for item in (value or "").split(",") if item.strip()]


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


def normalize_source_files_translation_mode(mode):
    """Normalize SOURCE_FILES_TRANSLATION_MODE into incremental or full."""
    normalized = (mode or "incremental").strip().lower()
    if not normalized:
        normalized = "incremental"

    if normalized not in SOURCE_FILES_TRANSLATION_MODE_ALIASES:
        valid_values = "incremental or full"
        raise ValueError(
            f"SOURCE_FILES_TRANSLATION_MODE must be {valid_values}; got {mode!r}."
        )

    return SOURCE_FILES_TRANSLATION_MODE_ALIASES[normalized]


def get_effective_source_files_translation_mode(mode, source_files):
    """Return the mode that actually applies to this run.

    Full-file translation is intentionally scoped to explicit SOURCE_FILES so a
    folder-only commit sync cannot accidentally rewrite every changed file.
    """
    normalized_mode = normalize_source_files_translation_mode(mode)
    if not source_files.strip():
        return "incremental"
    return normalized_mode


def resolve_full_translation_source_file_paths(source_files, source_folder, changed_files):
    """Resolve explicitly requested SOURCE_FILES to HEAD paths for full translation."""
    requested_paths = normalize_source_files(source_files, source_folder)
    resolved_paths = set()
    matched_requested_paths = set()

    for file in changed_files:
        current_path = getattr(file, "filename", "")
        previous_path = getattr(file, "previous_filename", None)
        candidate_paths = {path for path in (current_path, previous_path) if path}
        matched_paths = requested_paths & candidate_paths
        if not matched_paths:
            continue

        matched_requested_paths.update(matched_paths)
        if getattr(file, "status", "") != "removed" and current_path:
            resolved_paths.add(current_path)

    # Full mode must support explicit files that are not in the compare diff.
    resolved_paths.update(requested_paths - matched_requested_paths)
    return resolved_paths


def collect_source_files_for_full_translation(
    source_file_paths,
    changed_files,
    diff_context,
    github_client,
    ignore_files=None,
):
    """Fetch full HEAD content for selected markdown files."""
    ignore_files = set(ignore_files or [])
    full_translation_files = {}
    failures = {}

    removed_paths = {
        getattr(file, "filename", "")
        for file in changed_files
        if getattr(file, "status", "") == "removed"
    }

    for source_file_path in sorted(source_file_paths):
        if not source_file_path or not source_file_path.endswith(".md"):
            continue
        if source_file_path in ignore_files:
            thread_safe_print(f"   ⏭️  Skipping ignored file for full translation: {source_file_path}")
            continue
        if source_file_path in removed_paths:
            continue

        try:
            full_translation_files[source_file_path] = get_full_translation_source_content(
                source_file_path,
                diff_context,
                github_client,
            )
        except Exception as e:
            failures[source_file_path] = (
                f"Could not get source file content for full translation: "
                f"{sanitize_exception_message(e)}"
            )

    return full_translation_files, failures


def get_local_git_file_content(source_repo_path, ref, source_file_path):
    import subprocess

    result = subprocess.run(
        ["git", "-C", source_repo_path, "show", f"{ref}:{source_file_path}"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        detail = sanitize_local_git_error(
            result.stderr,
            source_repo_path,
        )
        if detail:
            detail = f": {detail}"
        raise RuntimeError(
            f"git show failed for {source_file_path} at source ref {ref}{detail}"
        )
    if result.stderr.strip():
        thread_safe_print(
            f"   ⚠️  git show produced stderr for {source_file_path}; using stdout only."
        )
    return result.stdout


def get_source_ref_content(source_file_path, diff_context, github_client, ref_name):
    """Read source content from the requested source ref."""
    source_repo_path = diff_context.get("source_repo_path")
    if source_repo_path:
        return get_local_git_file_content(
            source_repo_path,
            diff_context[ref_name],
            source_file_path,
        )

    return get_source_file_content(
        source_file_path,
        diff_context,
        github_client,
        ref_name=ref_name,
    )


def get_full_translation_source_content(source_file_path, diff_context, github_client):
    """Read full-translation source content from local HEAD ref when available."""
    return get_source_ref_content(
        source_file_path,
        diff_context,
        github_client,
        "head_ref",
    )


def sanitize_local_git_error(stderr, source_repo_path):
    """Return a safe git error detail without the local checkout path."""
    detail = (stderr or "").strip()
    if source_repo_path:
        detail = detail.replace(source_repo_path, "[SOURCE_REPO_PATH]")
    return sanitize_exception_message(detail)


def remove_incremental_work_for_files(
    file_paths,
    added_sections,
    modified_sections,
    deleted_sections,
    toc_files,
    keyword_files,
):
    """Remove per-diff work queues for files that will be translated in full."""
    for file_path in file_paths:
        added_sections.pop(file_path, None)
        modified_sections.pop(file_path, None)
        deleted_sections.pop(file_path, None)
        toc_files.pop(file_path, None)
        keyword_files.pop(file_path, None)


def apply_source_files_full_translation_mode(
    source_files,
    source_folder,
    filtered_changed_files,
    diff_context,
    github_client,
    commit_ignore_files,
    added_sections,
    modified_sections,
    deleted_sections,
    added_files,
    toc_files,
    keyword_files,
    translation_stats,
):
    """Switch selected SOURCE_FILES from diff-based queues to full-file translation."""
    thread_safe_print(
        "\n📄 SOURCE_FILES_TRANSLATION_MODE=full: translating selected markdown files as complete files."
    )
    source_file_paths = resolve_full_translation_source_file_paths(
        source_files,
        source_folder,
        filtered_changed_files,
    )
    full_translation_files, full_translation_failures = collect_source_files_for_full_translation(
        source_file_paths,
        filtered_changed_files,
        diff_context,
        github_client,
        ignore_files=commit_ignore_files,
    )
    full_translation_file_paths = set(full_translation_files) | set(full_translation_failures)
    remove_incremental_work_for_files(
        full_translation_file_paths,
        added_sections,
        modified_sections,
        deleted_sections,
        toc_files,
        keyword_files,
    )
    added_files.update(full_translation_files)
    for file_path in full_translation_failures:
        added_files.pop(file_path, None)
    for file_path, reason in full_translation_failures.items():
        translation_stats.mark_failure(file_path, reason)
        thread_safe_print(f"   ❌ {file_path}: {reason}")

    return full_translation_file_paths


def collect_toc_scope_added_files_from_snapshots(toc_files):
    """Return links in aggregate head TOCs that were absent from aggregate base TOCs."""
    base_links = set()
    head_links = set()

    for toc_data in (toc_files or {}).values():
        source_base_content = toc_data.get("source_base_content")
        source_head_content = toc_data.get("source_head_content")
        if source_base_content is None or source_head_content is None:
            continue

        base_links.update(extract_markdown_doc_links(source_base_content))
        head_links.update(extract_markdown_doc_links(source_head_content))

    return head_links - base_links


def collect_toc_scope_added_files(toc_files, diff_context=None, github_client=None, toc_file_paths=None):
    """Return Markdown docs newly linked from the aggregate Cloud TOC scope."""
    if not toc_file_paths or diff_context is None:
        return collect_toc_scope_added_files_from_snapshots(toc_files)

    aggregate_toc_files = {}
    changed_toc_files = toc_files or {}

    for toc_file in toc_file_paths:
        if toc_file in changed_toc_files:
            aggregate_toc_files[toc_file] = changed_toc_files[toc_file]
            continue

        try:
            source_base_content = get_source_ref_content(
                toc_file,
                diff_context,
                github_client,
                "base_ref",
            )
        except Exception as e:
            thread_safe_print(
                f"   ⚠️  Could not get base Cloud TOC content for {toc_file}: "
                f"{sanitize_exception_message(e)}"
            )
            source_base_content = ""

        try:
            source_head_content = get_source_ref_content(
                toc_file,
                diff_context,
                github_client,
                "head_ref",
            )
        except Exception as e:
            thread_safe_print(
                f"   ⚠️  Could not get head Cloud TOC content for {toc_file}: "
                f"{sanitize_exception_message(e)}"
            )
            source_head_content = ""

        aggregate_toc_files[toc_file] = {
            "source_base_content": source_base_content,
            "source_head_content": source_head_content,
        }

    return collect_toc_scope_added_files_from_snapshots(aggregate_toc_files)


def get_cloud_toc_file_paths(toc_files):
    configured_toc_files = parse_comma_separated_list(CLOUD_TOC_FILES)
    if configured_toc_files:
        return configured_toc_files

    return sorted((toc_files or {}).keys())


def apply_toc_scope_added_files(
    toc_files,
    diff_context,
    github_client,
    commit_ignore_files,
    added_sections,
    modified_sections,
    deleted_sections,
    added_files,
    keyword_files,
    translation_stats,
):
    """Queue files newly linked from Cloud TOCs for full file-added translation."""
    scope_added_file_paths = collect_toc_scope_added_files(
        toc_files,
        diff_context,
        github_client,
        get_cloud_toc_file_paths(toc_files),
    )
    if not scope_added_file_paths:
        return set()

    thread_safe_print(
        "\n📄 Cloud TOC scope added files: "
        f"{', '.join(sorted(scope_added_file_paths))}"
    )
    scope_added_file_contents, scope_added_failures = collect_source_files_for_full_translation(
        scope_added_file_paths,
        [],
        diff_context,
        github_client,
        ignore_files=commit_ignore_files,
    )
    queued_file_paths = set(scope_added_file_contents) | set(scope_added_failures)

    remove_incremental_work_for_files(
        queued_file_paths,
        added_sections,
        modified_sections,
        deleted_sections,
        # Keep the changed TOC itself queued; only the newly linked doc should
        # switch from diff-based work to full file-added translation.
        {},
        keyword_files,
    )
    added_files.update(scope_added_file_contents)

    for file_path in scope_added_failures:
        added_files.pop(file_path, None)
    for file_path, reason in scope_added_failures.items():
        translation_stats.mark_failure(file_path, reason)
        thread_safe_print(f"   ❌ {file_path}: {reason}")

    return queued_file_paths


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


def get_commit_ignore_files():
    """Return commit-mode ignore files.

    PR-mode TOC ignores are intentionally not inherited here because commit sync
    can be scoped to exactly the files that should be mirrored.
    """
    return list(COMMIT_BASED_MODE_IGNORE_FILES)


def get_commit_ignore_folders():
    """Return commit-mode ignore folders."""
    return list(COMMIT_BASED_MODE_IGNORE_FOLDERS)


def should_process_modified_file_as_added(source_file_path, repo_config, github_client):
    """Return True when a modified source file has no writable target baseline."""
    target_local_path = repo_config.get("target_local_path")
    if target_local_path:
        target_file_path = os.path.join(target_local_path, source_file_path)
        if os.path.exists(target_file_path):
            return False
        return True

    target_ref = repo_config.get("target_ref")
    if not target_ref:
        return False

    try:
        target_repo = github_client.get_repo(repo_config["target_repo"])
        target_repo.get_contents(source_file_path, ref=target_ref)
        return False
    except Exception:
        return True


def build_exclude_folders(repo_config):
    """Build the early-exclusion folder list for commit sync."""
    exclude_folders = []
    if repo_config.get("target_language") != "Chinese":
        return exclude_folders

    for folder_name in get_commit_ignore_folders():
        folder_name = folder_name.strip("/").strip()
        if not folder_name:
            continue
        if source_filters_explicitly_include(folder_name):
            thread_safe_print(f"ℹ️  Explicit source filters include '{folder_name}', skipping the default exclusion for commit sync.")
        else:
            exclude_folders.append(folder_name)
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
    try:
        source_files_translation_mode = get_effective_source_files_translation_mode(
            SOURCE_FILES_TRANSLATION_MODE,
            SOURCE_FILES,
        )
        configured_source_files_translation_mode = normalize_source_files_translation_mode(
            SOURCE_FILES_TRANSLATION_MODE
        )
    except Exception as e:
        thread_safe_print(f"❌ Invalid source files translation mode: {sanitize_exception_message(e)}")
        return 1

    required_env = {
        "SOURCE_REPO": SOURCE_REPO,
        "TARGET_REPO": TARGET_REPO,
        "GITHUB_TOKEN": GITHUB_TOKEN,
        "TARGET_REPO_PATH": TARGET_REPO_PATH,
        "SOURCE_HEAD_REF": SOURCE_HEAD_REF.strip(),
    }
    if source_files_translation_mode != "full":
        required_env["SOURCE_BASE_REF"] = SOURCE_BASE_REF.strip()

    if not all(required_env.values()):
        thread_safe_print("❌ Missing required environment variables:")
        thread_safe_print(f"   SOURCE_REPO: {SOURCE_REPO}")
        thread_safe_print(f"   TARGET_REPO: {TARGET_REPO}")
        thread_safe_print(f"   GITHUB_TOKEN: {'Set' if GITHUB_TOKEN else 'Not set'}")
        thread_safe_print(f"   TARGET_REPO_PATH: {TARGET_REPO_PATH}")
        if source_files_translation_mode != "full":
            thread_safe_print(f"   SOURCE_BASE_REF: {SOURCE_BASE_REF or '(empty)'}")
        thread_safe_print(f"   SOURCE_HEAD_REF: {SOURCE_HEAD_REF or '(empty)'}")
        return 1

    thread_safe_print("🔧 Auto Commit Sync Tool")
    thread_safe_print(f"📍 Source Repo: {SOURCE_REPO}")
    thread_safe_print(f"📍 Target Repo: {TARGET_REPO}")
    thread_safe_print(f"🌿 Source Branch: {SOURCE_BRANCH}")
    if source_files_translation_mode == "full":
        thread_safe_print("🧭 Compare Range: (not used in full mode)")
        thread_safe_print(f"🧭 Source Head Ref: {SOURCE_HEAD_REF}")
    else:
        thread_safe_print(f"🧭 Compare Range: {SOURCE_BASE_REF}...{SOURCE_HEAD_REF}")
    thread_safe_print(f"📁 Source Folder Filter: {SOURCE_FOLDER or '(none)'}")
    thread_safe_print(f"📄 Source File Filter: {SOURCE_FILES or '(none)'}")
    if configured_source_files_translation_mode != source_files_translation_mode:
        thread_safe_print(
            "📄 Source Files Translation Mode: "
            f"{source_files_translation_mode} "
            f"(configured {configured_source_files_translation_mode}, ignored because SOURCE_FILES is empty)"
        )
    else:
        thread_safe_print(f"📄 Source Files Translation Mode: {source_files_translation_mode}")
    thread_safe_print(f"📦 Source Repo Path: {SOURCE_REPO_PATH or '(remote compare API)'}")
    thread_safe_print(f"🎯 Target Ref: {TARGET_REF or '(default branch)'}")
    thread_safe_print(f"📖 Prefer Local Target Read: {PREFER_LOCAL_TARGET_FOR_READ}")
    thread_safe_print(f"📁 Target Repo Path: {TARGET_REPO_PATH}")
    thread_safe_print(f"🤖 AI Provider: {AI_PROVIDER}")
    thread_safe_print(f"🚦 Fail on Translation Error: {FAIL_ON_TRANSLATION_ERROR}")

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

    commit_ignore_files = get_commit_ignore_files()
    repo_config["ignore_files"] = commit_ignore_files
    repo_configs[SOURCE_REPO]["ignore_files"] = commit_ignore_files

    terms_path = TERMS_PATH
    if not terms_path and TARGET_REPO_PATH:
        candidate = os.path.join(TARGET_REPO_PATH, "resources", "terms.md")
        if os.path.exists(candidate):
            terms_path = candidate
    thread_safe_print(f"\n📚 Loading glossary from: {terms_path or '(not configured)'}")
    glossary = load_glossary(terms_path) if terms_path else []
    glossary_matcher = create_glossary_matcher(glossary)

    if source_files_translation_mode == "full":
        base_ref = SOURCE_BASE_REF.strip()
        head_ref = SOURCE_HEAD_REF.strip()
        thread_safe_print(f"\n🚀 Starting commit-based full sync for SOURCE_FILES at: {head_ref}")
        diff_context = {
            "mode": "commit",
            "source_repo": SOURCE_REPO,
            "target_repo": TARGET_REPO,
            # Full mode does not compare refs; base_ref is kept only for shared
            # source-context helpers that expect the key to exist.
            "base_ref": base_ref or head_ref,
            "head_ref": head_ref,
            "changed_files": [],
            "repo_config": repo_config,
            "source_repo_path": SOURCE_REPO_PATH,
            "source_url": f"https://github.com/{SOURCE_REPO}/tree/{head_ref}",
            "source_description": f"full SOURCE_FILES at {head_ref}",
        }
    else:
        try:
            base_ref, head_ref = resolve_compare_range()
        except Exception as e:
            thread_safe_print(f"❌ Failed to resolve compare range: {sanitize_exception_message(e)}")
            return 1

        thread_safe_print(f"\n🚀 Starting commit-based sync for: {base_ref}...{head_ref}")

        try:
            if SOURCE_REPO_PATH:
                diff_context = build_local_commit_diff_context(
                    SOURCE_REPO,
                    TARGET_REPO,
                    base_ref,
                    head_ref,
                    SOURCE_REPO_PATH,
                    repo_configs,
                )
            else:
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
            thread_safe_print(f"❌ Failed to build commit diff context: {sanitize_exception_message(e)}")
            return 1

    if source_files_translation_mode == "full":
        filtered_changed_files = []
        diff_context["changed_files"] = []
        thread_safe_print("📊 Compare diff: skipped for full translation mode")
        pr_diff = ""
    else:
        filtered_changed_files = filter_changed_files(
            diff_context["changed_files"],
            source_folder=SOURCE_FOLDER,
            source_files=SOURCE_FILES,
        )
        diff_context["changed_files"] = filtered_changed_files

        thread_safe_print(f"📊 Filtered changed files: {len(filtered_changed_files)}")
        pr_diff = build_diff_text(filtered_changed_files)

    if source_files_translation_mode == "full":
        thread_safe_print(
            "ℹ️  Full translation uses SOURCE_FILES from SOURCE_HEAD_REF "
            "instead of commit diff analysis."
        )
        added_sections = {}
        modified_sections = {}
        deleted_sections = {}
        added_files = {}
        deleted_files = []
        toc_files = {}
        keyword_files = {}
        added_images = []
        modified_images = []
        deleted_images = []
    else:
        if not filtered_changed_files:
            thread_safe_print("ℹ️  No matching source changes found after folder/file filtering.")
            return 0

        if pr_diff:
            thread_safe_print(f"✅ Built diff text: {len(pr_diff)} characters")
        else:
            thread_safe_print("ℹ️  No markdown patch text found; continuing in case there are file-level or image changes.")

        exclude_folders = build_exclude_folders(repo_config)

        try:
            added_sections, modified_sections, deleted_sections, added_files, deleted_files, toc_files, keyword_files, added_images, modified_images, deleted_images = analyze_source_changes(
                diff_context,
                github_client,
                special_files=SPECIAL_FILES,
                ignore_files=commit_ignore_files,
                repo_configs=repo_configs,
                max_non_system_sections=MAX_NON_SYSTEM_SECTIONS_FOR_AI,
                pr_diff=pr_diff,
                exclude_folders=exclude_folders,
            )
        except Exception as e:
            thread_safe_print(f"❌ Source diff analysis failed: {sanitize_exception_message(e)}")
            return 1

    translation_stats = TranslationStats()

    full_translation_file_paths = set()
    if source_files_translation_mode == "full":
        full_translation_file_paths = apply_source_files_full_translation_mode(
            SOURCE_FILES,
            SOURCE_FOLDER,
            filtered_changed_files,
            diff_context,
            github_client,
            commit_ignore_files,
            added_sections,
            modified_sections,
            deleted_sections,
            added_files,
            toc_files,
            keyword_files,
            translation_stats,
        )
    else:
        full_translation_file_paths.update(
            apply_toc_scope_added_files(
                toc_files,
                diff_context,
                github_client,
                commit_ignore_files,
                added_sections,
                modified_sections,
                deleted_sections,
                added_files,
                keyword_files,
                translation_stats,
            )
        )

    changed_file_paths = {
        file.filename for file in filtered_changed_files if getattr(file, "filename", None)
    }
    file_processing_count = (
        len(full_translation_file_paths)
        if source_files_translation_mode == "full"
        else len(changed_file_paths | full_translation_file_paths)
    )
    parallel_file_processing = should_parallelize_file_processing(file_processing_count)
    if parallel_file_processing:
        thread_safe_print(f"⚡ Translation has {file_processing_count} files; file-level translation will use parallel chunks.")

    if deleted_files:
        thread_safe_print(f"\n🗑️  Processing {len(deleted_files)} deleted files...")
        process_deleted_files(deleted_files, github_client, repo_config)
        git_add_changes(TARGET_REPO_PATH)

    if added_files:
        thread_safe_print(f"\n📄 Processing {len(added_files)} added files...")
        added_tasks = []
        for file_path, file_content in added_files.items():
            def run_added_file(path=file_path, content=file_content):
                success, failure_reasons = process_added_files(
                    {path: content},
                    diff_context,
                    github_client,
                    ai_client,
                    repo_config,
                    glossary_matcher=glossary_matcher,
                    return_details=True,
                    overwrite_existing=path in full_translation_file_paths,
                )
                if success:
                    return make_task_result("success")
                return make_task_result(
                    "failure",
                    failure_reasons.get(path, "Added file processor returned failure"),
                )

            added_tasks.append(make_file_task(file_path, run_added_file))

        added_results = run_file_tasks(added_tasks, "added files", parallel_file_processing)
        for result in added_results:
            _record_translation_task_result(result, translation_stats)
        git_add_successful_task_changes(added_results, TARGET_REPO_PATH)

    if toc_files:
        thread_safe_print(f"\n📋 Processing {len(toc_files)} TOC files...")
        toc_tasks = []
        for file_path, toc_data in toc_files.items():
            def run_toc_file(path=file_path, data=toc_data):
                if data.get("type") != "toc":
                    return make_task_result("failure", f"Unknown TOC data type: {data.get('type')}")

                success = process_toc_file(
                    path,
                    data,
                    diff_context,
                    github_client,
                    ai_client,
                    repo_config,
                    glossary_matcher=glossary_matcher,
                )
                if success:
                    return make_task_result("success")
                return make_task_result("failure", "TOC processor returned failure")

            toc_tasks.append(make_file_task(file_path, run_toc_file))

        toc_results = run_file_tasks(toc_tasks, "TOC files", parallel_file_processing)
        for result in toc_results:
            _record_translation_task_result(result, translation_stats)
        git_add_successful_task_changes(toc_results, TARGET_REPO_PATH)

    if keyword_files:
        thread_safe_print(f"\n📋 Processing {len(keyword_files)} keyword files...")
        keyword_tasks = []
        for file_path, keyword_data in keyword_files.items():
            def run_keyword_file(path=file_path, data=keyword_data):
                if data.get("type") != "keyword":
                    return make_task_result("failure", f"Unknown keyword data type: {data.get('type')}")

                success = process_keyword_file(
                    path,
                    data,
                    diff_context,
                    github_client,
                    ai_client,
                    repo_config,
                )
                if success:
                    return make_task_result("success")
                return make_task_result("failure", "Keyword processor returned failure")

            keyword_tasks.append(make_file_task(file_path, run_keyword_file))

        keyword_results = run_file_tasks(keyword_tasks, "keyword files", parallel_file_processing)
        for result in keyword_results:
            _record_translation_task_result(result, translation_stats)
        git_add_successful_task_changes(keyword_results, TARGET_REPO_PATH)

    if modified_sections:
        thread_safe_print(f"\n📝 Processing {len(modified_sections)} modified files...")
        modified_tasks = []
        for source_file_path, file_sections in modified_sections.items():
            def run_modified_file(path=source_file_path, sections=file_sections):
                return _process_commit_modified_file(
                    path,
                    sections,
                    pr_diff,
                    diff_context,
                    github_client,
                    ai_client,
                    repo_config,
                    glossary_matcher,
                )

            modified_tasks.append(
                make_file_task(
                    source_file_path,
                    run_modified_file,
                    resource_key=source_file_path.replace("/", "-").replace(".md", ""),
                )
            )

        modified_results = run_file_tasks(modified_tasks, "modified files", parallel_file_processing)
        for result in modified_results:
            _record_translation_task_result(result, translation_stats)
        git_add_successful_task_changes(modified_results, TARGET_REPO_PATH)

    if added_images or modified_images or deleted_images:
        thread_safe_print("\n🖼️  Processing images...")
        process_all_images(
            added_images,
            modified_images,
            deleted_images,
            diff_context,
            github_client,
            repo_config,
        )
        git_add_changes(TARGET_REPO_PATH)

    thread_safe_print("\n" + "=" * 80)
    thread_safe_print("📊 Final Summary:")
    thread_safe_print("=" * 80)
    thread_safe_print(f"   📄 Added files: {len(added_files)} processed")
    thread_safe_print(f"   🗑️  Deleted files: {len(deleted_files)} processed")
    thread_safe_print(f"   📋 TOC files: {len(toc_files)} processed")
    thread_safe_print(f"   📋 Keyword files: {len(keyword_files)} processed")
    thread_safe_print(f"   📝 Modified files: {len(modified_sections)} processed")
    thread_safe_print(f"   🖼️  Added images: {len(added_images)} processed")
    thread_safe_print(f"   🖼️  Modified images: {len(modified_images)} processed")
    thread_safe_print(f"   🖼️  Deleted images: {len(deleted_images)} processed")
    translation_stats.print_summary()
    translation_stats.write_failure_report(os.path.join(os.path.dirname(__file__), "temp_output"))
    thread_safe_print("=" * 80)
    if translation_stats.failed:
        thread_safe_print("⚠️  The commit-based sync workflow completed with per-file translation failures.")
        if FAIL_ON_TRANSLATION_ERROR:
            return 1
    else:
        thread_safe_print("🎉 The commit-based sync workflow completed successfully!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
