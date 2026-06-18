#!/usr/bin/env python3
"""
Diff Analyzer Module
Handles diff analysis, content retrieval, hierarchy building, and section extraction
for both PR-based and commit-compare-based workflows.
"""

from dataclasses import dataclass
import difflib
import json
import os
import re
import subprocess
import threading
from typing import Optional
from urllib.parse import urlparse
from github import Github
from log_sanitizer import sanitize_exception_message
from section_matcher import clean_title_for_matching
from special_file_utils import is_toc_file_name

# Thread-safe printing
print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    """Thread-safe print function"""
    with print_lock:
        print(*args, **kwargs)

def is_markdown_heading(line):
    """Return True only for real markdown headings at column 0."""
    if not line or not isinstance(line, str):
        return False
    if line != line.lstrip():
        return False
    return re.match(r'^#{1,10}\s+\S', line) is not None


def _github_url_path(value):
    """Return the path portion of a GitHub URL-like value without query/fragment."""
    parsed = urlparse(str(value).strip())
    if parsed.scheme or parsed.netloc:
        return parsed.path.strip("/")
    return str(value).split("?", 1)[0].split("#", 1)[0].strip("/")


def parse_pr_url(pr_url):
    """Parse a GitHub PR URL to get repo info."""
    path = _github_url_path(pr_url)
    match = re.match(
        r"^(?P<owner>[^/]+)/(?P<repo>[^/]+)/pull/(?P<pr_number>\d+)(?:/.*)?$",
        path,
    )
    if not match:
        raise ValueError(f"Invalid GitHub PR URL: {pr_url}")
    return match.group("owner"), match.group("repo"), int(match.group("pr_number"))


def parse_pr_commit_range_url(pr_url):
    """Parse a PR files commit-range URL, returning None for a plain PR URL.

    GitHub's PR files URLs normally use the `..` form. The `...` branch is
    accepted as a compatibility fallback for compare-style links that point to
    the same PR files surface.
    """
    path = _github_url_path(pr_url)
    match = re.match(
        r"^(?P<owner>[^/]+)/(?P<repo>[^/]+)/pull/(?P<pr_number>\d+)/files/(?P<commit_range>[^/]+)$",
        path,
    )
    if not match:
        return None

    commit_range = match.group("commit_range")
    if "..." in commit_range:
        base_ref, head_ref = commit_range.split("...", 1)
    elif ".." in commit_range:
        base_ref, head_ref = commit_range.split("..", 1)
    else:
        raise ValueError(f"Invalid PR files commit range URL: {pr_url}")

    if not base_ref or not head_ref:
        raise ValueError(f"Invalid PR files commit range URL: {pr_url}")

    return (
        match.group("owner"),
        match.group("repo"),
        int(match.group("pr_number")),
        base_ref,
        head_ref,
    )


@dataclass
class DiffFile:
    """Normalized changed-file representation shared by PR and compare modes."""

    filename: str
    status: str
    patch: Optional[str] = None
    previous_filename: Optional[str] = None


def is_diff_context(value):
    """Return True when the provided value is a normalized diff context."""
    return isinstance(value, dict) and {"mode", "source_repo", "base_ref", "head_ref", "changed_files"}.issubset(value.keys())


def normalize_changed_file(file):
    """Convert GitHub API file objects to a stable local shape."""
    return DiffFile(
        filename=file.filename,
        status=file.status,
        patch=getattr(file, "patch", None),
        previous_filename=getattr(file, "previous_filename", None),
    )


def filter_changed_files_to_pr_scope(changed_files, pr_files):
    """Keep only compare files that match current or previous PR paths."""
    pr_paths = set()
    for file in pr_files:
        if file.filename:
            pr_paths.add(file.filename)
        if file.previous_filename:
            pr_paths.add(file.previous_filename)

    return [
        file
        for file in changed_files
        if file.filename in pr_paths
        or (file.previous_filename and file.previous_filename in pr_paths)
    ]


def print_range_scope_summary(source_context):
    """Print commit range filtering counts when available."""
    if not source_context.get("source_range_url"):
        return
    compare_count = source_context.get("range_compare_file_count")
    pr_count = source_context.get("range_pr_file_count")
    matched_count = source_context.get("range_matched_file_count")
    if compare_count is None or pr_count is None or matched_count is None:
        return
    print(
        f"🔎 Range scope: compare files={compare_count}, "
        f"current PR files={pr_count}, matched files={matched_count}"
    )


def infer_language_direction(source_repo, target_repo):
    """Infer language direction based on repo naming convention."""
    if source_repo.endswith('-cn') and not target_repo.endswith('-cn'):
        return "Chinese", "English"
    if not source_repo.endswith('-cn') and target_repo.endswith('-cn'):
        return "English", "Chinese"
    return "English", "Chinese"


def get_repo_config_from_source_repo(source_repo, repo_configs, target_repo_override=None):
    """Get repository configuration based on source repo name."""
    if source_repo not in repo_configs:
        raise ValueError(f"Unsupported source repository: {source_repo}. Supported: {list(repo_configs.keys())}")

    config = repo_configs[source_repo].copy()
    config["source_repo"] = source_repo
    if target_repo_override:
        config["target_repo"] = target_repo_override
    if "source_language" not in config or "target_language" not in config:
        source_language, target_language = infer_language_direction(
            source_repo, config["target_repo"]
        )
        config.setdefault("source_language", source_language)
        config.setdefault("target_language", target_language)

    return config


def get_repo_config(pr_url, repo_configs):
    """Get repository configuration based on source repo."""
    owner, repo, pr_number = parse_pr_url(pr_url)
    source_repo = f"{owner}/{repo}"
    config = get_repo_config_from_source_repo(source_repo, repo_configs)
    config["pr_number"] = pr_number
    return config


def is_yes_option_enabled(value, default=True):
    """Interpret Yes/No style workflow options."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default

    normalized = str(value).strip().lower()
    if not normalized:
        return default
    if normalized in {"no", "false", "0", "n", "off"}:
        return False
    if normalized in {"yes", "true", "1", "y", "on"}:
        return True
    return default


def _normalize_requested_source_files(source_files):
    """Return a deduplicated set of repo-relative source file paths."""
    if not source_files:
        return set()

    if isinstance(source_files, str):
        source_files = source_files.split(",")

    normalized = set()
    for item in source_files:
        path = str(item).strip()
        if path.startswith("./"):
            path = path[2:]
        if path:
            normalized.add(path)
    return normalized


def should_ignore_related_resources_resource_card_sections(source_context):
    """Return True when commit mode should skip language-specific resources."""
    if not isinstance(source_context, dict) or source_context.get("mode") != "commit":
        return False
    repo_config = source_context.get("repo_config") or {}
    return is_yes_option_enabled(
        repo_config.get("ignore_resource_card_section", True),
        default=True,
    )


def build_diff_text(changed_files):
    """Render a unified diff text block from normalized changed files."""
    diff_content = []
    for file in changed_files:
        if file.filename.endswith('.md') and file.patch:
            diff_content.append(f"File: {file.filename}")
            diff_content.append(file.patch)
            diff_content.append("-" * 80)
    return "\n".join(diff_content)


def build_pr_diff_context(pr_url, github_client, repo_configs):
    """Build a normalized diff context from a PR URL."""
    owner, repo, pr_number = parse_pr_url(pr_url)
    source_repo = f"{owner}/{repo}"
    repository = github_client.get_repo(source_repo)
    pr = repository.get_pull(pr_number)
    repo_config = get_repo_config(pr_url, repo_configs)
    range_info = parse_pr_commit_range_url(pr_url)

    source_description = f"PR #{pr_number}: {pr.title}"
    extra_context = {}
    if range_info:
        _, _, _, base_ref, head_ref = range_info
        # In range mode, base_ref/head_ref describe the incremental source diff,
        # not the PR's original base/head branch refs.
        # GitHub compare can also include upstream commits merged into the PR
        # branch, so keep only files that belong to the current PR.
        pr_files = [normalize_changed_file(file) for file in pr.get_files()]
        comparison = repository.compare(base_ref, head_ref)
        comparison_files = [normalize_changed_file(file) for file in comparison.files]
        changed_files = filter_changed_files_to_pr_scope(comparison_files, pr_files)
        source_description = f"PR #{pr_number} commit range {base_ref}..{head_ref}: {pr.title}"
        extra_context["source_range_url"] = pr_url
        extra_context["range_compare_file_count"] = len(comparison_files)
        extra_context["range_pr_file_count"] = len(pr_files)
        extra_context["range_matched_file_count"] = len(changed_files)
    else:
        base_ref = getattr(pr.base, "sha", None) or getattr(pr.base, "ref", None) or repository.default_branch
        head_ref = getattr(pr.head, "sha", None) or getattr(pr.head, "ref", None) or repository.default_branch
        changed_files = [normalize_changed_file(file) for file in pr.get_files()]

    context = {
        "mode": "pr",
        "source_repo": source_repo,
        "target_repo": repo_config["target_repo"],
        "base_ref": base_ref,
        "head_ref": head_ref,
        "changed_files": changed_files,
        "repo_config": repo_config,
        "source_pr_url": f"https://github.com/{source_repo}/pull/{pr_number}",
        "source_url": pr_url,
        "source_description": source_description,
        "pr_number": pr_number,
        "title": pr.title,
    }
    context.update(extra_context)
    return context


def build_commit_diff_context(source_repo, target_repo, base_ref, head_ref, github_client, repo_configs):
    """Build a normalized diff context from a commit compare range."""
    repository = github_client.get_repo(source_repo)
    comparison = repository.compare(base_ref, head_ref)
    repo_config = get_repo_config_from_source_repo(source_repo, repo_configs, target_repo_override=target_repo)

    return {
        "mode": "commit",
        "source_repo": source_repo,
        "target_repo": repo_config["target_repo"],
        "base_ref": base_ref,
        "head_ref": head_ref,
        "changed_files": [normalize_changed_file(file) for file in comparison.files],
        "repo_config": repo_config,
        "source_url": f"https://github.com/{source_repo}/compare/{base_ref}...{head_ref}",
        "source_description": f"compare {base_ref}...{head_ref}",
    }


def _run_git(repo_path, args):
    return subprocess.check_output(
        ["git", "-C", repo_path, *args],
        text=True,
        stderr=subprocess.STDOUT,
    )


def _parse_git_name_status_line(line):
    parts = line.rstrip("\n").split("\t")
    if not parts or not parts[0]:
        return None

    status_code = parts[0]
    status_prefix = status_code[0]
    if status_prefix == "R" and len(parts) >= 3:
        return {
            "status": "renamed",
            "filename": parts[2],
            "previous_filename": parts[1],
        }
    if status_prefix == "A" and len(parts) >= 2:
        status = "added"
    elif status_prefix == "D" and len(parts) >= 2:
        status = "removed"
    else:
        status = "modified"

    if len(parts) < 2:
        return None
    return {
        "status": status,
        "filename": parts[1],
        "previous_filename": None,
    }


def build_local_commit_diff_context(
    source_repo,
    target_repo,
    base_ref,
    head_ref,
    source_repo_path,
    repo_configs,
):
    """Build a normalized diff context from a local source checkout.

    The GitHub Compare API truncates file lists at 300 files. The local checkout
    path lets scheduled commit syncs use `git diff` instead, while keeping the
    rest of the analyzer contract identical.
    """
    repo_config = get_repo_config_from_source_repo(source_repo, repo_configs, target_repo_override=target_repo)
    name_status = _run_git(source_repo_path, ["diff", "--name-status", "-M", base_ref, head_ref])
    changed_files = []

    for line in name_status.splitlines():
        parsed = _parse_git_name_status_line(line)
        if not parsed:
            continue

        patch_paths = [parsed["filename"]]
        if parsed["previous_filename"]:
            patch_paths.insert(0, parsed["previous_filename"])
        patch = _run_git(
            source_repo_path,
            ["diff", "--find-renames", "--unified=3", base_ref, head_ref, "--", *patch_paths],
        )
        changed_files.append(
            DiffFile(
                filename=parsed["filename"],
                status=parsed["status"],
                patch=patch,
                previous_filename=parsed["previous_filename"],
            )
        )

    return {
        "mode": "commit",
        "source_repo": source_repo,
        "target_repo": target_repo,
        "base_ref": base_ref,
        "head_ref": head_ref,
        "changed_files": changed_files,
        "repo_config": repo_config,
        "source_url": f"https://github.com/{source_repo}/compare/{base_ref}...{head_ref}",
        "source_description": f"local compare {base_ref}...{head_ref}",
    }


def get_pr_diff(pr_url, github_client):
    """Get the diff content from a GitHub PR."""
    try:
        owner, repo, pr_number = parse_pr_url(pr_url)
        source_repo = f"{owner}/{repo}"
        repository = github_client.get_repo(source_repo)
        range_info = parse_pr_commit_range_url(pr_url)
        if range_info:
            _, _, _, base_ref, head_ref = range_info
            # Compare alone can include upstream files merged into the PR branch.
            # Fetch the PR file scope so legacy callers get the same filtered diff
            # as the main PR workflow.
            pr = repository.get_pull(pr_number)
            pr_files = [normalize_changed_file(file) for file in pr.get_files()]
            comparison = repository.compare(base_ref, head_ref)
            comparison_files = [normalize_changed_file(file) for file in comparison.files]
            files = filter_changed_files_to_pr_scope(comparison_files, pr_files)
        else:
            pr = repository.get_pull(pr_number)
            files = [normalize_changed_file(file) for file in pr.get_files()]
        return build_diff_text(files)
    except Exception as e:
        print(f"   ❌ Error getting PR diff: {sanitize_exception_message(e)}")
        return None


def resolve_source_repo_and_ref(source_context_or_pr_url, github_client, ref_name="head_ref"):
    """Resolve source repo and ref from either a diff context or a legacy PR URL."""
    if is_diff_context(source_context_or_pr_url):
        return (
            source_context_or_pr_url["source_repo"],
            source_context_or_pr_url[ref_name],
        )

    owner, repo, pr_number = parse_pr_url(source_context_or_pr_url)
    repository = github_client.get_repo(f"{owner}/{repo}")
    pr = repository.get_pull(pr_number)
    if ref_name == "base_ref":
        ref = getattr(pr.base, "sha", None) or getattr(pr.base, "ref", None) or repository.default_branch
    else:
        ref = getattr(pr.head, "sha", None) or getattr(pr.head, "ref", None) or repository.default_branch
    return f"{owner}/{repo}", ref


def get_source_file_content(file_path, source_context_or_pr_url, github_client, ref_name="head_ref"):
    """Read a source file as UTF-8 text from the resolved source ref."""
    source_repo, ref = resolve_source_repo_and_ref(source_context_or_pr_url, github_client, ref_name=ref_name)
    repository = github_client.get_repo(source_repo)
    return repository.get_contents(file_path, ref=ref).decoded_content.decode("utf-8")


def get_source_file_bytes(file_path, source_context_or_pr_url, github_client, ref_name="head_ref"):
    """Read a source file as bytes from the resolved source ref."""
    source_repo, ref = resolve_source_repo_and_ref(source_context_or_pr_url, github_client, ref_name=ref_name)
    repository = github_client.get_repo(source_repo)
    return repository.get_contents(file_path, ref=ref).decoded_content


def get_target_file_content(
    file_path,
    github_client,
    target_repo,
    target_local_path=None,
    prefer_local_target_for_read=False,
    target_ref=None,
):
    """Read target file content from local checkout, target ref, or default branch."""
    if prefer_local_target_for_read and target_local_path:
        local_file_path = os.path.join(target_local_path, file_path)
        if os.path.exists(local_file_path):
            try:
                with open(local_file_path, 'r', encoding='utf-8') as f:
                    return f.read(), f"local:{local_file_path}"
            except Exception as e:
                print(
                    f"   ⚠️  Error reading local target file {local_file_path}: {sanitize_exception_message(e)}"
                )

    repository = github_client.get_repo(target_repo)
    if target_ref:
        refs_to_try = [target_ref]
    else:
        refs_to_try = [repository.default_branch]

    last_error = None
    for ref in refs_to_try:
        try:
            content = repository.get_contents(file_path, ref=ref).decoded_content.decode('utf-8')
            return content, f"remote:{target_repo}@{ref}"
        except Exception as e:
            last_error = e

    if last_error:
        raise last_error
    raise FileNotFoundError(file_path)

def get_changed_line_ranges(file):
    """Get the ranges of lines that were changed in the PR"""
    changed_ranges = []
    patch = file.patch
    if not patch:
        return changed_ranges
    
    lines = patch.split('\n')
    current_line = 0
    
    for line in lines:
        if line.startswith('@@'):
            # Parse the hunk header to get line numbers
            match = re.search(r'\+(\d+),?(\d+)?', line)
            if match:
                current_line = int(match.group(1))
        elif line.startswith('+') and not line.startswith('+++'):
            # This is an added line
            changed_ranges.append(current_line)
            current_line += 1
        elif line.startswith('-') and not line.startswith('---'):
            # This is a deleted line, also consider as changed
            changed_ranges.append(current_line)
            # Don't increment current_line for deleted lines
            continue
        elif line.startswith(' '):
            # Context line
            current_line += 1
    
    return changed_ranges


def get_changed_line_numbers_from_operations(operations):
    """Return changed head-side line numbers from effective diff operations."""
    changed_lines = []
    for key in ("added_lines", "modified_lines", "deleted_lines"):
        for entry in operations.get(key, []):
            line_number = (
                get_head_line_number(entry)
                if key == "deleted_lines"
                else entry.get("line_number")
            )
            if line_number is not None:
                changed_lines.append(line_number)
    return changed_lines


def get_head_line_number(diff_entry):
    """Return the head-side line number for a parsed diff entry."""
    return diff_entry.get("head_line_number", diff_entry.get("line_number"))


def normalize_line_endings(content):
    """Normalize all line endings to LF for diff analysis."""
    if content is None:
        return ""
    return content.replace("\r\n", "\n").replace("\r", "\n")


def split_normalized_lines(content):
    """Split content into lines after normalizing line endings for analysis."""
    return normalize_line_endings(content).split('\n')


def line_ending_kinds(content):
    """Return the line-ending styles present in content."""
    if not content:
        return set()

    kinds = set()
    index = 0
    while index < len(content):
        char = content[index]
        if char == "\r":
            if index + 1 < len(content) and content[index + 1] == "\n":
                kinds.add("CRLF")
                index += 2
            else:
                kinds.add("CR")
                index += 1
        elif char == "\n":
            kinds.add("LF")
            index += 1
        else:
            index += 1
    return kinds


def operations_change_count(operations):
    """Return total changed-line entries in an operations dict."""
    return sum(len(operations.get(key, [])) for key in ("added_lines", "deleted_lines", "modified_lines"))


def analyze_normalized_snapshot_diff_operations(base_content, head_content):
    """Build diff operations from base/head snapshots after normalizing line endings.

    GitHub patches can show a whole-file rewrite when a commit changes line
    endings. Using normalized snapshots suppresses that noise while preserving
    real content changes.
    """
    base_lines = normalize_line_endings(base_content).splitlines()
    head_lines = normalize_line_endings(head_content).splitlines()

    if base_lines == head_lines:
        return {
            'added_lines': [],
            'deleted_lines': [],
            'modified_lines': []
        }

    patch_lines = list(
        difflib.unified_diff(
            base_lines,
            head_lines,
            fromfile="base",
            tofile="head",
            n=3,
            lineterm="",
        )
    )
    patch = "\n".join(patch_lines)
    return analyze_diff_operations(DiffFile(filename="", status="modified", patch=patch))


def maybe_use_normalized_snapshot_operations(operations, base_content, head_content):
    """Use normalized snapshot operations when they remove line-ending noise."""
    if line_ending_kinds(base_content) == line_ending_kinds(head_content):
        return operations

    snapshot_operations = analyze_normalized_snapshot_diff_operations(base_content, head_content)
    original_count = operations_change_count(operations)
    snapshot_count = operations_change_count(snapshot_operations)

    if original_count and snapshot_count < original_count:
        print(
            f"   🧹 Normalized line-ending diff noise: "
            f"{original_count} patch changes -> {snapshot_count} snapshot changes"
        )
        return snapshot_operations

    return operations


def analyze_diff_operations(file):
    """Analyze diff to categorize operations as added, modified, or deleted (improved GitHub-like approach)"""
    operations = {
        'added_lines': [],      # Lines that were added
        'deleted_lines': [],    # Lines that were deleted  
        'modified_lines': []    # Lines that were modified (both added and deleted content)
    }
    
    patch = file.patch
    if not patch:
        return operations
    
    lines = patch.split('\n')
    current_line = 0
    deleted_line = 0
    
    # Parse diff and keep track of sequence order for better modification detection
    diff_sequence = []  # Track the order of operations in diff
    
    for i, line in enumerate(lines):
        if line.startswith('@@'):
            # Parse the hunk header to get line numbers
            # Format: @@ -old_start,old_count +new_start,new_count @@
            match = re.search(r'-(\d+),?(\d+)?\s+\+(\d+),?(\d+)?', line)
            if match:
                deleted_line = int(match.group(1))
                current_line = int(match.group(3))
        elif line.startswith('+') and not line.startswith('+++'):
            # This is an added line
            added_entry = {
                'line_number': current_line,
                'content': line[1:],  # Remove the '+' prefix
                'is_header': line[1:].strip().startswith('#'),
                'diff_index': i  # Track position in diff
            }
            operations['added_lines'].append(added_entry)
            diff_sequence.append(('added', added_entry))
            current_line += 1
        elif line.startswith('-') and not line.startswith('---'):
            # This is a deleted line
            deleted_entry = {
                'line_number': deleted_line,
                'head_line_number': current_line,
                'content': line[1:],  # Remove the '-' prefix
                'is_header': line[1:].strip().startswith('#'),
                'diff_index': i  # Track position in diff
            }
            operations['deleted_lines'].append(deleted_entry)
            diff_sequence.append(('deleted', deleted_entry))
            deleted_line += 1
        elif line.startswith(' '):
            # Context line (unchanged)
            current_line += 1
            deleted_line += 1
    
    # GitHub-like modification detection: based on diff sequence proximity
    modified_pairs = []
    deleted_headers = [d for d in operations['deleted_lines'] if d['is_header']]
    added_headers = [a for a in operations['added_lines'] if a['is_header']]
    
    used_added_indices = set()
    used_deleted_indices = set()
    
    def heading_level(title):
        match = re.match(r'^(#{1,10})\s+', title.strip())
        return len(match.group(1)) if match else None

    def normalize_heading_title(title):
        cleaned = re.sub(r'^#{1,10}\s*', '', title.strip())
        cleaned = cleaned.replace('`', '')
        cleaned = re.sub(r'\s*\{#[^}]+\}\s*$', '', cleaned)
        cleaned = re.sub(r'\{\{\{\s*\.([A-Za-z0-9_-]+)\s*\}\}\}', r'\1', cleaned)
        cleaned = re.sub(r'[：:：.。]+', ':', cleaned)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned

    def extract_numbered_heading_prefix(title):
        cleaned = normalize_heading_title(title)
        match = re.match(
            r'^(?P<label>step|option|phase|part|chapter|section)\s*(?P<number>\d+)\b',
            cleaned,
            flags=re.IGNORECASE,
        )
        if match:
            return (match.group('label').lower(), int(match.group('number')))
        return None

    def heading_keywords(title):
        stop_words = {
            'a', 'an', 'and', 'are', 'as', 'at', 'by', 'for', 'from', 'in',
            'into', 'of', 'on', 'or', 'the', 'to', 'with', 'your',
        }
        words = re.findall(r'[A-Za-z0-9]+', normalize_heading_title(title).lower())
        return {word for word in words if word not in stop_words}

    def heading_keyword_list(title):
        stop_words = {
            'a', 'an', 'and', 'are', 'as', 'at', 'by', 'for', 'from', 'in',
            'into', 'of', 'on', 'or', 'the', 'to', 'with', 'your',
        }
        words = re.findall(r'[A-Za-z0-9]+', normalize_heading_title(title).lower())
        return [word for word in words if word not in stop_words]

    header_diff_events = sorted(
        [('deleted', header) for header in deleted_headers]
        + [('added', header) for header in added_headers],
        key=lambda item: item[1]['diff_index'],
    )

    def diff_line_is_heading(index):
        if index < 0 or index >= len(lines):
            return False
        line = lines[index]
        if not line or line.startswith(('@@', '+++', '---')):
            return False
        if line[0] not in (' ', '+', '-'):
            return False
        return heading_level(line[1:].strip()) is not None

    def has_intervening_heading(old_header, new_header):
        start = min(old_header['diff_index'], new_header['diff_index']) + 1
        end = max(old_header['diff_index'], new_header['diff_index'])
        return any(diff_line_is_heading(index) for index in range(start, end))

    def is_likely_header_batch_boundary(old_header, new_header):
        """Detect a delete-block/add-block boundary, such as -A -B +C +D."""
        old_index = old_header['diff_index']
        new_index = new_header['diff_index']
        adjacent_index = None
        for event_index, (_, header) in enumerate(header_diff_events):
            if header['diff_index'] == old_index:
                adjacent_index = event_index
                break

        if adjacent_index is None or adjacent_index + 1 >= len(header_diff_events):
            return False
        if header_diff_events[adjacent_index + 1][1]['diff_index'] != new_index:
            return False

        previous_event = header_diff_events[adjacent_index - 1] if adjacent_index > 0 else None
        next_event = (
            header_diff_events[adjacent_index + 2]
            if adjacent_index + 2 < len(header_diff_events)
            else None
        )
        previous_is_nearby_delete = (
            previous_event is not None
            and previous_event[0] == 'deleted'
            and old_index - previous_event[1]['diff_index'] <= 5
        )
        next_is_nearby_add = (
            next_event is not None
            and next_event[0] == 'added'
            and next_event[1]['diff_index'] - new_index <= 5
        )
        return previous_is_nearby_delete and next_is_nearby_add

    # Helper function for semantic similarity
    def are_headers_similar(old, new):
        if heading_level(old) != heading_level(new):
            return False

        # Remove markdown markers
        old_clean = normalize_heading_title(old)
        new_clean = normalize_heading_title(new)
        
        # Check if one is a substring/extension of the other
        if old_clean in new_clean or new_clean in old_clean:
            return True

        # Treat tutorial-style heading renames as modifications when the
        # structural step prefix is preserved, even if the wording changed a lot.
        old_prefix = extract_numbered_heading_prefix(old)
        new_prefix = extract_numbered_heading_prefix(new)
        if old_prefix and old_prefix == new_prefix:
            return True

        old_keywords = heading_keywords(old)
        new_keywords = heading_keywords(new)
        if old_keywords and new_keywords:
            overlap = len(old_keywords & new_keywords)
            smaller_keyword_count = min(len(old_keywords), len(new_keywords))
            if overlap >= 2 and overlap / smaller_keyword_count >= 0.5:
                return True

        old_keyword_list = heading_keyword_list(old)
        new_keyword_list = heading_keyword_list(new)
        if old_keyword_list and new_keyword_list:
            same_leading_keyword = old_keyword_list[0] == new_keyword_list[0]
            short_heading = min(len(old_keyword_list), len(new_keyword_list)) <= 2
            overlap = len(set(old_keyword_list) & set(new_keyword_list))
            smaller_keyword_count = min(len(old_keyword_list), len(new_keyword_list))
            if same_leading_keyword and short_heading and overlap / smaller_keyword_count >= 0.5:
                return True
        
        # Check for similar patterns (like appending -pu, -new, etc.)
        old_base = old_clean.split('-')[0]
        new_base = new_clean.split('-')[0]
        if old_base and new_base and old_base == new_base:
            return True
            
        return False
    
    # GitHub-like approach: Look for adjacent or close operations in diff sequence
    for i, deleted_header in enumerate(deleted_headers):
        if i in used_deleted_indices:
            continue
            
        for j, added_header in enumerate(added_headers):
            if j in used_added_indices:
                continue
                
            deleted_content = deleted_header['content'].strip()
            added_content = added_header['content'].strip()
            
            # Check if they are close in the diff sequence (GitHub's approach)
            diff_distance = abs(added_header['diff_index'] - deleted_header['diff_index'])
            is_close_in_diff = diff_distance <= 5  # Allow small gap for context lines

            same_heading_level = heading_level(deleted_content) == heading_level(added_content)
            has_no_intervening_heading = not has_intervening_heading(deleted_header, added_header)
            is_batch_boundary = is_likely_header_batch_boundary(deleted_header, added_header)
            is_structural_rename = (
                same_heading_level
                and has_no_intervening_heading
                and not is_batch_boundary
            )
            
            # Check semantic similarity
            is_similar = are_headers_similar(deleted_content, added_content)
            
            # GitHub-like logic: adjacent same-level headings are usually a line
            # replacement even when the wording changes substantially. Keep
            # semantic matching as a fallback for close but less tidy diffs.
            if is_close_in_diff and (is_structural_rename or is_similar):
                modified_pairs.append({
                    'deleted': deleted_header,
                    'added': added_header,
                    'original_content': deleted_header['content']
                })
                used_added_indices.add(j)
                used_deleted_indices.add(i)
                break
            # Fallback: strong semantic similarity even if not adjacent
            elif is_similar and abs(added_header['line_number'] - deleted_header['line_number']) <= 20:
                modified_pairs.append({
                    'deleted': deleted_header,
                    'added': added_header,
                    'original_content': deleted_header['content']
                })
                used_added_indices.add(j)
                used_deleted_indices.add(i)
                break
    
    # Remove identified modifications from pure additions/deletions
    for pair in modified_pairs:
        if pair['deleted'] in operations['deleted_lines']:
            operations['deleted_lines'].remove(pair['deleted'])
        if pair['added'] in operations['added_lines']:
            operations['added_lines'].remove(pair['added'])
        # Store both new and original content for modified headers
        modified_entry = pair['added'].copy()
        modified_entry['original_content'] = pair['original_content']
        operations['modified_lines'].append(modified_entry)
    
    return operations

def _is_sub_heading_content(content):
    """Return True if content is a sub-section heading (## or deeper)."""
    stripped = (content or "").strip()
    return bool(re.match(r'^#{2,}\s+\S', stripped))


def _collect_sub_heading_line_numbers(file_content):
    """Return a set of 1-based line numbers for sub-section headings (## or deeper).

    Headings inside fenced code blocks are excluded.
    """
    lines = split_normalized_lines(file_content or "")
    sub_headings = set()
    in_code_block = False
    code_block_delimiter = None

    for line_num, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped.startswith('```') or stripped.startswith('~~~'):
            if not in_code_block:
                in_code_block = True
                code_block_delimiter = stripped[:3]
                continue
            elif stripped.startswith(code_block_delimiter):
                in_code_block = False
                code_block_delimiter = None
                continue
        if in_code_block:
            continue
        if is_markdown_heading(line) and _is_sub_heading_content(line):
            sub_headings.add(line_num)

    return sub_headings


RESTRUCTURE_CHANGED_LINES_RATIO = float(
    os.environ.get("RESTRUCTURE_CHANGED_LINES_RATIO", "0.5")
)


def detect_restructured_file(file_content, base_file_content, operations):
    """Detect if a modified document has been structurally restructured.

    A document is considered restructured when BOTH conditions are met:
      1. ALL sub-section headings (## or deeper) are in the diff's changed
         lines — every heading was either added, deleted, or genuinely modified
         (renamed) and no heading was left unchanged.
      2. The number of changed lines in the diff accounts for at least 50% of
         the document's total lines, preventing small heading-only edits from
         triggering a costly full retranslation.

    Headings that appear as "modified" but have identical old/new content are
    treated as unchanged (this happens with full-rewrite patches).
    """
    from collections import Counter

    head_sub_headings = _collect_sub_heading_line_numbers(file_content)
    base_sub_headings = _collect_sub_heading_line_numbers(base_file_content)

    if len(head_sub_headings) < 2 and len(base_sub_headings) < 2:
        return False

    # Collect genuinely modified sub-heading lines (content actually changed).
    # Headings paired as "modified" but with identical content are unchanged.
    genuinely_modified_head_lines = set()
    genuinely_modified_base_counter = Counter()

    for line in operations['modified_lines']:
        if not line['is_header']:
            continue
        new_content = line['content'].strip()
        old_content = line.get('original_content', '').strip()
        is_new_sub = _is_sub_heading_content(new_content)
        is_old_sub = _is_sub_heading_content(old_content)

        if new_content == old_content:
            continue
        if is_new_sub:
            genuinely_modified_head_lines.add(line['line_number'])
        if is_old_sub:
            genuinely_modified_base_counter[old_content] += 1

    # --- HEAD side: every sub-heading must be truly added or genuinely modified ---
    diff_head_changed_lines = set()
    for line in operations['added_lines']:
        if line['is_header'] and _is_sub_heading_content(line['content']):
            diff_head_changed_lines.add(line['line_number'])
    diff_head_changed_lines |= genuinely_modified_head_lines

    if head_sub_headings and not head_sub_headings.issubset(diff_head_changed_lines):
        return False

    # --- BASE side: every sub-heading must be truly deleted or genuinely modified.
    # Use Counter (multiset) to handle duplicate heading contents correctly:
    # e.g. two "## Examples" sections where only one is deleted.
    base_lines = split_normalized_lines(base_file_content or "")
    base_sub_heading_counter = Counter()
    for line_num in base_sub_headings:
        if 1 <= line_num <= len(base_lines):
            base_sub_heading_counter[base_lines[line_num - 1].strip()] += 1

    diff_base_changed_counter = Counter()
    for line in operations['deleted_lines']:
        if line['is_header'] and _is_sub_heading_content(line['content']):
            diff_base_changed_counter[line['content'].strip()] += 1
    diff_base_changed_counter += genuinely_modified_base_counter

    if base_sub_heading_counter:
        for content, count in base_sub_heading_counter.items():
            if diff_base_changed_counter.get(content, 0) < count:
                return False

    # --- Changed-lines ratio check: the diff must touch a substantial portion
    # of the document to qualify as a restructure, not just rename headings.
    changed_line_count = (
        len(operations['added_lines'])
        + len(operations['deleted_lines'])
        + len(operations['modified_lines'])
    )
    head_line_count = len(split_normalized_lines(file_content or ""))
    base_line_count = len(base_lines)
    doc_line_count = max(head_line_count, base_line_count, 1)

    if changed_line_count / doc_line_count < RESTRUCTURE_CHANGED_LINES_RATIO:
        return False

    return True


def build_hierarchy_dict(file_content):
    """Build hierarchy dictionary from file content, excluding content inside code blocks"""
    lines = split_normalized_lines(file_content)
    level_stack = []
    all_hierarchy_dict = {}
    
    # Track code block state
    in_code_block = False
    code_block_delimiter = None  # Track the type of code block (``` or ```)
    
    # Build complete hierarchy for all headers
    for line_num, line in enumerate(lines, 1):
        original_line = line
        line = line.strip()
        
        # Check for code block delimiters
        if line.startswith('```') or line.startswith('~~~'):
            if not in_code_block:
                # Entering a code block
                in_code_block = True
                code_block_delimiter = line[:3]  # Store the delimiter type
                continue
            elif line.startswith(code_block_delimiter):
                # Exiting a code block
                in_code_block = False
                code_block_delimiter = None
                continue
        
        # Skip processing if we're inside a code block
        if in_code_block:
            continue
        
        # Process headers only if not in code block
        if is_markdown_heading(original_line):
            match = re.match(r'^(#{1,10})\s+(.+)', line)
            if match:
                level = len(match.group(1))
                title = match.group(2).strip()
                
                # Remove items from stack that are at same or deeper level
                while level_stack and level_stack[-1][0] >= level:
                    level_stack.pop()
                
                # Build hierarchy with special handling for top-level titles
                if level == 1:
                    # Top-level titles are included directly without hierarchy path
                    hierarchy_line = line
                elif level_stack:
                    # For other levels, build path but skip the top-level title (level 1)
                    path_parts = [item[1] for item in level_stack if item[0] > 1]  # Skip level 1 items
                    path_parts.append(line)
                    hierarchy_line = " > ".join(path_parts)
                else:
                    # Fallback for other cases
                    hierarchy_line = line
                
                if hierarchy_line:  # Only add non-empty hierarchies
                    all_hierarchy_dict[line_num] = hierarchy_line
                
                level_stack.append((level, line))
    
    return all_hierarchy_dict

def build_hierarchy_path(lines, line_num, all_headers):
    """Build the full hierarchy path for a header at given line"""
    if line_num not in all_headers:
        return []
    
    current_header = all_headers[line_num]
    current_level = current_header['level']
    hierarchy_path = []
    
    # Find all parent headers
    for check_line in sorted(all_headers.keys()):
        if check_line >= line_num:
            break
        
        header = all_headers[check_line]
        if header['level'] < current_level:
            # This is a potential parent
            # Remove any headers at same or deeper level
            while hierarchy_path and hierarchy_path[-1]['level'] >= header['level']:
                hierarchy_path.pop()
            hierarchy_path.append(header)
    
    # Add current header
    hierarchy_path.append(current_header)
    
    return hierarchy_path

def build_hierarchy_for_modified_section(file_content, target_line_num, original_line, base_hierarchy_dict):
    """Build hierarchy path for a modified section using original content"""
    lines = split_normalized_lines(file_content)
    
    # Get the level of the original header
    original_match = re.match(r'^(#{1,10})\s+(.+)', original_line)
    if not original_match:
        return None
    
    original_level = len(original_match.group(1))
    original_title = original_match.group(2).strip()
    
    # Find parent sections by looking backwards from target line
    level_stack = []
    
    for line_num in range(1, target_line_num):
        if line_num in base_hierarchy_dict:
            # This is a header line
            line_content = lines[line_num - 1].strip()
            if line_content.startswith('#'):
                match = re.match(r'^(#{1,10})\s+(.+)', line_content)
                if match:
                    level = len(match.group(1))
                    title = match.group(2).strip()
                    
                    # Remove items from stack that are at same or deeper level
                    while level_stack and level_stack[-1][0] >= level:
                        level_stack.pop()
                    
                    # Add this header to stack if it's a potential parent
                    if level < original_level:
                        level_stack.append((level, line_content))
    
    # Build hierarchy path using original content
    if level_stack:
        path_parts = [item[1] for item in level_stack[1:]]  # Skip first level
        path_parts.append(original_line)
        hierarchy_line = " > ".join(path_parts)
    else:
        hierarchy_line = original_line if original_level > 1 else ""
    
    return hierarchy_line if hierarchy_line else None


def get_fence_marker(line):
    """Return a markdown fence marker for code block boundaries."""
    match = re.match(r'^(`{3,}|~{3,})', (line or "").strip())
    return match.group(1) if match else None


def find_section_end_index(lines, start_index, current_level, stop_level1_at_next_heading=False):
    """Find the 0-based exclusive end index for a markdown section.

    By default, a level-1 heading owns the document until the next level-1
    heading. Some call sites need title-like behavior where a level-1 heading
    stops at the first child heading; pass stop_level1_at_next_heading=True for
    that narrower boundary.
    """
    in_code_block = False
    code_block_delimiter = None

    for index in range(start_index + 1, len(lines)):
        raw_line = lines[index]
        stripped_line = raw_line.strip()
        fence_marker = get_fence_marker(stripped_line)

        if fence_marker:
            if not in_code_block:
                in_code_block = True
                code_block_delimiter = fence_marker
            elif stripped_line.startswith(code_block_delimiter):
                in_code_block = False
                code_block_delimiter = None
            continue

        if in_code_block or not is_markdown_heading(raw_line):
            continue

        line_level = len(stripped_line.split()[0]) if stripped_line.split() else 0
        if line_level <= current_level or (
            stop_level1_at_next_heading and current_level == 1
        ):
            return index

    return len(lines)


def find_section_boundaries(lines, hierarchy_dict):
    """Find the start and end line for each section based on hierarchy"""
    section_boundaries = {}
    
    # Sort sections by line number
    sorted_sections = sorted(hierarchy_dict.items(), key=lambda x: int(x[0]))
    
    for i, (line_num, hierarchy) in enumerate(sorted_sections):
        start_line = int(line_num) - 1  # Convert to 0-based index
        
        if start_line >= len(lines):
            continue
            
        # Get current section level
        current_line = lines[start_line].strip()
        if not current_line.startswith('#'):
            continue
            
        current_level = len(current_line.split()[0])  # Count # characters
        end_line = find_section_end_index(lines, start_line, current_level)
        
        section_boundaries[line_num] = {
            'start': start_line,
            'end': end_line,
            'hierarchy': hierarchy,
            'level': current_level
        }
    
    return section_boundaries


RELATED_RESOURCES_RESOURCE_CARD_RE = re.compile(
    r"<RelatedResources\b[^>]*>.*?<ResourceCard\b.*?</RelatedResources\s*>",
    re.DOTALL,
)


def remove_fenced_code_blocks(content):
    """Return content with markdown fenced code blocks removed."""
    lines = split_normalized_lines(content)
    kept_lines = []
    in_code_block = False
    code_block_delimiter = None

    for line in lines:
        stripped_line = line.strip()
        fence_marker = get_fence_marker(stripped_line)

        if fence_marker:
            if not in_code_block:
                in_code_block = True
                code_block_delimiter = fence_marker
            elif stripped_line.startswith(code_block_delimiter):
                in_code_block = False
                code_block_delimiter = None
            continue

        if not in_code_block:
            kept_lines.append(line)

    return "\n".join(kept_lines)


def contains_related_resources_resource_card_block(content):
    """Return True for a RelatedResources block that contains ResourceCard."""
    if not isinstance(content, str) or not content:
        return False
    searchable_content = remove_fenced_code_blocks(content)
    return RELATED_RESOURCES_RESOURCE_CARD_RE.search(searchable_content) is not None


def find_related_resources_resource_card_sections(file_content):
    """Find markdown sections that contain RelatedResources ResourceCard blocks."""
    lines = split_normalized_lines(file_content)
    hierarchy_dict = build_hierarchy_dict(file_content)
    matching_sections = []

    for line_num, hierarchy in sorted(hierarchy_dict.items(), key=lambda item: int(item[0])):
        start = int(line_num) - 1
        if start < 0 or start >= len(lines):
            continue

        current_line = lines[start].strip()
        if not is_markdown_heading(lines[start]):
            continue

        current_level = len(current_line.split()[0])
        end = find_section_end_index(
            lines,
            start,
            current_level,
            stop_level1_at_next_heading=True,
        )

        section_content = "\n".join(lines[start:end])
        if contains_related_resources_resource_card_block(section_content):
            matching_sections.append(
                {
                    "line_number": int(line_num),
                    "hierarchy": hierarchy,
                    "start": start,
                    "end": end,
                }
            )

    return matching_sections


def remove_related_resources_resource_card_sections(file_content):
    """Remove language-specific RelatedResources sections from source content."""
    matching_sections = find_related_resources_resource_card_sections(file_content)
    if not matching_sections:
        return file_content, []

    lines = split_normalized_lines(file_content)
    removed_line_indexes = set()
    for section in matching_sections:
        removed_line_indexes.update(range(section["start"], section["end"]))

    kept_lines = [
        line
        for index, line in enumerate(lines)
        if index not in removed_line_indexes
    ]
    while kept_lines and not kept_lines[-1].strip():
        kept_lines.pop()

    stripped_content = "\n".join(kept_lines)
    if stripped_content and file_content.endswith(("\n", "\r")):
        stripped_content += "\n"

    return stripped_content, matching_sections


def build_related_resources_resource_card_line_ranges(file_content):
    """Return 1-based line ranges for RelatedResources ResourceCard sections."""
    return [
        (section["start"] + 1, section["end"] + 1)
        for section in find_related_resources_resource_card_sections(file_content)
    ]


def line_number_in_ranges(line_number, ranges):
    """Return True if a 1-based line number is inside any [start, end) range."""
    return any(start <= line_number < end for start, end in ranges)


def filter_related_resources_resource_card_diff(diff_text, base_content, head_content):
    """Remove RelatedResources section lines from a unified diff for AI prompts."""
    if not diff_text:
        return diff_text

    base_ranges = build_related_resources_resource_card_line_ranges(base_content)
    head_ranges = build_related_resources_resource_card_line_ranges(head_content)
    if not base_ranges and not head_ranges:
        return diff_text

    filtered_lines = []
    old_line_number = None
    new_line_number = None

    for line in diff_text.splitlines():
        if line.startswith("@@"):
            match = re.search(r'-(\d+),?(\d+)?\s+\+(\d+),?(\d+)?', line)
            if match:
                old_line_number = int(match.group(1))
                new_line_number = int(match.group(3))
            filtered_lines.append(line)
            continue

        if line.startswith(("+++", "---")):
            filtered_lines.append(line)
            continue

        if line.startswith("+"):
            should_skip = (
                new_line_number is not None
                and line_number_in_ranges(new_line_number, head_ranges)
            )
            if not should_skip:
                filtered_lines.append(line)
            if new_line_number is not None:
                new_line_number += 1
            continue

        if line.startswith("-"):
            should_skip = (
                old_line_number is not None
                and line_number_in_ranges(old_line_number, base_ranges)
            )
            if not should_skip:
                filtered_lines.append(line)
            if old_line_number is not None:
                old_line_number += 1
            continue

        if line.startswith(" "):
            should_skip = (
                old_line_number is not None
                and new_line_number is not None
                and (
                    line_number_in_ranges(old_line_number, base_ranges)
                    or line_number_in_ranges(new_line_number, head_ranges)
                )
            )
            if not should_skip:
                filtered_lines.append(line)
            if old_line_number is not None:
                old_line_number += 1
            if new_line_number is not None:
                new_line_number += 1
            continue

        filtered_lines.append(line)

    return "\n".join(filtered_lines)


def filter_related_resources_resource_card_source_diff(source_diff_dict):
    """Drop source-diff entries for language-specific RelatedResources sections."""
    filtered = {}
    skipped_entries = []

    for key, diff_info in source_diff_dict.items():
        if not isinstance(diff_info, dict):
            filtered[key] = diff_info
            continue

        old_content = diff_info.get("old_content") or ""
        new_content = diff_info.get("new_content") or ""
        if (
            contains_related_resources_resource_card_block(old_content)
            or contains_related_resources_resource_card_block(new_content)
        ):
            skipped_entries.append((key, diff_info))
            continue

        filtered[key] = diff_info

    return filtered, skipped_entries


def extract_section_content(lines, start_line, hierarchy_dict):
    """Extract the content of a section starting from start_line (includes sub-sections)"""
    if not lines or start_line < 1 or start_line > len(lines):
        return ""
    
    start_index = start_line - 1  # Convert to 0-based index
    section_content = []
    
    # Find the header at start_line
    raw_current_line = lines[start_index]
    current_line = raw_current_line.strip()
    if not is_markdown_heading(raw_current_line):
        return ""
    
    # Get the level of current header
    current_level = len(current_line.split()[0])  # Count # characters
    section_content.append(current_line)
    
    # Track fenced code blocks so '#' inside code does not terminate section extraction.
    in_code_block = False
    code_block_delimiter = None

    # Special handling for top-level titles (level 1)
    if current_level == 1:
        # For top-level titles, only extract content until the first next-level header (##)
        for i in range(start_index + 1, len(lines)):
            raw_line = lines[i]
            line = raw_line.strip()

            if line.startswith('```') or line.startswith('~~~'):
                if not in_code_block:
                    in_code_block = True
                    code_block_delimiter = line[:3]
                elif line.startswith(code_block_delimiter):
                    in_code_block = False
                    code_block_delimiter = None
                section_content.append(raw_line.rstrip())
                continue

            if not in_code_block and is_markdown_heading(raw_line):
                # Check if this is a header of next level (##, ###, etc.)
                line_level = len(line.split()[0]) if line.split() else 0
                if line_level > current_level:
                    # Found first subsection, stop here for top-level titles
                    break
                elif line_level <= current_level:
                    # Found same or higher level header, also stop
                    break
            
            section_content.append(raw_line.rstrip())  # Keep original line without trailing whitespace
    else:
        # For non-top-level titles, use the original logic
        # Extract content until we hit the next header of same or higher level
        for i in range(start_index + 1, len(lines)):
            raw_line = lines[i]
            line = raw_line.strip()

            if line.startswith('```') or line.startswith('~~~'):
                if not in_code_block:
                    in_code_block = True
                    code_block_delimiter = line[:3]
                elif line.startswith(code_block_delimiter):
                    in_code_block = False
                    code_block_delimiter = None
                section_content.append(raw_line.rstrip())
                continue

            if not in_code_block and is_markdown_heading(raw_line):
                # Check if this is a header of same or higher level
                line_level = len(line.split()[0]) if line.split() else 0
                if line_level <= current_level:
                    # Found a header of same or higher level, stop here regardless
                    # Each section should be extracted individually
                    break
            
            section_content.append(raw_line.rstrip())  # Keep original line without trailing whitespace
    
    return '\n'.join(section_content)

def extract_section_direct_content(lines, start_line):
    """Extract ONLY the direct content of a section (excluding sub-sections) - for source diff dict"""
    if not lines or start_line < 1 or start_line > len(lines):
        return ""
    
    start_index = start_line - 1  # Convert to 0-based index
    section_content = []
    
    # Find the header at start_line
    raw_current_line = lines[start_index]
    current_line = raw_current_line.strip()
    if not is_markdown_heading(raw_current_line):
        return ""
    
    # Add the header line
    section_content.append(current_line)
    
    # Only extract until the first markdown header (any level), excluding headers inside code blocks.
    in_code_block = False
    code_block_delimiter = None

    for i in range(start_index + 1, len(lines)):
        raw_line = lines[i]
        line = raw_line.strip()

        if line.startswith('```') or line.startswith('~~~'):
            if not in_code_block:
                in_code_block = True
                code_block_delimiter = line[:3]
            elif line.startswith(code_block_delimiter):
                in_code_block = False
                code_block_delimiter = None
            section_content.append(raw_line.rstrip())
            continue

        if not in_code_block and is_markdown_heading(raw_line):
            # Stop at ANY header to get only direct content
            break
        section_content.append(raw_line.rstrip())
    
    return '\n'.join(section_content)

def find_first_heading_line(file_lines):
    """
    Find the 1-based line number of the first top-level heading (# ).
    Returns 0 if no heading is found (entire file is frontmatter).
    """
    for i, line in enumerate(file_lines):
        if line.strip().startswith('# '):
            return i + 1  # 1-based
    return 0


def find_first_level2_line(file_lines):
    """
    Find the 1-based line number of the first level-2 heading (## ).
    Returns 0 if no ## heading is found.
    """
    for i, line in enumerate(file_lines):
        if line.strip().startswith('## '):
            return i + 1
    return 0


def has_changes_in_range(operations, start_1based, end_1based,
                         base_start_1based=None, base_end_1based=None):
    """
    Check if any operations affect lines in [start, end) (1-based).
    Added/modified lines are checked against (start, end).
    Deleted lines are checked against (base_start, base_end).
    """
    if base_start_1based is None:
        base_start_1based = start_1based
    if base_end_1based is None:
        base_end_1based = end_1based

    for line in operations.get('added_lines', []):
        if start_1based <= line['line_number'] < end_1based:
            return True
    for line in operations.get('modified_lines', []):
        if start_1based <= line['line_number'] < end_1based:
            return True
    for line in operations.get('deleted_lines', []):
        if base_start_1based <= line['line_number'] < base_end_1based:
            return True
    return False


def extract_frontmatter_content(file_lines):
    """Extract content from the beginning of file to the first top-level header"""
    if not file_lines:
        return ""
    
    frontmatter_lines = []
    for i, line in enumerate(file_lines):
        line_stripped = line.strip()
        # Stop when we hit the first top-level header
        if line_stripped.startswith('# '):
            break
        frontmatter_lines.append(line.rstrip())
    
    return '\n'.join(frontmatter_lines)


def extract_intro_section_content(file_lines):
    """
    Extract intro section content: from the first top-level heading (#)
    to the line before the first level-2 header (##).
    Excludes frontmatter (everything before #).

    Returns: (intro_content, start_line_1based, end_line_1based)
      - start_line_1based: line number of the first # heading
      - end_line_1based: line number of the first ## heading (content stops before it)
      - If no # heading found, returns ("", 0, 0)
    """
    if not file_lines:
        return "", 0, 0

    start_idx = None
    for i, line in enumerate(file_lines):
        if line.strip().startswith('# '):
            start_idx = i
            break

    if start_idx is None:
        return "", 0, 0

    intro_lines = []
    end_line = len(file_lines)
    for i in range(start_idx, len(file_lines)):
        if file_lines[i].strip().startswith('## '):
            end_line = i + 1  # 1-based
            break
        intro_lines.append(file_lines[i].rstrip())
    
    intro_content = '\n'.join(intro_lines)
    return intro_content, start_idx + 1, end_line


def detect_intro_section_changes(operations, file_lines):
    """
    Detect if the intro section has any changes.
    Returns: (has_intro_changes, first_level2_line)
    """
    if not file_lines:
        return False, 0
    
    # Find the first level-2 header line
    first_level2_line = 0
    for i, line in enumerate(file_lines, 1):
        if line.strip().startswith('## '):
            first_level2_line = i
            break
    
    if first_level2_line == 0:
        # No level-2 header, entire file is considered intro
        first_level2_line = len(file_lines) + 1
    
    # Check if any operations affect the intro section (before first_level2_line)
    has_changes = False
    
    for added_line in operations.get('added_lines', []):
        if added_line['line_number'] < first_level2_line:
            has_changes = True
            break
    
    if not has_changes:
        for modified_line in operations.get('modified_lines', []):
            if modified_line['line_number'] < first_level2_line:
                has_changes = True
                break
    
    if not has_changes:
        for deleted_line in operations.get('deleted_lines', []):
            # Deleted lines have line numbers from the old file
            # We need to check if they were in the intro section
            if deleted_line['line_number'] < first_level2_line:
                has_changes = True
                break
    
    return has_changes, first_level2_line


def extract_affected_sections(hierarchy_dict, file_lines):
    """Extract all affected sections based on hierarchy dict"""
    affected_sections = {}
    
    for line_num, hierarchy in hierarchy_dict.items():
        if line_num == "0" and hierarchy == "frontmatter":
            # Special handling for frontmatter
            frontmatter_content = extract_frontmatter_content(file_lines)
            if frontmatter_content:
                affected_sections[line_num] = frontmatter_content
        else:
            line_number = int(line_num)
            section_content = extract_section_content(file_lines, line_number, hierarchy_dict)
            
            if section_content:
                affected_sections[line_num] = section_content
    
    return affected_sections

def find_containing_section(line_num, all_headers):
    """Find which section a line belongs to"""
    current_section = None
    for header_line_num in sorted(all_headers.keys()):
        if header_line_num <= line_num:
            current_section = header_line_num
        else:
            break
    return current_section

def find_affected_sections(lines, changed_lines, all_headers):
    """Find which sections are affected by the changes"""
    affected_sections = set()
    
    for changed_line in changed_lines:
        # Find the section this changed line belongs to
        current_section = None
        
        # Find the most recent header before or at the changed line
        for line_num in sorted(all_headers.keys()):
            if line_num <= changed_line:
                current_section = line_num
            else:
                break
        
        if current_section:
            # Only add the directly affected section (the one that directly contains the change)
            affected_sections.add(current_section)
    
    return affected_sections

def find_sections_by_operation_type(lines, operations, all_headers, base_hierarchy_dict=None):
    """Find sections affected by different types of operations"""
    sections = {
        'added': set(),
        'modified': set(), 
        'deleted': set()
    }
    
    # Process added lines
    for added_line in operations['added_lines']:
        line_num = added_line['line_number']
        if added_line['is_header']:
            # This is a new header - only mark the section as added if the header itself is new
            sections['added'].add(line_num)
        # Note: We don't mark sections as "added" just because they contain new non-header content
        # That would be a "modified" section, not an "added" section
    
    # Process modified lines  
    for modified_line in operations['modified_lines']:
        line_num = modified_line['line_number']
        if modified_line['is_header']:
            sections['modified'].add(line_num)
        else:
            section = find_containing_section(line_num, all_headers)
            if section:
                sections['modified'].add(section)
    
    # Process deleted lines - use base hierarchy to find deleted sections
    for deleted_line in operations['deleted_lines']:
        if deleted_line['is_header']:
            deleted_base_line_num = deleted_line['line_number']

            # Try direct lookup by base line number first (most reliable)
            if base_hierarchy_dict and deleted_base_line_num in base_hierarchy_dict:
                sections['deleted'].add(deleted_base_line_num)
                print(f"   🗑️  Detected deleted section: {deleted_line['content']} (line {deleted_base_line_num})")
                continue

            # Fallback to title matching
            deleted_title = clean_title_for_matching(deleted_line['content'])
            search_hierarchy = base_hierarchy_dict if base_hierarchy_dict else all_headers
            
            found_deleted = False
            for line_num, hierarchy_line in search_hierarchy.items():
                if isinstance(hierarchy_line, dict):
                    original_title = clean_title_for_matching(hierarchy_line.get('title', ''))
                elif ' > ' in hierarchy_line:
                    original_title = clean_title_for_matching(hierarchy_line.split(' > ')[-1])
                else:
                    original_title = clean_title_for_matching(hierarchy_line)
                
                if deleted_title == original_title:
                    sections['deleted'].add(line_num)
                    print(f"   🗑️  Detected deleted section: {deleted_line['content']} (line {line_num})")
                    found_deleted = True
                    break
            
            if not found_deleted:
                print(f"   ⚠️  Could not find deleted section: {deleted_line['content']}")
    
    return sections


def get_target_hierarchy_and_content(
    file_path,
    github_client,
    target_repo,
    target_local_path=None,
    prefer_local_target_for_read=False,
    target_ref=None,
):
    """Get target hierarchy and content"""
    try:
        file_content, target_source = get_target_file_content(
            file_path,
            github_client,
            target_repo,
            target_local_path=target_local_path,
            prefer_local_target_for_read=prefer_local_target_for_read,
            target_ref=target_ref,
        )
        lines = split_normalized_lines(file_content)

        # Build hierarchy using same method
        hierarchy = build_hierarchy_dict(file_content)

        print(f"   📌 Target baseline source: {target_source}")
        return hierarchy, lines
    except Exception as e:
        print(f"   ❌ Error getting target file: {sanitize_exception_message(e)}")
        return {}, []

def get_source_sections_content(source_context_or_pr_url, file_path, source_affected, github_client):
    """Get the content of source sections for better context."""
    try:
        file_content = get_source_file_content(
            file_path,
            source_context_or_pr_url,
            github_client,
            ref_name="head_ref",
        )
        lines = split_normalized_lines(file_content)
        
        # Extract source sections
        source_sections = {}
        
        for line_num, hierarchy in source_affected.items():
            if line_num == "0" and hierarchy == "frontmatter":
                # Special handling for frontmatter
                frontmatter_content = extract_frontmatter_content(lines)
                if frontmatter_content:
                    source_sections[line_num] = frontmatter_content
            else:
                line_number = int(line_num)
                section_content = extract_section_content(lines, line_number, source_affected)
                if section_content:
                    source_sections[line_num] = section_content
        
        return source_sections
    except Exception as e:
        thread_safe_print(f"   ⚠️  Could not get source sections: {sanitize_exception_message(e)}")
        return {}

def get_source_file_hierarchy(file_path, pr_url, github_client, get_base_version=False):
    """Get source file hierarchy from PR head or base"""
    try:
        ref_name = "base_ref" if get_base_version else "head_ref"
        source_file_content = get_source_file_content(
            file_path,
            pr_url,
            github_client,
            ref_name=ref_name,
        )
            
        source_hierarchy = build_hierarchy_dict(source_file_content)
        
        return source_hierarchy
        
    except Exception as e:
        thread_safe_print(f"   ❌ Error getting source file hierarchy: {sanitize_exception_message(e)}")
        return {}



def trim_content_before_tabs_panel(content):
    """Trim section content at the first TabsPanel line."""
    if not isinstance(content, str) or not content:
        return content, False

    lines = split_normalized_lines(content)
    for idx, line in enumerate(lines):
        if "<TabsPanel" in line:
            trimmed = "\n".join(lines[:idx]).rstrip()
            return trimmed, True
    return content, False


def normalize_keywords_regular_source_diff(source_diff_dict):
    """For keywords.md regular path, keep only non-TabsPanel content."""
    normalized = {}
    dropped = 0

    for key, diff_info in source_diff_dict.items():
        if not isinstance(diff_info, dict):
            normalized[key] = diff_info
            continue

        updated = diff_info.copy()
        old_content = updated.get('old_content')
        new_content = updated.get('new_content')

        old_trimmed, old_has_tabs = trim_content_before_tabs_panel(old_content)
        new_trimmed, new_has_tabs = trim_content_before_tabs_panel(new_content)

        updated['old_content'] = old_trimmed
        updated['new_content'] = new_trimmed

        if old_has_tabs or new_has_tabs:
            # Tell downstream updater to replace only content before this marker.
            updated['target_end_marker'] = '<TabsPanel'

        op = updated.get('operation', '')
        old_norm = (updated.get('old_content') or '').strip()
        new_norm = (updated.get('new_content') or '').strip()

        # Drop no-op entries after trimming to avoid empty/identical updates.
        if op == 'modified' and old_norm == new_norm:
            dropped += 1
            continue
        if op == 'added' and not new_norm:
            dropped += 1
            continue
        if op == 'deleted' and not old_norm:
            dropped += 1
            continue

        normalized[key] = updated

    return normalized, dropped


def is_structural_added_heading_prefix_line(line):
    """Return True for added wrapper lines that belong to the next heading."""
    stripped = (line or "").strip()
    return re.match(r'^<CustomContent\b[^>]*>\s*$', stripped) is not None


def is_structural_mdx_wrapper_line(line):
    """Return True for standalone MDX component boundary lines."""
    stripped = (line or "").strip()
    if not stripped:
        return False
    if stripped.endswith("/>"):
        return False
    return re.match(r'^</?[A-Z][A-Za-z0-9]*(?:\s+[^<>]*)?>\s*$', stripped) is not None


def collect_added_heading_prefix_lines(file_lines, operations):
    """Collect structural added lines immediately before added headings.

    These lines are wrappers for the new section, not content changes in the
    preceding existing section. Return both a mapping of added heading line to
    prefix lines and the line numbers to ignore for modified-section detection.
    """
    added_line_numbers = {
        entry.get("line_number")
        for entry in operations.get("added_lines", [])
        if entry.get("line_number") is not None
    }
    added_heading_lines = sorted(
        entry.get("line_number")
        for entry in operations.get("added_lines", [])
        if entry.get("is_header") and entry.get("line_number") is not None
    )

    prefix_lines_by_heading = {}
    ignored_line_numbers = set()

    for heading_line in added_heading_lines:
        prefixes = []
        candidate_line = heading_line - 1
        consecutive_blank_lines = 0

        while candidate_line in added_line_numbers and 1 <= candidate_line <= len(file_lines):
            raw_line = file_lines[candidate_line - 1]
            stripped_line = raw_line.strip()

            if not stripped_line:
                # Allow a single separator blank around a wrapper/heading pair,
                # but do not sweep unbounded blank-only additions into the prefix.
                if consecutive_blank_lines >= 1:
                    break
                consecutive_blank_lines += 1
            elif is_structural_added_heading_prefix_line(raw_line):
                consecutive_blank_lines = 0
            else:
                break

            prefixes.append(raw_line.rstrip("\r"))
            ignored_line_numbers.add(candidate_line)
            candidate_line -= 1

        if prefixes:
            content_prefixes = list(reversed(prefixes))
            while content_prefixes and not content_prefixes[0].strip():
                content_prefixes.pop(0)
            if content_prefixes:
                prefix_lines_by_heading[heading_line] = content_prefixes

    return prefix_lines_by_heading, ignored_line_numbers


def find_previous_heading_line(line_number, all_headers):
    """Return the nearest heading line strictly before line_number."""
    previous = None
    for header_line in sorted(all_headers.keys()):
        if header_line >= line_number:
            break
        previous = header_line
    return previous


def collect_structural_wrapper_boundary_sections(operations, all_headers, ignored_added_lines=None):
    """Find existing sections whose direct ranges changed only by wrapper tags.

    A standalone MDX wrapper can sit between headings, for example just before a
    heading or just after a wrapped section. In that case normal "containing
    section" lookup can attach the change to the next heading and later filter
    it out because that section's direct content is unchanged. Marking the
    previous section preserves the boundary edit as a regular modified range.
    """
    ignored_added_lines = ignored_added_lines or set()
    boundary_sections = set()

    for operation_key in ("added_lines", "deleted_lines", "modified_lines"):
        for entry in operations.get(operation_key, []):
            if entry.get("is_header"):
                continue
            if not is_structural_mdx_wrapper_line(entry.get("content", "")):
                continue

            if operation_key == "deleted_lines":
                boundary_line = get_head_line_number(entry)
            else:
                boundary_line = entry.get("line_number")
                if operation_key == "added_lines" and boundary_line in ignored_added_lines:
                    continue

            if boundary_line is None:
                continue

            section_line = find_previous_heading_line(boundary_line, all_headers)
            if section_line is not None:
                boundary_sections.add(section_line)

    return boundary_sections

def find_previous_section_for_added(added_sections, hierarchy_dict):
    """Find the previous section hierarchy for each added section group"""
    insertion_points = {}
    
    if not added_sections:
        return insertion_points
    
    # Group consecutive added sections
    added_list = sorted(list(added_sections))
    groups = []
    current_group = [added_list[0]]
    
    for i in range(1, len(added_list)):
        if added_list[i] - added_list[i-1] <= 10:  # Consider sections within 10 lines as consecutive
            current_group.append(added_list[i])
        else:
            groups.append(current_group)
            current_group = [added_list[i]]
    groups.append(current_group)
    
    # For each group, find the previous section hierarchy
    for group in groups:
        first_new_section = min(group)
        
        # Find the section that comes before this group
        previous_section_line = None
        previous_section_hierarchy = None
        
        for line_num_str in sorted(hierarchy_dict.keys(), key=int):
            line_num = int(line_num_str)
            if line_num < first_new_section:
                previous_section_line = line_num
                previous_section_hierarchy = hierarchy_dict[line_num_str]
            else:
                break
        
        if previous_section_hierarchy:
            insertion_points[f"group_{groups.index(group)}"] = {
                'previous_section_hierarchy': previous_section_hierarchy,
                'previous_section_line': previous_section_line,
                'new_sections': group,
                'insertion_type': 'multiple' if len(group) > 1 else 'single'
            }
            print(f"   📍 Added section group: {len(group)} sections after '{previous_section_hierarchy}'")
        else:
            print(f"   ⚠️  Could not find previous section for added sections starting at line {first_new_section}")
    
    return insertion_points

def build_source_diff_dict(modified_sections, added_sections, deleted_sections, all_hierarchy_dict, base_hierarchy_dict, operations, file_content, base_file_content):
    """Build source diff dictionary with correct structure for matching"""
    source_diff_dict = {}
    
    # Check if intro section has changes
    file_lines = split_normalized_lines(file_content)
    base_file_lines = split_normalized_lines(base_file_content)
    added_heading_prefix_lines, _ = collect_added_heading_prefix_lines(file_lines, operations)
    has_intro_changes, first_level2_line = detect_intro_section_changes(operations, file_lines)
    
    if has_intro_changes:
        print(f"   🎯 Pre-section changes detected (before line {first_level2_line})")
        
        # Independently detect frontmatter changes and intro_section changes.
        # frontmatter  = line 1 → line before first # heading
        # intro_section = first # heading → line before first ## heading
        first_heading = find_first_heading_line(file_lines)
        base_first_heading = find_first_heading_line(base_file_lines)
        
        fm_end = first_heading if first_heading else len(file_lines) + 1
        base_fm_end = base_first_heading if base_first_heading else len(base_file_lines) + 1
        
        has_fm_changes = has_changes_in_range(
            operations, 1, fm_end, 1, base_fm_end)
        
        intro_start = first_heading if first_heading else first_level2_line
        base_intro_start = base_first_heading if base_first_heading else first_level2_line
        has_intro_body_changes = has_changes_in_range(
            operations, intro_start, first_level2_line, base_intro_start, first_level2_line)
        
        if has_fm_changes:
            print(f"   📋 Frontmatter changes detected (before first # heading)")
            new_fm = extract_frontmatter_content(file_lines)
            old_fm = extract_frontmatter_content(base_file_lines)
            source_diff_dict["frontmatter"] = {
                "new_line_number": 0,
                "original_hierarchy": "frontmatter",
                "operation": "modified",
                "new_content": new_fm,
                "old_content": old_fm
            }
            print(f"   ✅ Frontmatter section added to diff dict")
        
        if has_intro_body_changes:
            print(f"   📝 Intro section changes detected (# to ##)")
            new_intro, intro_start_line, _ = extract_intro_section_content(file_lines)
            old_intro, base_intro_start_line, _ = extract_intro_section_content(base_file_lines)
            source_diff_dict["intro_section"] = {
                "new_line_number": intro_start_line,
                "original_hierarchy": "intro_section",
                "operation": "modified",
                "new_content": new_intro,
                "old_content": old_intro
            }
            print(f"   ✅ Intro section added to diff dict (from line {intro_start_line})")
        
        # Filter out all sections that are within intro section range
        # These will be handled as part of the intro section
        def is_in_intro_section(line_num):
            """Check if a line number is within the intro section"""
            return line_num < first_level2_line
        
        # Filter modified sections
        modified_sections = {k: v for k, v in modified_sections.items() 
                           if not is_in_intro_section(int(k) if k != "0" else 0)}
        
        # Filter added sections
        added_sections = {k: v for k, v in added_sections.items() 
                         if not is_in_intro_section(int(k))}
        
        # Filter deleted sections
        deleted_sections = {k: v for k, v in deleted_sections.items() 
                           if not is_in_intro_section(int(k))}
        
        print(f"   🔍 After filtering intro section: {len(modified_sections)} modified, {len(added_sections)} added, {len(deleted_sections)} deleted")
    
    # Helper function to extract section content (only direct content, no sub-sections)
    def extract_section_content_for_diff(line_num, hierarchy_dict):
        if str(line_num) == "0":
            # Handle frontmatter
            return extract_frontmatter_content(file_lines)
        else:
            return extract_section_direct_content(file_lines, line_num)
    
    # Helper function to extract old content from base file (only direct content, no sub-sections)
    def extract_old_content_for_diff(line_num, base_hierarchy_dict, base_file_content):
        if str(line_num) == "0":
            # Handle frontmatter from base file
            return extract_frontmatter_content(base_file_lines)
        else:
            return extract_section_direct_content(base_file_lines, line_num)
    
    # Helper function to extract old content by hierarchy (for modified sections that may have moved)
    def extract_old_content_by_hierarchy(original_hierarchy, base_hierarchy_dict, base_file_content):
        """Extract old content by finding the section with matching hierarchy in base file (only direct content)"""
        if original_hierarchy == "frontmatter":
            return extract_frontmatter_content(base_file_lines)
        
        # Find the line number in base file that matches the original hierarchy
        for base_line_num_str, base_hierarchy in base_hierarchy_dict.items():
            if base_hierarchy == original_hierarchy:
                base_line_num = int(base_line_num_str) if base_line_num_str != "0" else 0
                if base_line_num == 0:
                    return extract_frontmatter_content(base_file_lines)
                else:
                    return extract_section_direct_content(base_file_lines, base_line_num)
        
        # If exact match not found, return empty string
        print(f"   ⚠️  Could not find matching hierarchy in base file: {original_hierarchy}")
        return ""

    # Full-section fallback for nested/sub-section edits that direct-content extraction can miss
    def extract_old_full_content_by_hierarchy(original_hierarchy, base_hierarchy_dict, base_file_content):
        """Extract old content by hierarchy including sub-sections"""
        if original_hierarchy == "frontmatter":
            return extract_frontmatter_content(base_file_lines)

        for base_line_num, base_hierarchy in base_hierarchy_dict.items():
            if base_hierarchy == original_hierarchy:
                if base_line_num == 0:
                    return extract_frontmatter_content(base_file_lines)
                return extract_section_content(base_file_lines, base_line_num, base_hierarchy_dict)

        return ""
    
    # Helper function to build complete hierarchy for a section using base file info
    def build_complete_original_hierarchy(line_num, current_hierarchy, base_hierarchy_dict, operations):
        """Build complete hierarchy path for original section"""
        line_num_str = str(line_num)
        
        # Special cases: frontmatter and top-level titles
        if line_num_str == "0":
            return "frontmatter"
        
        # Check if this line was modified and has original content
        for modified_line in operations.get('modified_lines', []):
            if (modified_line.get('is_header') and 
                modified_line.get('line_number') == line_num and 
                'original_content' in modified_line):
                original_line = modified_line['original_content'].strip()
                
                # For top-level titles, return the original content directly
                if ' > ' not in current_hierarchy:
                    return original_line
                
                # For nested sections, build the complete hierarchy using original content
                # Find the hierarchy path using base hierarchy dict and replace the leaf with original
                if line_num in base_hierarchy_dict:
                    base_hierarchy = base_hierarchy_dict[line_num]
                    if ' > ' in base_hierarchy:
                        # Replace the leaf (last part) with original content
                        hierarchy_parts = base_hierarchy.split(' > ')
                        hierarchy_parts[-1] = original_line
                        return ' > '.join(hierarchy_parts)
                    else:
                        # Single level, return original content
                        return original_line
                
                # Fallback: return original content
                return original_line
        
        # If not modified, use base hierarchy if available
        if line_num_str in base_hierarchy_dict:
            return base_hierarchy_dict[line_num_str]
        
        # If not found in base (new section), use current hierarchy
        return current_hierarchy
    
    # Process modified sections
    for line_num_str, hierarchy in modified_sections.items():
        line_num = int(line_num_str) if line_num_str != "0" else 0
        
        # Build complete original hierarchy
        original_hierarchy = build_complete_original_hierarchy(line_num, hierarchy, base_hierarchy_dict, operations)
        
        # Extract both old and new content
        new_content = extract_section_content_for_diff(line_num, all_hierarchy_dict)
        # Use hierarchy-based lookup for old content instead of line number
        old_content = extract_old_content_by_hierarchy(original_hierarchy, base_hierarchy_dict, base_file_content)
        
        # Only include if content actually changed.
        # Fallback to full-section comparison to catch nested-content edits.
        if new_content == old_content:
            new_full_content = extract_section_content(file_lines, line_num, all_hierarchy_dict)
            old_full_content = extract_old_full_content_by_hierarchy(original_hierarchy, base_hierarchy_dict, base_file_content)
            if new_full_content != old_full_content:
                new_content = new_full_content
                old_content = old_full_content

        if new_content != old_content:
            # Check if this is a bottom modified section (no next section in base file)
            is_bottom_modified = False
            if line_num in base_hierarchy_dict:
                # Get all sections in base file sorted by line number
                base_sections = sorted([(int(ln), hier) for ln, hier in base_hierarchy_dict.items() if ln != "0"])
                
                # Check if there's any section after this line in base file
                has_next_section = any(base_line > line_num for base_line, _ in base_sections)
                
                if not has_next_section:
                    is_bottom_modified = True
                    print(f"   ✅ Bottom modified section detected at line {line_num_str}: no next section in base file")
            
            # Use special marker for bottom modified sections
            if is_bottom_modified:
                final_original_hierarchy = f"bottom-modified-{line_num}"
            else:
                final_original_hierarchy = original_hierarchy
            
            modified_entry = {
                "new_line_number": line_num,
                "original_hierarchy": final_original_hierarchy,
                "operation": "modified",
                "new_content": new_content,
                "old_content": old_content
            }
            if is_bottom_modified:
                modified_entry["matching_hierarchy"] = original_hierarchy

            source_diff_dict[f"modified_{line_num_str}"] = modified_entry
            print(f"   ✅ Real modification detected at line {line_num_str}: content changed")
        else:
            print(f"   🚫 Filtered out false positive at line {line_num_str}: content unchanged (likely line shift artifact)")
    
    # Process added sections - find next section from current document hierarchy
    for line_num_str, hierarchy in added_sections.items():
        line_num = int(line_num_str)
        
        print(f"   🔍 Finding next section for added section at line {line_num}: {hierarchy}")
        
        # Strategy: Find the next section directly from the current document (post-PR)
        # Get all current sections sorted by line number
        current_sections = sorted([(int(ln), curr_hierarchy) for ln, curr_hierarchy in all_hierarchy_dict.items()])
        print(f"   📋 Current sections around line {line_num}: {[(ln, h.split(' > ')[-1] if ' > ' in h else h) for ln, h in current_sections if abs(ln - line_num) <= 15]}")
        
        next_section_original_hierarchy = None
        
        # Find the next section that comes after the added section in the current document
        for curr_line_num, curr_hierarchy in current_sections:
            if curr_line_num > line_num:
                # Found the next section in current document
                # Now find its original hierarchy in base document
                curr_line_str = str(curr_line_num)
                
                # Get the original hierarchy for this next section
                # Use the same logic as build_complete_original_hierarchy to get original content
                if curr_line_str in base_hierarchy_dict:
                    # Check if this section was modified
                    was_modified = False
                    for modified_line in operations.get('modified_lines', []):
                        if (modified_line.get('is_header') and 
                            modified_line.get('line_number') == curr_line_num and 
                            'original_content' in modified_line):
                            # This section was modified, use original content
                            original_line = modified_line['original_content'].strip()
                            base_hierarchy = base_hierarchy_dict[curr_line_str]
                            
                            if ' > ' in base_hierarchy:
                                # Replace the leaf with original content
                                hierarchy_parts = base_hierarchy.split(' > ')
                                hierarchy_parts[-1] = original_line
                                next_section_original_hierarchy = ' > '.join(hierarchy_parts)
                            else:
                                next_section_original_hierarchy = original_line
                            
                            print(f"   ✅ Found next section (modified): line {curr_line_num} -> {next_section_original_hierarchy.split(' > ')[-1] if ' > ' in next_section_original_hierarchy else next_section_original_hierarchy}")
                            was_modified = True
                            break
                    
                    if not was_modified:
                        # Section was not modified, use base hierarchy directly
                        next_section_original_hierarchy = base_hierarchy_dict[curr_line_str]
                        print(f"   ✅ Found next section (unchanged): line {curr_line_num} -> {next_section_original_hierarchy.split(' > ')[-1] if ' > ' in next_section_original_hierarchy else next_section_original_hierarchy}")
                    
                    break
                else:
                    # This next section might also be new or modified
                    # Try to find it by content matching in base hierarchy
                    curr_leaf = curr_hierarchy.split(' > ')[-1] if ' > ' in curr_hierarchy else curr_hierarchy
                    curr_clean = clean_title_for_matching(curr_leaf)

                    leaf_match_candidates = []
                    for base_line_str, base_hierarchy in base_hierarchy_dict.items():
                        base_leaf = base_hierarchy.split(' > ')[-1] if ' > ' in base_hierarchy else base_hierarchy
                        base_clean = clean_title_for_matching(base_leaf)
                        if curr_clean == base_clean:
                            leaf_match_candidates.append((base_line_str, base_hierarchy))

                    if len(leaf_match_candidates) == 1:
                        next_section_original_hierarchy = leaf_match_candidates[0][1]
                        print(f"   ✅ Found next section (by content): {leaf_match_candidates[0][1].split(' > ')[-1] if ' > ' in leaf_match_candidates[0][1] else leaf_match_candidates[0][1]}")
                        break
                    elif len(leaf_match_candidates) > 1:
                        # Multiple candidates share the same leaf title (e.g. "Step 3"
                        # under different parent sections). Disambiguate by comparing
                        # the parent hierarchy path from the HEAD file against each
                        # candidate in the base file.
                        curr_parents = curr_hierarchy.split(' > ')[:-1] if ' > ' in curr_hierarchy else []
                        scored_candidates = []

                        for cand_line, cand_hierarchy in leaf_match_candidates:
                            cand_parents = cand_hierarchy.split(' > ')[:-1] if ' > ' in cand_hierarchy else []
                            score = 0
                            for i in range(1, min(len(curr_parents), len(cand_parents)) + 1):
                                if clean_title_for_matching(curr_parents[-i]) == clean_title_for_matching(cand_parents[-i]):
                                    score += 1
                                else:
                                    break
                            scored_candidates.append((score, cand_line, cand_hierarchy))

                        max_score = max(sc for sc, _, _ in scored_candidates)
                        if max_score > 0:
                            top_tier = [(cl, ch) for sc, cl, ch in scored_candidates if sc == max_score]
                            if len(top_tier) == 1:
                                best_candidate = top_tier[0]
                            else:
                                # Tie at the same positive score: break by ordinal
                                # position among same-titled headings.  Comparing
                                # raw BASE vs HEAD line numbers is unreliable when
                                # insertions/deletions shift the file, so instead we
                                # match the Nth occurrence in HEAD to the Nth in BASE.
                                head_same_title = sorted([
                                    (int(ln), h) for ln, h in all_hierarchy_dict.items()
                                    if clean_title_for_matching(
                                        h.split(' > ')[-1] if ' > ' in h else h
                                    ) == curr_clean
                                ])
                                curr_ordinal = next(
                                    (idx for idx, (ln, _) in enumerate(head_same_title)
                                     if ln == curr_line_num),
                                    0,
                                )

                                base_same_title = sorted([
                                    (int(ln), h) for ln, h in base_hierarchy_dict.items()
                                    if clean_title_for_matching(
                                        h.split(' > ')[-1] if ' > ' in h else h
                                    ) == curr_clean
                                ])
                                base_ordinal_map = {
                                    str(ln): idx
                                    for idx, (ln, _) in enumerate(base_same_title)
                                }

                                top_tier.sort(
                                    key=lambda x: abs(
                                        base_ordinal_map.get(x[0], len(base_same_title))
                                        - curr_ordinal
                                    )
                                )
                                best_candidate = top_tier[0]
                                print(
                                    f"   ⚠️  {len(top_tier)} candidates tied at parent-hierarchy score {max_score} "
                                    f"for '{curr_clean}'; chose base line {best_candidate[0]} by ordinal position "
                                    f"(HEAD ordinal {curr_ordinal}, BASE ordinal {base_ordinal_map.get(best_candidate[0], '?')})"
                                )
                            next_section_original_hierarchy = best_candidate[1]
                            leaf_for_log = best_candidate[1].split(' > ')[-1] if ' > ' in best_candidate[1] else best_candidate[1]
                            print(f"   ✅ Found next section (by content, disambiguated among {len(leaf_match_candidates)} candidates): {leaf_for_log}")
                            break
                        else:
                            print(f"   ⚠️  {len(leaf_match_candidates)} candidates for '{curr_clean}' but parent hierarchy could not disambiguate, continuing search...")
                    
                    print(f"   ⚠️  Next section at line {curr_line_num} not found in base, continuing search...")
        
        # If no next section found, this is being added at the end
        if not next_section_original_hierarchy:
            print(f"   ✅ Bottom section detected: this section is added at the end of document")
            # Use special marker for bottom added sections - no matching needed
            next_section_original_hierarchy = f"bottom-added-{line_num}"
        
        new_content = extract_section_content_for_diff(line_num, all_hierarchy_dict)
        prefix_lines = added_heading_prefix_lines.get(line_num, [])
        if prefix_lines:
            new_content = "\n".join(prefix_lines + ([new_content] if new_content is not None else []))

        source_diff_dict[f"added_{line_num_str}"] = {
            "new_line_number": line_num,
            "original_hierarchy": next_section_original_hierarchy,
            "operation": "added",
            "new_content": new_content,
            "old_content": None  # Added sections have no old content
        }
    
    # Process deleted sections - use original hierarchy from base file
    for line_num_str, hierarchy in deleted_sections.items():
        line_num = int(line_num_str)
        # Use complete hierarchy from base file
        original_hierarchy = base_hierarchy_dict.get(line_num_str, hierarchy)
        
        # Extract old content for deleted sections
        old_content = extract_old_content_for_diff(line_num, base_hierarchy_dict, base_file_content)
        
        source_diff_dict[f"deleted_{line_num_str}"] = {
            "new_line_number": line_num,
            "original_hierarchy": original_hierarchy,
            "operation": "deleted",
            "new_content": None,  # No new content for deleted sections
            "old_content": old_content  # Show what was deleted
        }
    
    # Sort the dictionary by new_line_number for better readability
    sorted_items = sorted(source_diff_dict.items(), key=lambda x: x[1]['new_line_number'])
    source_diff_dict = dict(sorted_items)
    
    return source_diff_dict

def analyze_source_changes(
    source_context_or_pr_url,
    github_client,
    special_files=None,
    ignore_files=None,
    repo_configs=None,
    max_non_system_sections=120,
    pr_diff=None,
    exclude_folders=None,
    source_files=None,
):
    """Analyze source language changes and categorize them as added, modified, or deleted
    
    Args:
        exclude_folders: list of folder names to skip entirely (e.g. ["tidb-cloud", "ai"])
    """
    # Import modules needed in this function
    import os
    import json
    from toc_processor import process_toc_operations
    from keword_processor import find_tabs_region, parse_letter_blocks, diff_changed_letters
    from image_processor import is_image_file
    
    if exclude_folders is None:
        exclude_folders = []
    requested_source_files = _normalize_requested_source_files(source_files)
    
    if is_diff_context(source_context_or_pr_url):
        source_context = source_context_or_pr_url
        source_repo = source_context["source_repo"]
        repository = github_client.get_repo(source_repo)
        base_ref = source_context["base_ref"]
        head_ref = source_context["head_ref"]
        files = [
            file if isinstance(file, DiffFile) else normalize_changed_file(file)
            for file in source_context["changed_files"]
        ]
        repo_config = source_context.get("repo_config") or get_repo_config_from_source_repo(
            source_repo,
            repo_configs,
            target_repo_override=source_context.get("target_repo"),
        )
        source_description = source_context.get("source_description") or f"compare {base_ref}...{head_ref}"
        print(f"📋 Processing diff: {source_description}")
        print_range_scope_summary(source_context)
    else:
        source_context = build_pr_diff_context(
            source_context_or_pr_url,
            github_client,
            repo_configs,
        )
        source_repo = source_context["source_repo"]
        repository = github_client.get_repo(source_repo)
        base_ref = source_context["base_ref"]
        head_ref = source_context["head_ref"]
        repo_config = source_context["repo_config"]
        files = [
            file if isinstance(file, DiffFile) else normalize_changed_file(file)
            for file in source_context["changed_files"]
        ]
        if source_context.get("source_range_url"):
            print(f"📋 Processing diff: {source_context['source_description']}")
        else:
            print(f"📋 Processing PR #{source_context['pr_number']}: {source_context['title']}")
        print_range_scope_summary(source_context)
    
    # Separate markdown files and image files
    markdown_files = [f for f in files if f.filename.endswith('.md')]
    image_files = [f for f in files if is_image_file(f.filename)]
    
    print(f"📄 Found {len(markdown_files)} markdown files")
    print(f"🖼️  Found {len(image_files)} image files")
    
    if exclude_folders:
        def _is_excluded(path):
            return any(path.startswith(folder + "/") or path == folder for folder in exclude_folders)
        
        excluded_md = [f for f in markdown_files if _is_excluded(f.filename)]
        excluded_img = [f for f in image_files if _is_excluded(f.filename)]
        markdown_files = [f for f in markdown_files if not _is_excluded(f.filename)]
        image_files = [f for f in image_files if not _is_excluded(f.filename)]
        
        if excluded_md or excluded_img:
            print(f"🚫 Early exclusion: skipped {len(excluded_md)} markdown + {len(excluded_img)} image files under {exclude_folders}")
            for f in excluded_md:
                print(f"   ⏭️  {f.filename}")
            for f in excluded_img:
                print(f"   ⏭️  {f.filename}")

    if requested_source_files:
        markdown_files = [f for f in markdown_files if f.filename in requested_source_files]
        image_files = [f for f in image_files if f.filename in requested_source_files]
        print(
            "📄 SOURCE_FILES early filter applied before diff analysis: "
            f"{', '.join(sorted(requested_source_files))}"
        )

    # Return dictionaries for different operation types
    added_sections = {}      # New sections that were added
    modified_sections = {}   # Existing sections that were modified  
    deleted_sections = {}    # Sections that were deleted
    added_files = {}         # Completely new files that were added
    deleted_files = []       # Completely deleted files
    ignored_files = []       # Files that were ignored
    toc_files = {}           # Special TOC files requiring special processing
    keyword_files = {}       # Special keyword files requiring keyword-specific processing
    
    # Image-related returns
    added_images = []        # New image files that were added
    modified_images = []     # Image files that were modified
    deleted_images = []      # Image files that were deleted
    restructured_files = set()  # Modified files rerouted to full translation
    
    for file in markdown_files:
        print(f"\n🔍 Analyzing {file.filename}")
        
        # Check if this file should be ignored
        if file.filename in ignore_files:
            print(f"   ⏭️  Skipping ignored file: {file.filename}")
            ignored_files.append(file.filename)
            continue
        
        # Check if this is a completely new file or deleted file
        if file.status == 'added':
            print(f"   ➕ Detected new file: {file.filename}")
            try:
                file_content = repository.get_contents(file.filename, ref=head_ref).decoded_content.decode('utf-8')
                if should_ignore_related_resources_resource_card_sections(source_context):
                    file_content, skipped_sections = remove_related_resources_resource_card_sections(file_content)
                    if skipped_sections:
                        print(
                            f"   🧹 Skipped {len(skipped_sections)} RelatedResources section(s) "
                            "from added commit-mode file"
                        )
                        for section in skipped_sections:
                            print(f"      - {section['hierarchy']}")
                    if not file_content.strip():
                        print(
                            "   ⏭️  Skipping added file because no translatable content remains "
                            "after RelatedResources filtering"
                        )
                        continue
                added_files[file.filename] = file_content
                print(f"   ✅ Added complete file for translation")
                continue
            except Exception as e:
                print(f"   ❌ Error getting new file content: {sanitize_exception_message(e)}")
                continue
        
        elif file.status == 'removed':
            print(f"   🗑️  Detected deleted file: {file.filename}")
            deleted_files.append(file.filename)
            print(f"   ✅ Marked file for deletion")
            continue

        elif file.status == 'renamed':
            previous_filename = getattr(file, 'previous_filename', None)
            if previous_filename:
                print(f"   🔄 Detected renamed file: {previous_filename} -> {file.filename}")
                deleted_files.append(previous_filename)
            else:
                print(f"   🔄 Detected renamed file without previous path: {file.filename}")

            try:
                file_content = repository.get_contents(file.filename, ref=head_ref).decoded_content.decode('utf-8')
                if should_ignore_related_resources_resource_card_sections(source_context):
                    file_content, skipped_sections = remove_related_resources_resource_card_sections(file_content)
                    if skipped_sections:
                        print(
                            f"   🧹 Skipped {len(skipped_sections)} RelatedResources section(s) "
                            "from renamed commit-mode file"
                        )
                        for section in skipped_sections:
                            print(f"      - {section['hierarchy']}")
                    if not file_content.strip():
                        print(
                            "   ⏭️  Skipping renamed file because no translatable content remains "
                            "after RelatedResources filtering"
                        )
                        continue
                added_files[file.filename] = file_content
                print(f"   ✅ Treating renamed markdown as delete old + add new")
            except Exception as e:
                print(f"   ❌ Error getting renamed file content: {sanitize_exception_message(e)}")
            continue
        
        # For modified files, check if it's a special file like TOC.md
        try:
            file_content = repository.get_contents(file.filename, ref=head_ref).decoded_content.decode('utf-8')
        except Exception as e:
            print(f"   ❌ Error getting content: {sanitize_exception_message(e)}")
            continue
        
        basename = os.path.basename(file.filename)
        operations = None
        base_file_content_preloaded = None
        keyword_regular_only = False

        special_files = special_files or []
        is_keyword_file = basename == "keywords.md" and basename in special_files
        is_toc_file = is_toc_file_name(file.filename, ignore_files)

        # Check if this is a special file requiring dedicated processing
        if is_keyword_file or is_toc_file:
            
            # --- keywords.md: keyword-specific processor ---
            if is_keyword_file:
                print(f"   📋 Detected keyword file: {file.filename}")
                operations = analyze_diff_operations(file)

                source_head_lines = split_normalized_lines(file_content)
                try:
                    base_file_content_preloaded = repository.get_contents(file.filename, ref=base_ref).decoded_content.decode('utf-8')
                except Exception as e:
                    print(
                        f"   ⚠️  Could not get base keywords.md content: {sanitize_exception_message(e)}"
                    )
                    base_file_content_preloaded = file_content

                source_base_lines = split_normalized_lines(base_file_content_preloaded)

                head_tabs_region = find_tabs_region(source_head_lines)
                base_tabs_region = find_tabs_region(source_base_lines)

                head_blocks = parse_letter_blocks(source_head_lines, head_tabs_region)
                base_blocks = parse_letter_blocks(source_base_lines, base_tabs_region)
                changed_letters = diff_changed_letters(base_blocks, head_blocks)

                tabs_changes = {}
                if changed_letters:
                    target_blocks = {}
                    target_blocks_source = "none"

                    try:
                        target_file_content, target_blocks_source = get_target_file_content(
                            file.filename,
                            github_client,
                            repo_config['target_repo'],
                            target_local_path=repo_config.get('target_local_path'),
                            prefer_local_target_for_read=bool(
                                repo_config.get('prefer_local_target_for_read', False)
                            ),
                            target_ref=repo_config.get('target_ref'),
                        )
                        target_lines = split_normalized_lines(target_file_content)
                        target_tabs_region = find_tabs_region(target_lines)
                        target_blocks = parse_letter_blocks(target_lines, target_tabs_region)

                        if not target_blocks and target_blocks_source.startswith("local:"):
                            target_file_content, target_blocks_source = get_target_file_content(
                                file.filename,
                                github_client,
                                repo_config['target_repo'],
                                target_ref=repo_config.get('target_ref'),
                            )
                            target_lines = split_normalized_lines(target_file_content)
                            target_tabs_region = find_tabs_region(target_lines)
                            target_blocks = parse_letter_blocks(target_lines, target_tabs_region)
                    except Exception as e:
                        print(
                            f"   ⚠️  Could not get target keyword file content for tabs changes: {sanitize_exception_message(e)}"
                        )
                        target_blocks = {}

                    print(f"   📌 Keyword target baseline source: {target_blocks_source}")

                    for letter, change_data in changed_letters.items():
                        tabs_changes[letter] = {
                            "source_old_block": change_data.get("source_old_block"),
                            "source_new_block": change_data.get("source_new_block"),
                            "source_diff": change_data.get("source_diff", ""),
                            "target_old_block": target_blocks.get(letter, {}).get("content")
                        }

                if tabs_changes:
                    keyword_files[file.filename] = {
                        "type": "keyword",
                        "tabs_changes": tabs_changes
                    }
                    print(f"   📋 Keyword TabsPanel operations queued: {sorted(tabs_changes.keys())}")
                else:
                    print(f"   ℹ️  No TabsPanel letter changes found")

                def line_in_tabs_region(line_number, region):
                    if not region:
                        return False
                    if not line_number:
                        return False
                    # line_number is 1-based; region end_idx is 0-based exclusive.
                    return (region["start_idx"] + 1) <= line_number <= region["end_idx"]

                filtered_operations = {
                    "added_lines": [
                        line for line in operations["added_lines"]
                        if not line_in_tabs_region(line.get("line_number"), head_tabs_region)
                    ],
                    "modified_lines": [
                        line for line in operations["modified_lines"]
                        if not line_in_tabs_region(line.get("line_number"), head_tabs_region)
                    ],
                    "deleted_lines": [
                        line for line in operations["deleted_lines"]
                        if not line_in_tabs_region(line.get("line_number"), base_tabs_region)
                    ]
                }

                print(
                    f"   🧹 Filtered non-tabs diff lines: "
                    f"{len(filtered_operations['added_lines'])} added, "
                    f"{len(filtered_operations['modified_lines'])} modified, "
                    f"{len(filtered_operations['deleted_lines'])} deleted"
                )

                operations = filtered_operations
                if not any([
                    filtered_operations["added_lines"],
                    filtered_operations["modified_lines"],
                    filtered_operations["deleted_lines"]
                ]):
                    print(f"   ⏭️  No non-tabs changes in keywords.md, skipping regular section processing")
                    continue

                keyword_regular_only = True

            if is_toc_file:
                # --- TOC files: TOC-specific processor ---
                print(f"   📋 Detected special file: {file.filename}")

                # Get target file content for comparison
                try:
                    target_file_content, target_source = get_target_file_content(
                        file.filename,
                        github_client,
                        repo_config['target_repo'],
                        target_local_path=repo_config.get('target_local_path'),
                        prefer_local_target_for_read=bool(
                            repo_config.get('prefer_local_target_for_read', False)
                        ),
                        target_ref=repo_config.get('target_ref'),
                    )
                    target_lines = split_normalized_lines(target_file_content)
                    print(f"   📌 TOC target baseline source: {target_source}")
                except Exception as e:
                    print(f"   ⚠️  Could not get target file content: {sanitize_exception_message(e)}")
                    continue

                # Analyze diff operations for TOC.md
                operations = analyze_diff_operations(file)
                source_lines = split_normalized_lines(file_content)

                try:
                    source_base_content = repository.get_contents(file.filename, ref=base_ref).decoded_content.decode('utf-8')
                except Exception as e:
                    print(f"   ⚠️  Could not get base TOC content: {sanitize_exception_message(e)}")
                    source_base_content = None

                has_toc_diff = any([
                    operations['added_lines'],
                    operations['modified_lines'],
                    operations['deleted_lines']
                ])

                use_snapshot_sync = (
                    source_context.get("mode") == "commit"
                    and has_toc_diff
                    and source_base_content is not None
                )

                if use_snapshot_sync:
                    # Use full source snapshots for TOC files. This handles
                    # unlinked TOC group rows and moved nested sections more
                    # reliably than operation-by-operation insertion.
                    toc_files[file.filename] = {
                        'type': 'toc',
                        'operations': [],
                        'source_base_content': source_base_content,
                        'source_head_content': file_content,
                        'source_added_line_numbers': [
                            line['line_number']
                            for line in operations['added_lines']
                        ]
                    }
                    print(f"   📋 TOC snapshot sync queued for processing")
                else:
                    # Fallback to legacy operation-level processing.
                    toc_results = process_toc_operations(
                        file.filename,
                        operations,
                        source_lines,
                        target_lines,
                        "",
                        source_base_lines=(
                            split_normalized_lines(source_base_content)
                            if source_base_content is not None
                            else None
                        ),
                    )  # Local path will be determined later

                    # Store TOC operations for later processing
                    if any([toc_results['added'], toc_results['modified'], toc_results['deleted']]):
                        # Combine all operations for processing
                        all_toc_operations = []
                        all_toc_operations.extend(toc_results['added'])
                        all_toc_operations.extend(toc_results['modified'])
                        all_toc_operations.extend(toc_results['deleted'])

                        # Add to special TOC processing queue (separate from regular sections)
                        toc_files[file.filename] = {
                            'type': 'toc',
                            'operations': all_toc_operations
                        }

                        print(f"   📋 TOC operations queued for processing:")
                        if toc_results['added']:
                            print(f"      ➕ Added: {len(toc_results['added'])} entries")
                        if toc_results['modified']:
                            print(f"      ✏️  Modified: {len(toc_results['modified'])} entries")
                        if toc_results['deleted']:
                            print(f"      ❌ Deleted: {len(toc_results['deleted'])} entries")
                    else:
                        print(f"   ℹ️  No TOC operations found")
                
                continue  # Skip regular processing for special files
        
        # Analyze diff operations
        if operations is None:
            operations = analyze_diff_operations(file)
        print(f"   📝 Diff analysis: {len(operations['added_lines'])} added, {len(operations['modified_lines'])} modified, {len(operations['deleted_lines'])} deleted lines")
        
        lines = split_normalized_lines(file_content)
        all_headers = {}
        
        # Track code block state
        in_code_block = False
        code_block_delimiter = None
        
        # First pass: collect all headers (excluding those in code blocks)
        for line_num, line in enumerate(lines, 1):
            original_line = line
            line = line.strip()
            
            # Check for code block delimiters
            if line.startswith('```') or line.startswith('~~~'):
                if not in_code_block:
                    # Entering a code block
                    in_code_block = True
                    code_block_delimiter = line[:3]
                    continue
                elif line.startswith(code_block_delimiter):
                    # Exiting a code block
                    in_code_block = False
                    code_block_delimiter = None
                    continue
            
            # Skip processing if we're inside a code block
            if in_code_block:
                continue
            
            # Process headers only if not in code block
            if is_markdown_heading(original_line):
                match = re.match(r'^(#{1,10})\s+(.+)', line)
                if match:
                    level = len(match.group(1))
                    title = match.group(2).strip()
                    all_headers[line_num] = {
                        'level': level,
                        'title': title,
                        'line': line
                    }
        
        # Build complete hierarchy from HEAD (after changes)
        all_hierarchy_dict = build_hierarchy_dict(file_content)
        
        # For deletion/modification detection, compare against the diff base ref.
        try:
            if base_file_content_preloaded is not None:
                base_file_content = base_file_content_preloaded
            else:
                base_file_content = repository.get_contents(file.filename, ref=base_ref).decoded_content.decode('utf-8')
            base_hierarchy_dict = build_hierarchy_dict(base_file_content)
        except Exception as e:
            print(f"   ⚠️  Could not get base file content: {sanitize_exception_message(e)}")
            base_hierarchy_dict = all_hierarchy_dict
            base_file_content = file_content  # Fallback to current content

        operations = maybe_use_normalized_snapshot_operations(
            operations,
            base_file_content,
            file_content,
        )
        print(f"   📝 Effective diff analysis: {len(operations['added_lines'])} added, {len(operations['modified_lines'])} modified, {len(operations['deleted_lines'])} deleted lines")

        # Detect restructured documents in commit mode: if ALL sub-section
        # headings are in the diff, route the file to full translation instead
        # of incremental section-level processing.
        if (
            source_context.get("mode") == "commit"
            and detect_restructured_file(file_content, base_file_content, operations)
        ):
            print(f"   🔄 Detected restructured document: all sub-section headings are in the diff")
            print(f"   🔄 Routing {file.filename} to full translation instead of incremental processing")
            restructured_content = file_content
            if should_ignore_related_resources_resource_card_sections(source_context):
                restructured_content, skipped_sections = remove_related_resources_resource_card_sections(
                    restructured_content
                )
                if skipped_sections:
                    print(
                        f"   🧹 Skipped {len(skipped_sections)} RelatedResources section(s) "
                        "from restructured file"
                    )
                    for section in skipped_sections:
                        print(f"      - {section['hierarchy']}")
                if not restructured_content.strip():
                    print(
                        "   ⏭️  Skipping restructured file because no translatable "
                        "content remains after RelatedResources filtering"
                    )
                    continue
            added_files[file.filename] = restructured_content
            restructured_files.add(file.filename)
            print(f"   ✅ Queued restructured file for full translation")
            continue

        # Find sections by operation type with corrected logic
        sections_by_type = find_sections_by_operation_type(lines, operations, all_headers, base_hierarchy_dict)
        
        # Prioritize modified headers over added ones (fix for header changes like --host -> --hosts)
        modified_header_lines = set()
        for modified_line in operations['modified_lines']:
            if modified_line['is_header']:
                modified_header_lines.add(modified_line['line_number'])
        
        # Remove modified header lines from added set
        sections_by_type['added'] = sections_by_type['added'] - modified_header_lines
        
        # Enhanced logic: check for actual content changes within sections
        # This helps detect changes in section content (not just headers)
        print(f"   🔍 Enhanced detection: checking for actual section content changes...")
        
        # Get only lines that have actual content changes (exclude headers)
        real_content_changes = set()
        _, structural_added_prefix_lines = collect_added_heading_prefix_lines(lines, operations)
        structural_wrapper_boundary_sections = collect_structural_wrapper_boundary_sections(
            operations,
            all_headers,
            ignored_added_lines=structural_added_prefix_lines,
        )
        
        # Added lines (new content, excluding headers)
        for added_line in operations['added_lines']:
            if not added_line['is_header'] and added_line['line_number'] not in structural_added_prefix_lines:
                real_content_changes.add(added_line['line_number'])
        
        # Deleted lines (removed content, excluding headers)
        for deleted_line in operations['deleted_lines']:
            if not deleted_line['is_header']:
                head_line_number = get_head_line_number(deleted_line)
                if head_line_number is not None:
                    real_content_changes.add(head_line_number)
        
        # Modified lines (changed content, excluding headers)
        for modified_line in operations['modified_lines']:
            if not modified_line['is_header']:
                real_content_changes.add(modified_line['line_number'])
        
        print(f"   📝 Real content changes (non-header): {sorted(real_content_changes)}")
        
        # Find sections that contain actual content changes
        content_affected_sections = set()
        for changed_line in real_content_changes:
            # Find which section this changed line belongs to
            containing_section = None
            for line_num in sorted(all_headers.keys()):
                if line_num <= changed_line:
                    containing_section = line_num
                else:
                    break
            
            if containing_section and containing_section not in sections_by_type['added']:
                # Additional check: make sure this is not just a line number shift
                # Only add if the change is within reasonable distance from the section header
                # AND if the changed line is not part of a completely deleted section header
                is_deleted_header = False
                for deleted_line in operations['deleted_lines']:
                    deleted_head_line = get_head_line_number(deleted_line)
                    if (deleted_line['is_header'] and 
                        deleted_head_line is not None and
                        abs(changed_line - deleted_head_line) <= 2):
                        is_deleted_header = True
                        print(f"   ⚠️  Skipping change at line {changed_line} (deleted header near line {deleted_head_line})")
                        break
                
                # More precise filtering: check if this change is actually meaningful
                # Skip changes that are part of deleted content or line shifts due to deletions
                should_include = True
                
                # Skip exact deleted headers
                for deleted_line in operations['deleted_lines']:
                    deleted_head_line = get_head_line_number(deleted_line)
                    if (deleted_line['is_header'] and 
                        changed_line == deleted_head_line):
                        should_include = False
                        print(f"   ⚠️  Skipping change at line {changed_line} (exact deleted header)")
                        break
                
                # Skip changes that are very close to deleted content AND far from their containing section
                # This helps filter out line shift artifacts while keeping real content changes
                if should_include:
                    for deleted_line in operations['deleted_lines']:
                        deleted_head_line = get_head_line_number(deleted_line)
                        if deleted_head_line is None:
                            continue
                        # Only skip if both conditions are met:
                        # 1. Very close to deleted content (within 5 lines)
                        # 2. The change is far from its containing section (likely a shift artifact)
                        distance_to_deletion = abs(changed_line - deleted_head_line)
                        distance_to_section = changed_line - containing_section
                        
                        if (distance_to_deletion <= 5 and distance_to_section > 100):
                            should_include = False
                            print(f"   ⚠️  Skipping change at line {changed_line} (likely line shift: {distance_to_deletion} lines from deletion, {distance_to_section} from section)")
                            break

                if should_include: # filtering legitimate changes in large sections or at section ends
                    content_affected_sections.add(containing_section)
                    distance_to_section = changed_line - containing_section
                    #print(f"   📝 Content change at line {changed_line} affects section at line {containing_section} (distance: {distance_to_section})")
        
        # Add content-modified sections to the modified set, but exclude sections that are already marked as added or deleted
        for line_num in sorted(structural_wrapper_boundary_sections):
            if (
                line_num not in sections_by_type['added']
                and line_num not in sections_by_type['deleted']
            ):
                if line_num not in sections_by_type['modified']:
                    print(f"   🧩 Added structural-wrapper-modified section at line {line_num}")
                sections_by_type['modified'].add(line_num)

        for line_num in content_affected_sections:
            if (line_num not in sections_by_type['modified'] and 
                line_num not in sections_by_type['added'] and
                line_num not in sections_by_type['deleted']):  # ✅ Critical fix: exclude deleted sections      
                sections_by_type['modified'].add(line_num)
                print(f"   📝 Added content-modified section at line {line_num}")
            elif line_num in sections_by_type['deleted']:
                print(f"   🚫 Skipping content-modified section at line {line_num}: already marked as deleted")
        
        # Prepare sections data for source_diff_dict
        file_modified = {}
        file_added = {}
        file_deleted = {}
        
        # Build modified sections
        for line_num in sections_by_type['modified']:
            if line_num in all_hierarchy_dict:
                file_modified[str(line_num)] = all_hierarchy_dict[line_num]
        
        # Build added sections  
        for line_num in sections_by_type['added']:
            if line_num in all_hierarchy_dict:
                file_added[str(line_num)] = all_hierarchy_dict[line_num]
        
        # Build deleted sections
        for line_num in sections_by_type['deleted']:
            if line_num in base_hierarchy_dict:
                file_deleted[str(line_num)] = base_hierarchy_dict[line_num]
        
        print(f"   📊 Real content changes: {sorted(real_content_changes)}")
        
        # Build source diff dictionary
        source_diff_dict = build_source_diff_dict(
            file_modified, file_added, file_deleted, 
            all_hierarchy_dict, base_hierarchy_dict, 
            operations, file_content, base_file_content
        )

        if basename == "keywords.md" and keyword_regular_only:
            source_diff_dict, dropped_entries = normalize_keywords_regular_source_diff(source_diff_dict)
            print(
                f"   🔧 keywords.md regular diff normalized: "
                f"{len(source_diff_dict)} kept, {dropped_entries} dropped"
            )
            if not source_diff_dict:
                print(f"   ⏭️  No non-tabs section content remains after normalization")
                continue

        if should_ignore_related_resources_resource_card_sections(source_context):
            source_diff_dict, skipped_related_entries = filter_related_resources_resource_card_source_diff(
                source_diff_dict
            )
            if skipped_related_entries:
                print(
                    f"   🧹 Skipped {len(skipped_related_entries)} RelatedResources section(s) "
                    "from commit-mode diff analysis"
                )
                for key, diff_info in skipped_related_entries:
                    print(
                        f"      - {key}: {diff_info.get('original_hierarchy', '')}"
                    )
                if not source_diff_dict:
                    print(
                        "   ⏭️  No translatable section changes remain after "
                        "RelatedResources filtering"
                    )
                    continue
        
        # Breakpoint: Output source_diff_dict to file for review with file prefix
        
        # Ensure temp_output directory exists
        script_dir = os.path.dirname(os.path.abspath(__file__))
        temp_dir = os.path.join(script_dir, "temp_output")
        os.makedirs(temp_dir, exist_ok=True)
        
        file_prefix = file.filename.replace('/', '-').replace('.md', '')
        output_file = os.path.join(temp_dir, f"{file_prefix}-source-diff-dict.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(source_diff_dict, f, ensure_ascii=False, indent=2)
        
        print(f"   💾 Saved source diff dictionary to: {output_file}")
        print(f"   📊 Source diff dictionary contains {len(source_diff_dict)} sections:")
        for key, diff_info in source_diff_dict.items():
            print(f"      {diff_info['operation']}: {key} -> original_hierarchy: {diff_info['original_hierarchy']}")
        
        # source-diff-dict.json generation is complete, continue to next step in main.py
        
        # For modified headers, we need to build a mapping using original titles for matching
        original_hierarchy_dict = all_hierarchy_dict.copy()
        
        # Update hierarchy dict to use original content for modified headers when needed for matching
        for line_num in sections_by_type['modified']:
            if line_num in all_headers:
                header_info = all_headers[line_num]
                # Check if this header was modified and has original content
                for op in operations['modified_lines']:
                    if (op['is_header'] and 
                        op['line_number'] == line_num and 
                        'original_content' in op):
                        # Create hierarchy path using original content for matching
                        original_line = op['original_content'].strip()
                        if original_line.startswith('#'):
                            # Build original hierarchy for matching
                            original_hierarchy = build_hierarchy_for_modified_section(
                                file_content, line_num, original_line, all_hierarchy_dict)
                            if original_hierarchy:
                                original_hierarchy_dict[line_num] = original_hierarchy
                        break
        
        # Process added sections
        if sections_by_type['added']:
            file_added = {}
            # Find insertion points using the simplified logic: 
            # Record the previous section hierarchy for each added section
            insertion_points = find_previous_section_for_added(sections_by_type['added'], all_hierarchy_dict)
            
            # Get actual content for added sections
            for line_num in sections_by_type['added']:
                if line_num in all_hierarchy_dict:
                    file_added[str(line_num)] = all_hierarchy_dict[line_num]
            
            # Get source sections content (actual content, not just hierarchy)
            if file_added:
                source_sections_content = get_source_sections_content(source_context, file.filename, file_added, github_client)
                file_added = source_sections_content  # Replace hierarchy with actual content
            
            if file_added:
                added_sections[file.filename] = {
                    'sections': file_added,
                    'insertion_points': insertion_points
                }
                print(f"   ➕ Found {len(file_added)} added sections with {len(insertion_points)} insertion points")
        
        # Process modified sections
        if sections_by_type['modified']:
            file_modified = {}
            for line_num in sections_by_type['modified']:
                if line_num in original_hierarchy_dict:
                    file_modified[str(line_num)] = original_hierarchy_dict[line_num]
            
            if file_modified:
                modified_entry = {
                    'sections': file_modified,
                    'original_hierarchy': original_hierarchy_dict,
                    'current_hierarchy': all_hierarchy_dict
                }
                if basename == "keywords.md" and keyword_regular_only:
                    modified_entry['keyword_regular_only'] = True
                modified_sections[file.filename] = modified_entry
                print(f"   ✏️  Found {len(file_modified)} modified sections")
        
        # Process deleted sections  
        if sections_by_type['deleted']:
            file_deleted = {}
            for line_num in sections_by_type['deleted']:
                # Use base hierarchy to get the deleted section info
                if line_num in base_hierarchy_dict:
                    file_deleted[str(line_num)] = base_hierarchy_dict[line_num]
            
            if file_deleted:
                deleted_sections[file.filename] = file_deleted
                print(f"   ❌ Found {len(file_deleted)} deleted sections")
        
        # Enhanced logic: also check content-level changes using legacy detection
        # This helps detect changes in section content (not just headers)
        print(f"   🔍 Enhanced detection: checking content-level changes...")
        changed_lines = [
            line_number
            for line_number in get_changed_line_numbers_from_operations(operations)
            if line_number not in structural_added_prefix_lines
        ]
        affected_sections = find_affected_sections(lines, changed_lines, all_headers)
        
        legacy_modified = {}
        for line_num in affected_sections:
            if line_num in sections_by_type['added'] or line_num in sections_by_type['deleted']:
                continue

            if line_num in all_hierarchy_dict:
                section_hierarchy = all_hierarchy_dict[line_num]
                # Only add if not already detected by operation-type analysis
                already_detected = False
                if file.filename in modified_sections:
                    for existing_line, existing_hierarchy in modified_sections[file.filename].get('sections', {}).items():
                        if existing_hierarchy == section_hierarchy:
                            already_detected = True
                            break
                
                if not already_detected:
                    legacy_modified[str(line_num)] = section_hierarchy
        
        if legacy_modified:
            print(f"   ✅ Enhanced detection found {len(legacy_modified)} additional content-modified sections")
            # Merge with existing modified sections
            if file.filename in modified_sections:
                # Merge the sections
                existing_sections = modified_sections[file.filename].get('sections', {})
                existing_sections.update(legacy_modified)
                modified_sections[file.filename]['sections'] = existing_sections
            else:
                # Create new entry
                modified_entry = {
                    'sections': legacy_modified,
                    'original_hierarchy': all_hierarchy_dict,
                    'current_hierarchy': all_hierarchy_dict
                }
                if basename == "keywords.md" and keyword_regular_only:
                    modified_entry['keyword_regular_only'] = True
                modified_sections[file.filename] = modified_entry
        
        # Ensure files with source_diff_dict-only work still enter the regular
        # modified-file processor. Downstream processing loads source_diff_dict
        # and handles both modified and added sections from there; without this
        # synthetic queue entry, a file with only newly added sections would be
        # analyzed but never translated.
        if source_diff_dict and file.filename not in modified_sections:
            synthetic_sections = {}
            for key, diff_info in source_diff_dict.items():
                operation = diff_info.get("operation")
                # Deleted sections do not need translation work in the
                # regular modified-file queue.
                if operation not in ("added", "modified"):
                    continue

                if key == "frontmatter":
                    synthetic_sections["0"] = "frontmatter"
                    continue
                if key == "intro_section":
                    synthetic_sections["intro_section"] = "intro_section"
                    continue

                line_number = diff_info.get("new_line_number")
                if line_number is None:
                    continue

                hierarchy = all_hierarchy_dict.get(
                    int(line_number),
                    diff_info.get("original_hierarchy", ""),
                )
                synthetic_sections[str(line_number)] = hierarchy

            if synthetic_sections:
                modified_sections[file.filename] = {
                    'sections': synthetic_sections,
                    'original_hierarchy': all_hierarchy_dict,
                    'current_hierarchy': all_hierarchy_dict
                }
                print(f"   ✏️  Added file to modified_sections via source_diff_dict-only work: {list(synthetic_sections.values())}")
    
    # Process image files
    print(f"\n🖼️  Analyzing image files...")
    for file in image_files:
        print(f"\n🔍 Analyzing image: {file.filename}")
        
        # Check if this file should be ignored
        if file.filename in ignore_files:
            print(f"   ⏭️  Skipping ignored image: {file.filename}")
            ignored_files.append(file.filename)
            continue
        
        # Categorize image operations based on file status
        if file.status == 'added':
            print(f"   ➕ Detected new image: {file.filename}")
            added_images.append(file.filename)
        elif file.status == 'removed':
            print(f"   🗑️  Detected deleted image: {file.filename}")
            deleted_images.append(file.filename)
        elif file.status == 'modified':
            print(f"   🔄 Detected modified image: {file.filename}")
            modified_images.append(file.filename)
        elif file.status == 'renamed':
            # Renamed images are treated as delete old + add new
            print(f"   🔄 Detected renamed image: {file.previous_filename} -> {file.filename}")
            if hasattr(file, 'previous_filename') and file.previous_filename:
                deleted_images.append(file.previous_filename)
            added_images.append(file.filename)
    
    print(f"\n📊 Summary:")
    #print(f"   ✏️  Modified files: {} files") 
    print(f"   📄 Added files: {len(added_files)} files")
    print(f"   🗑️  Deleted files: {len(deleted_files)} files")
    print(f"   📋 TOC files: {len(toc_files)} files")
    print(f"   📋 Keyword files: {len(keyword_files)} files")
    print(f"   🖼️  Added images: {len(added_images)} images")
    print(f"   🖼️  Modified images: {len(modified_images)} images")
    print(f"   🖼️  Deleted images: {len(deleted_images)} images")
    if ignored_files:
        print(f"   ⏭️  Ignored files: {len(ignored_files)} files")
        for ignored_file in ignored_files:
            print(f"      - {ignored_file}")
    
    return added_sections, modified_sections, deleted_sections, added_files, deleted_files, toc_files, keyword_files, added_images, modified_images, deleted_images, restructured_files
