"""Utilities for identifying special documentation files and source scopes."""

import os


def find_heading_line_indices(lines, is_heading):
    """Return the indices of markdown heading lines, skipping fenced code blocks.

    ``lines`` is a pre-split list of lines and ``is_heading`` is the predicate
    used to recognize a heading line, injected so callers can supply their own
    heading detection.  Centralizes the code-fence tracking that would otherwise
    be duplicated wherever headings are scanned.
    """
    indices = []
    in_code_block = False
    code_block_delimiter = None
    for index, raw_line in enumerate(lines):
        stripped = raw_line.strip()
        if stripped.startswith("```") or stripped.startswith("~~~"):
            if not in_code_block:
                in_code_block = True
                code_block_delimiter = stripped[:3]
            elif stripped.startswith(code_block_delimiter):
                in_code_block = False
                code_block_delimiter = None
            continue
        if in_code_block:
            continue
        if is_heading(raw_line):
            indices.append(index)
    return indices


def _normalize_rel_path(path):
    return (path or "").strip().lstrip("/")


def path_resource_key(file_path, strip_markdown_extension=True):
    """Return a collision-free, readable key for a repository-relative path."""
    value = str(file_path or "").replace("\\", "/")
    if strip_markdown_extension and value.endswith(".md"):
        value = value[:-3]
    return value.replace("%", "%25").replace("--", "%2D%2D").replace("/", "--")


def is_toc_file_name(filename, ignore_files=None):
    """Return True for TOC files that need TOC-specific processing."""
    normalized_filename = _normalize_rel_path(filename)
    basename = os.path.basename(normalized_filename)
    ignored = {_normalize_rel_path(path) for path in (ignore_files or [])}
    ignored_basenames = {os.path.basename(path) for path in ignored}

    if normalized_filename in ignored or basename in ignored_basenames:
        return False

    return basename == "TOC.md" or basename.startswith("TOC-")


def is_index_file_name(filename, ignore_files=None):
    """Return True for _index.md files that need index-specific processing."""
    normalized_filename = _normalize_rel_path(filename)
    basename = os.path.basename(normalized_filename)
    ignored = {_normalize_rel_path(path) for path in (ignore_files or [])}
    ignored_basenames = {os.path.basename(path) for path in ignored}

    if normalized_filename in ignored or basename in ignored_basenames:
        return False

    return basename == "_index.md"


def is_learning_path_index_content(content):
    """Return True when the _index.md content should use the special index
    processor (snapshot-sync) instead of regular section-based translation.

    Returns True when the file body (outside frontmatter and code blocks)
    contains no markdown headings (``#``).  Heading-less _index.md files —
    whether they use ``<LearningPathContainer>`` components or are link-only
    landing pages — cannot be reliably processed by heading-based section
    matching and need the dedicated snapshot-sync path.
    """
    if not content:
        return False

    in_frontmatter = False
    frontmatter_fence_count = 0
    in_code_block = False

    for line in content.split("\n"):
        stripped = line.strip()

        if stripped == "---":
            frontmatter_fence_count += 1
            in_frontmatter = frontmatter_fence_count == 1
            if frontmatter_fence_count == 2:
                in_frontmatter = False
            continue

        if in_frontmatter:
            continue

        if stripped.startswith("```") or stripped.startswith("~~~"):
            in_code_block = not in_code_block
            continue

        if in_code_block:
            continue

        if stripped.startswith("#"):
            return False

    return True


def source_scope_includes_folder(folder_name, source_folder=None, source_files=None):
    """Return True when source filters include a folder path."""
    folder = _normalize_rel_path(folder_name).strip("/")
    if not folder:
        return False

    normalized_source_folder = _normalize_rel_path(source_folder).strip("/")
    if normalized_source_folder == folder:
        return True

    for item in (source_files or "").split(","):
        rel = _normalize_rel_path(item).strip("/")
        if rel == folder or rel.startswith(folder + "/"):
            return True

    return False
