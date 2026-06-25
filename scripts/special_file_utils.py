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


def is_toc_file_name(filename, ignore_files=None):
    """Return True for TOC files that need TOC-specific processing."""
    normalized_filename = _normalize_rel_path(filename)
    basename = os.path.basename(normalized_filename)
    ignored = {_normalize_rel_path(path) for path in (ignore_files or [])}
    ignored_basenames = {os.path.basename(path) for path in ignored}

    if normalized_filename in ignored or basename in ignored_basenames:
        return False

    return basename == "TOC.md" or basename.startswith("TOC-")


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
