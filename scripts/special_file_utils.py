"""Utilities for identifying special documentation files and source scopes."""

import os


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
