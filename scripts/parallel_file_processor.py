"""Helpers for optional file-level parallel processing."""

import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from ai_client import thread_safe_print
from log_sanitizer import sanitize_exception_message


DEFAULT_PARALLEL_FILE_THRESHOLD = 10
DEFAULT_PARALLEL_FILE_WORKERS = 3


def _positive_int_from_env(name, default):
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError:
        thread_safe_print(f"⚠️  Invalid {name}={value!r}; using default {default}")
        return default
    return parsed if parsed > 0 else default


def get_parallel_file_threshold():
    return _positive_int_from_env("DIFF_PARALLEL_FILE_THRESHOLD", DEFAULT_PARALLEL_FILE_THRESHOLD)


def get_parallel_file_workers():
    return _positive_int_from_env("DIFF_PARALLEL_WORKERS", DEFAULT_PARALLEL_FILE_WORKERS)


def should_parallelize_file_processing(diff_file_count):
    return diff_file_count > get_parallel_file_threshold() and get_parallel_file_workers() > 1


def count_unique_file_paths(*collections):
    paths = set()
    for collection in collections:
        if not collection:
            continue
        if isinstance(collection, dict):
            paths.update(path for path in collection.keys() if path)
        else:
            paths.update(path for path in collection if path)
    return len(paths)


def make_file_task(file_path, func, resource_key=None):
    return {
        "file_path": file_path,
        "resource_key": resource_key or file_path,
        "func": func,
    }


def make_task_result(status, reason=""):
    return {
        "status": status,
        "reason": reason,
    }


def has_duplicate_task_resources(tasks):
    seen = set()
    duplicates = set()
    for task in tasks:
        key = task.get("resource_key") or task.get("file_path")
        if key in seen:
            duplicates.add(key)
        seen.add(key)
    return duplicates


def _split_into_chunks(items, chunk_count):
    if not items:
        return []
    chunk_count = max(1, min(chunk_count, len(items)))
    chunk_size = math.ceil(len(items) / chunk_count)
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def _run_task(index, task):
    file_path = task["file_path"]
    try:
        return {
            "index": index,
            "file_path": file_path,
            "ok": True,
            "result": task["func"](),
            "error": None,
        }
    except Exception as e:
        return {
            "index": index,
            "file_path": file_path,
            "ok": False,
            "result": None,
            "error": sanitize_exception_message(e),
        }


def _run_chunk(chunk_index, indexed_tasks):
    thread_safe_print(f"   ⚡ Chunk {chunk_index}: processing {len(indexed_tasks)} file(s)")
    return [_run_task(index, task) for index, task in indexed_tasks]


def _chunk_failure_results(indexed_tasks, error):
    return [
        {
            "index": index,
            "file_path": task["file_path"],
            "ok": False,
            "result": None,
            "error": error,
        }
        for index, task in indexed_tasks
    ]


def run_file_tasks(tasks, group_name, parallel_enabled=False, max_workers=None):
    if not tasks:
        return []

    indexed_tasks = list(enumerate(tasks))

    if not parallel_enabled or len(tasks) == 1:
        return [_run_task(index, task) for index, task in indexed_tasks]

    duplicate_resources = has_duplicate_task_resources(tasks)
    if duplicate_resources:
        thread_safe_print(
            "⚠️  Parallel processing disabled because multiple tasks share output resources: "
            + ", ".join(sorted(duplicate_resources))
        )
        return [_run_task(index, task) for index, task in indexed_tasks]

    worker_count = min(max_workers or get_parallel_file_workers(), len(tasks))
    chunks = _split_into_chunks(indexed_tasks, worker_count)
    thread_safe_print(
        f"\n⚡ Parallel processing enabled for {group_name}: "
        f"{len(tasks)} file(s) split into {len(chunks)} chunk(s)"
    )

    results = []
    with ThreadPoolExecutor(max_workers=len(chunks), thread_name_prefix="FileChunk") as executor:
        future_to_chunk = {
            executor.submit(_run_chunk, chunk_index, chunk): (chunk_index, chunk)
            for chunk_index, chunk in enumerate(chunks, 1)
        }
        for future in as_completed(future_to_chunk):
            chunk_index, chunk = future_to_chunk[future]
            try:
                chunk_results = future.result()
                results.extend(chunk_results)
            except Exception as e:
                error = sanitize_exception_message(e)
                thread_safe_print(f"   ❌ Chunk {chunk_index} failed: {error}")
                results.extend(_chunk_failure_results(chunk, error))

    return sorted(results, key=lambda item: item["index"])
