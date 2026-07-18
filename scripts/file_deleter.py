"""
File Deleter Module
Handles processing of deleted files and deleted sections
"""

import os
import threading
from github import Github
from log_sanitizer import sanitize_exception_message, safe_target_path
from workflow_outcome import FileOutcomes

# Thread-safe printing
print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def process_deleted_files(deleted_files, github_client, repo_config, return_outcomes=False):
    """Process deleted files by removing them from target repository.

    Returns True if all deletions succeeded, False otherwise.
    """
    if not deleted_files:
        thread_safe_print("\n🗑️  No files to delete")
        return FileOutcomes() if return_outcomes else True
    
    thread_safe_print(f"\n🗑️  Processing {len(deleted_files)} deleted files...")
    
    target_local_path = repo_config['target_local_path']
    all_success = True
    outcomes = FileOutcomes()
    
    for file_path in deleted_files:
        thread_safe_print(f"\n🗑️  Processing deleted file: {file_path}")
        try:
            target_file_path = safe_target_path(target_local_path, file_path)
        except ValueError as e:
            thread_safe_print(f"   ❌ {sanitize_exception_message(e)}")
            all_success = False
            outcomes.add(file_path, "failed", sanitize_exception_message(e))
            continue
        
        # Check if file exists in target
        if os.path.exists(target_file_path):
            try:
                os.remove(target_file_path)
                thread_safe_print(f"   ✅ Deleted file: {target_file_path}")
                outcomes.add(file_path, "success")
            except Exception as e:
                thread_safe_print(
                    f"   ❌ Error deleting file {target_file_path}: {sanitize_exception_message(e)}"
                )
                all_success = False
                outcomes.add(file_path, "failed", sanitize_exception_message(e))
        else:
            thread_safe_print(f"   ⚠️  Target file not found: {target_file_path}")
            outcomes.add(file_path, "skipped", "Target file already absent")
    
    thread_safe_print(f"\n✅ Completed processing deleted files")
    return outcomes if return_outcomes else all_success

# Section deletion logic moved to file_updater.py
