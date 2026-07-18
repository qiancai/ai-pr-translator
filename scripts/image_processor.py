"""
Image Processor Module
Handles processing of image files (add, delete, modify) in target repository
"""

import os
import threading
from github import Github
from file_io import atomic_write_bytes
from log_sanitizer import sanitize_exception_message, safe_target_path
from workflow_outcome import FileOutcomes

# Thread-safe printing
print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    """Thread-safe print function"""
    with print_lock:
        print(*args, **kwargs)

# Common image file extensions
IMAGE_EXTENSIONS = ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.bmp', '.ico']

def is_image_file(filename):
    """Check if a file is an image based on its extension"""
    _, ext = os.path.splitext(filename.lower())
    return ext in IMAGE_EXTENSIONS

def download_image_from_source(file_path, source_context_or_pr_url, github_client):
    """Download image content from source repository"""
    from diff_analyzer import get_source_file_bytes
    
    try:
        return get_source_file_bytes(
            file_path,
            source_context_or_pr_url,
            github_client,
            ref_name="head_ref",
        )
    except Exception as e:
        thread_safe_print(
            f"   ❌ Error downloading image from source: {sanitize_exception_message(e)}"
        )
        return None

def process_added_images(added_images, source_context_or_pr_url, github_client, repo_config):
    """Process newly added image files by copying them to target repository"""
    if not added_images:
        thread_safe_print("\n🖼️  No new images to process")
        return FileOutcomes()
    
    thread_safe_print(f"\n🖼️  Processing {len(added_images)} newly added images...")
    
    target_local_path = repo_config['target_local_path']
    outcomes = FileOutcomes()
    
    for file_path in added_images:
        thread_safe_print(f"\n📸 Processing new image: {file_path}")
        
        # Create target file path
        try:
            target_file_path = safe_target_path(target_local_path, file_path)
        except ValueError as e:
            reason = sanitize_exception_message(e)
            thread_safe_print(f"   ❌ {reason}")
            outcomes.add(file_path, "failed", reason)
            continue
        target_dir = os.path.dirname(target_file_path)
        
        # Create directory if it doesn't exist
        if not os.path.exists(target_dir):
            try:
                os.makedirs(target_dir, exist_ok=True)
                thread_safe_print(f"   📁 Created directory: {target_dir}")
            except Exception as e:
                reason = f"Failed to create target directory: {sanitize_exception_message(e)}"
                thread_safe_print(f"   ❌ {reason}")
                outcomes.add(file_path, "failed", reason)
                continue
        
        # Check if file already exists
        if os.path.exists(target_file_path):
            thread_safe_print(f"   ⚠️  Target image already exists: {target_file_path}")
            # For modified images, we want to overwrite
            thread_safe_print(f"   🔄 Overwriting existing image...")
        
        # Download image from source
        image_data = download_image_from_source(file_path, source_context_or_pr_url, github_client)
        
        if image_data is None:
            thread_safe_print(f"   ❌ Failed to download image: {file_path}")
            outcomes.add(file_path, "failed", "Failed to download image")
            continue
        
        # Write image data to target file
        try:
            atomic_write_bytes(target_file_path, image_data)
            
            thread_safe_print(f"   ✅ Saved image to: {target_file_path}")
            outcomes.add(file_path, "success")
            
        except Exception as e:
            thread_safe_print(
                f"   ❌ Error saving image {target_file_path}: {sanitize_exception_message(e)}"
            )
            outcomes.add(file_path, "failed", sanitize_exception_message(e))
    
    thread_safe_print(f"\n✅ Completed processing all new images")
    return outcomes

def process_modified_images(modified_images, source_context_or_pr_url, github_client, repo_config):
    """Process modified image files by replacing them in target repository"""
    if not modified_images:
        thread_safe_print("\n🖼️  No modified images to process")
        return FileOutcomes()
    
    thread_safe_print(f"\n🖼️  Processing {len(modified_images)} modified images...")
    
    target_local_path = repo_config['target_local_path']
    outcomes = FileOutcomes()
    
    for file_path in modified_images:
        thread_safe_print(f"\n🔄 Processing modified image: {file_path}")
        
        # Create target file path
        try:
            target_file_path = safe_target_path(target_local_path, file_path)
        except ValueError as e:
            reason = sanitize_exception_message(e)
            thread_safe_print(f"   ❌ {reason}")
            outcomes.add(file_path, "failed", reason)
            continue
        target_dir = os.path.dirname(target_file_path)
        
        # Create directory if it doesn't exist (in case of path changes)
        if not os.path.exists(target_dir):
            try:
                os.makedirs(target_dir, exist_ok=True)
                thread_safe_print(f"   📁 Created directory: {target_dir}")
            except Exception as e:
                reason = f"Failed to create target directory: {sanitize_exception_message(e)}"
                thread_safe_print(f"   ❌ {reason}")
                outcomes.add(file_path, "failed", reason)
                continue
        
        # Download updated image from source
        image_data = download_image_from_source(file_path, source_context_or_pr_url, github_client)
        
        if image_data is None:
            thread_safe_print(f"   ❌ Failed to download modified image: {file_path}")
            outcomes.add(file_path, "failed", "Failed to download modified image")
            continue
        
        # Write updated image data to target file (overwrite)
        try:
            atomic_write_bytes(target_file_path, image_data)
            
            thread_safe_print(f"   ✅ Updated image: {target_file_path}")
            outcomes.add(file_path, "success")
            
        except Exception as e:
            thread_safe_print(
                f"   ❌ Error updating image {target_file_path}: {sanitize_exception_message(e)}"
            )
            outcomes.add(file_path, "failed", sanitize_exception_message(e))
    
    thread_safe_print(f"\n✅ Completed processing all modified images")
    return outcomes

def process_deleted_images(deleted_images, repo_config):
    """Process deleted image files by removing them from target repository"""
    if not deleted_images:
        thread_safe_print("\n🖼️  No images to delete")
        return FileOutcomes()
    
    thread_safe_print(f"\n🖼️  Processing {len(deleted_images)} deleted images...")
    
    target_local_path = repo_config['target_local_path']
    outcomes = FileOutcomes()
    
    for file_path in deleted_images:
        thread_safe_print(f"\n🗑️  Processing deleted image: {file_path}")
        
        # Create target file path
        try:
            target_file_path = safe_target_path(target_local_path, file_path)
        except ValueError as e:
            reason = sanitize_exception_message(e)
            thread_safe_print(f"   ❌ {reason}")
            outcomes.add(file_path, "failed", reason)
            continue
        
        # Check if file exists in target
        if os.path.exists(target_file_path):
            try:
                os.remove(target_file_path)
                thread_safe_print(f"   ✅ Deleted image: {target_file_path}")
                outcomes.add(file_path, "success")
            except Exception as e:
                thread_safe_print(
                    f"   ❌ Error deleting image {target_file_path}: {sanitize_exception_message(e)}"
                )
                outcomes.add(file_path, "failed", sanitize_exception_message(e))
        else:
            thread_safe_print(f"   ⚠️  Target image not found: {target_file_path}")
            outcomes.add(file_path, "skipped", "Target image already absent")
    
    thread_safe_print(f"\n✅ Completed processing deleted images")
    return outcomes

def process_all_images(added_images, modified_images, deleted_images, source_context_or_pr_url, github_client, repo_config):
    """Process all image operations: add, modify, and delete"""
    thread_safe_print("\n" + "="*80)
    thread_safe_print("🖼️  IMAGE PROCESSING")
    thread_safe_print("="*80)
    
    # Process in order: delete first, then add/modify
    outcomes = FileOutcomes()
    outcomes.update(process_deleted_images(deleted_images, repo_config))
    outcomes.update(process_added_images(added_images, source_context_or_pr_url, github_client, repo_config))
    outcomes.update(process_modified_images(modified_images, source_context_or_pr_url, github_client, repo_config))
    
    thread_safe_print("\n" + "="*80)
    thread_safe_print("✅ IMAGE PROCESSING COMPLETED")
    thread_safe_print("="*80)
    return outcomes
