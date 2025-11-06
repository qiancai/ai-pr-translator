"""
Image Processor Module
Handles processing of image files (add, delete, modify) in target repository
"""

import os
import threading
from github import Github

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

def download_image_from_source(file_path, pr_url, github_client):
    """Download image content from source repository"""
    from pr_analyzer import parse_pr_url
    
    try:
        owner, repo, pr_number = parse_pr_url(pr_url)
        repository = github_client.get_repo(f"{owner}/{repo}")
        pr = repository.get_pull(pr_number)
        
        # Get the image file content from PR head (after changes)
        file_content = repository.get_contents(file_path, ref=pr.head.sha)
        
        # For images, we need the binary content
        image_data = file_content.decoded_content
        
        return image_data
        
    except Exception as e:
        thread_safe_print(f"   ❌ Error downloading image from source: {e}")
        return None

def process_added_images(added_images, pr_url, github_client, repo_config):
    """Process newly added image files by copying them to target repository"""
    if not added_images:
        thread_safe_print("\n🖼️  No new images to process")
        return
    
    thread_safe_print(f"\n🖼️  Processing {len(added_images)} newly added images...")
    
    target_local_path = repo_config['target_local_path']
    
    for file_path in added_images:
        thread_safe_print(f"\n📸 Processing new image: {file_path}")
        
        # Create target file path
        target_file_path = os.path.join(target_local_path, file_path)
        target_dir = os.path.dirname(target_file_path)
        
        # Create directory if it doesn't exist
        if not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)
            thread_safe_print(f"   📁 Created directory: {target_dir}")
        
        # Check if file already exists
        if os.path.exists(target_file_path):
            thread_safe_print(f"   ⚠️  Target image already exists: {target_file_path}")
            # For modified images, we want to overwrite
            thread_safe_print(f"   🔄 Overwriting existing image...")
        
        # Download image from source
        image_data = download_image_from_source(file_path, pr_url, github_client)
        
        if image_data is None:
            thread_safe_print(f"   ❌ Failed to download image: {file_path}")
            continue
        
        # Write image data to target file
        try:
            with open(target_file_path, 'wb') as f:
                f.write(image_data)
            
            thread_safe_print(f"   ✅ Saved image to: {target_file_path}")
            
        except Exception as e:
            thread_safe_print(f"   ❌ Error saving image {target_file_path}: {e}")
    
    thread_safe_print(f"\n✅ Completed processing all new images")

def process_modified_images(modified_images, pr_url, github_client, repo_config):
    """Process modified image files by replacing them in target repository"""
    if not modified_images:
        thread_safe_print("\n🖼️  No modified images to process")
        return
    
    thread_safe_print(f"\n🖼️  Processing {len(modified_images)} modified images...")
    
    target_local_path = repo_config['target_local_path']
    
    for file_path in modified_images:
        thread_safe_print(f"\n🔄 Processing modified image: {file_path}")
        
        # Create target file path
        target_file_path = os.path.join(target_local_path, file_path)
        target_dir = os.path.dirname(target_file_path)
        
        # Create directory if it doesn't exist (in case of path changes)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)
            thread_safe_print(f"   📁 Created directory: {target_dir}")
        
        # Download updated image from source
        image_data = download_image_from_source(file_path, pr_url, github_client)
        
        if image_data is None:
            thread_safe_print(f"   ❌ Failed to download modified image: {file_path}")
            continue
        
        # Write updated image data to target file (overwrite)
        try:
            with open(target_file_path, 'wb') as f:
                f.write(image_data)
            
            thread_safe_print(f"   ✅ Updated image: {target_file_path}")
            
        except Exception as e:
            thread_safe_print(f"   ❌ Error updating image {target_file_path}: {e}")
    
    thread_safe_print(f"\n✅ Completed processing all modified images")

def process_deleted_images(deleted_images, repo_config):
    """Process deleted image files by removing them from target repository"""
    if not deleted_images:
        thread_safe_print("\n🖼️  No images to delete")
        return
    
    thread_safe_print(f"\n🖼️  Processing {len(deleted_images)} deleted images...")
    
    target_local_path = repo_config['target_local_path']
    
    for file_path in deleted_images:
        thread_safe_print(f"\n🗑️  Processing deleted image: {file_path}")
        
        # Create target file path
        target_file_path = os.path.join(target_local_path, file_path)
        
        # Check if file exists in target
        if os.path.exists(target_file_path):
            try:
                os.remove(target_file_path)
                thread_safe_print(f"   ✅ Deleted image: {target_file_path}")
            except Exception as e:
                thread_safe_print(f"   ❌ Error deleting image {target_file_path}: {e}")
        else:
            thread_safe_print(f"   ⚠️  Target image not found: {target_file_path}")
    
    thread_safe_print(f"\n✅ Completed processing deleted images")

def process_all_images(added_images, modified_images, deleted_images, pr_url, github_client, repo_config):
    """Process all image operations: add, modify, and delete"""
    thread_safe_print("\n" + "="*80)
    thread_safe_print("🖼️  IMAGE PROCESSING")
    thread_safe_print("="*80)
    
    # Process in order: delete first, then add/modify
    process_deleted_images(deleted_images, repo_config)
    process_added_images(added_images, pr_url, github_client, repo_config)
    process_modified_images(modified_images, pr_url, github_client, repo_config)
    
    thread_safe_print("\n" + "="*80)
    thread_safe_print("✅ IMAGE PROCESSING COMPLETED")
    thread_safe_print("="*80)

