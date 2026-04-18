"""
Main Entry Point for GitHub Workflow
Orchestrates the entire auto-sync workflow in GitHub Actions environment
"""

import sys
import os
import json
import re
import subprocess
import tiktoken
from github import Github, Auth

# Configuration from environment variables
SOURCE_PR_URL = os.getenv("SOURCE_PR_URL")
TARGET_PR_URL = os.getenv("TARGET_PR_URL")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
AI_PROVIDER = os.getenv("AI_PROVIDER", "deepseek")
TARGET_REPO_PATH = os.getenv("TARGET_REPO_PATH")
SKIP_GIT_ADD = os.getenv("SKIP_GIT_ADD", "false").lower() == "true"
TIDB_CLOUD_ABSOLUTE_LINK_PREFIX = os.getenv(
    "TIDB_CLOUD_ABSOLUTE_LINK_PREFIX",
    "https://docs.pingcap.com/tidbcloud/",
)
os.environ.setdefault(
    "TIDB_CLOUD_ABSOLUTE_LINK_PREFIX",
    TIDB_CLOUD_ABSOLUTE_LINK_PREFIX,
)

# Import all modules
from ai_client import UnifiedAIClient, thread_safe_print, print_lock, PROVIDER_MAX_TOKENS
from diff_analyzer import analyze_source_changes, get_repo_config, get_target_hierarchy_and_content, parse_pr_url
from image_processor import process_all_images
from file_adder import process_added_files
from file_deleter import process_deleted_files
from file_updater import process_files_in_batches, process_added_sections, process_modified_sections, process_deleted_sections
from toc_processor import process_toc_file
from keword_processor import process_keyword_file
from section_matcher import match_source_diff_to_target
from glossary import load_glossary, create_glossary_matcher
from log_sanitizer import sanitize_exception_message
from parallel_file_processor import (
    count_unique_file_paths,
    make_file_task,
    make_task_result,
    run_file_tasks,
    should_parallelize_file_processing,
)
from special_file_utils import is_toc_file_name
from workflow_ignore_config import load_workflow_ignore_config

# Glossary terms path (optional, defaults to resources/terms.md in the docs repo)
TERMS_PATH = os.getenv("TERMS_PATH", "")

# Processing limit configuration
MAX_NON_SYSTEM_SECTIONS_FOR_AI = 120
SOURCE_TOKEN_LIMIT = 50000  # Maximum tokens for source new_content before skipping file processing
AI_MAX_TOKENS = PROVIDER_MAX_TOKENS.get(AI_PROVIDER, 8192)

# Special file configuration
SPECIAL_FILES = ["TOC.md", "keywords.md"]
WORKFLOW_IGNORE_CONFIG = load_workflow_ignore_config()
PR_MODE_IGNORE_FILES = WORKFLOW_IGNORE_CONFIG["PR_MODE_IGNORE_FILES"]
PR_MODE_IGNORE_FOLDERS = WORKFLOW_IGNORE_CONFIG["PR_MODE_IGNORE_FOLDERS"]

# Repository configuration for workflow
def get_workflow_repo_configs():
    """Get repository configuration based on environment variables"""
    if not SOURCE_PR_URL or not TARGET_PR_URL:
        raise ValueError("SOURCE_PR_URL and TARGET_PR_URL must be set")
    
    # Parse source and target repo info
    source_parts = SOURCE_PR_URL.split('/')
    target_parts = TARGET_PR_URL.split('/')
    
    source_owner, source_repo = source_parts[-4], source_parts[-3]
    target_owner, target_repo = target_parts[-4], target_parts[-3]
    
    source_repo_key = f"{source_owner}/{source_repo}"
    target_repo_key = f"{target_owner}/{target_repo}"
    
    # Determine language direction based on repo names
    if source_repo.endswith('-cn') and not target_repo.endswith('-cn'):
        # Chinese to English
        source_language = "Chinese"
        target_language = "English"
    elif not source_repo.endswith('-cn') and target_repo.endswith('-cn'):
        # English to Chinese
        source_language = "English"
        target_language = "Chinese"
    else:
        # Default fallback
        source_language = "English"
        target_language = "Chinese"
    
    return {
        source_repo_key: {
            "target_repo": target_repo_key,
            "target_local_path": TARGET_REPO_PATH,
            "prefer_local_target_for_read": False,
            "source_language": source_language,
            "target_language": target_language
        }
    }

def ensure_temp_output_dir():
    """Ensure the temp_output directory exists"""
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    temp_dir = os.path.join(script_dir, "temp_output")
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir

def clean_temp_output_dir():
    """Clean the temp_output directory at the start of execution"""
    import shutil
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    temp_dir = os.path.join(script_dir, "temp_output")
    if os.path.exists(temp_dir):
        if os.path.isdir(temp_dir):
            shutil.rmtree(temp_dir)
            thread_safe_print(f"🧹 Cleaned existing temp_output directory")
        else:
            # Remove file if it exists
            os.remove(temp_dir)
            thread_safe_print(f"🧹 Removed existing temp_output file")
    os.makedirs(temp_dir, exist_ok=True)
    thread_safe_print(f"📁 Created temp_output directory: {temp_dir}")
    return temp_dir

def git_add_changes(target_repo_path):
    """Stage all current changes in the target repo so they survive a later failure."""
    if SKIP_GIT_ADD:
        thread_safe_print("   ⏭️  SKIP_GIT_ADD=true; skipping git add .")
        return

    result = subprocess.run(
        ["git", "add", "."],
        cwd=target_repo_path,
        capture_output=True, text=True
    )
    if result.returncode == 0:
        thread_safe_print(f"   ✅ git add . completed in {target_repo_path}")
    else:
        thread_safe_print(f"   ❌ git add . failed: {result.stderr}")

def git_add_successful_task_changes(task_results, target_repo_path):
    """Stage successful task writes after a parallel batch has finished."""
    has_success = any(
        result["ok"] and result["result"] and result["result"].get("status") == "success"
        for result in task_results
    )
    if not has_success:
        return

    # Keep staging out of worker threads so git add never races with active file writes.
    git_add_changes(target_repo_path)

def estimate_tokens(text):
    """Calculate accurate token count using tiktoken (GPT-4/3.5 encoding)"""
    if not text:
        return 0
    try:
        enc = tiktoken.get_encoding("cl100k_base")  # GPT-4/3.5 encoding
        tokens = enc.encode(text)
        return len(tokens)
    except Exception as e:
        # Fallback to character approximation if tiktoken fails
        thread_safe_print(
            f"   ⚠️  Tiktoken encoding failed: {sanitize_exception_message(e)}, using character approximation"
        )
        return len(text) // 4

def print_token_estimation(prompt_text, context="AI translation"):
    """Print accurate token consumption for a request"""
    actual_tokens = estimate_tokens(prompt_text)
    char_count = len(prompt_text)
    thread_safe_print(f"   💰 {context}")
    thread_safe_print(f"      📝 Input: {char_count:,} characters")
    thread_safe_print(f"      🔢 Actual tokens: {actual_tokens:,} (using tiktoken cl100k_base)")
    return actual_tokens

def check_source_token_limit(source_diff_dict_file, token_limit=SOURCE_TOKEN_LIMIT):
    """Check if the total tokens of all new_content in source-diff-dict exceeds the limit"""
    try:
        with open(source_diff_dict_file, 'r', encoding='utf-8') as f:
            source_diff_dict = json.load(f)
        
        total_new_content = ""
        section_count = 0
        
        for key, section_data in source_diff_dict.items():
            if isinstance(section_data, dict):
                new_content = section_data.get('new_content', '')
                if new_content:
                    total_new_content += new_content + "\n"
                    section_count += 1
        
        if not total_new_content.strip():
            thread_safe_print(f"   ⚠️  No new_content found in {source_diff_dict_file}")
            return True, 0, 0  # Allow processing if no content to check
        
        total_tokens = estimate_tokens(total_new_content)
        char_count = len(total_new_content)
        
        thread_safe_print(f"   📊 Source token limit check:")
        thread_safe_print(f"      📝 Total new_content: {char_count:,} characters from {section_count} sections")
        thread_safe_print(f"      🔢 Total tokens: {total_tokens:,}")
        thread_safe_print(f"      🚧 Token limit: {token_limit:,}")
        
        if total_tokens > token_limit:
            thread_safe_print(f"      ❌ Token limit exceeded! ({total_tokens:,} > {token_limit:,})")
            return False, total_tokens, token_limit
        else:
            thread_safe_print(f"      ✅ Within token limit ({total_tokens:,} ≤ {token_limit:,})")
            return True, total_tokens, token_limit
            
    except Exception as e:
        thread_safe_print(
            f"   ❌ Error checking token limit for {source_diff_dict_file}: {sanitize_exception_message(e)}"
        )
        return True, 0, 0  # Allow processing on error to avoid blocking


def extract_source_diff_section_title(section_data):
    """Return a compact source section title for failure reports."""
    for field in ("new_content", "old_content"):
        content = section_data.get(field) or ""
        for line in str(content).splitlines():
            stripped = line.strip()
            if re.match(r'^#{1,10}\s+\S', stripped):
                return re.sub(r'^#{1,10}\s+', '', stripped).strip()

    hierarchy = section_data.get("original_hierarchy", "")
    if hierarchy:
        leaf = hierarchy.split(" > ")[-1]
        return re.sub(r'^#{1,10}\s+', '', leaf.strip()).strip()

    return "(unknown section)"


def get_unmatched_modified_source_sections(source_diff_dict, matched_sections):
    """Find modified source sections that could not be mapped to target sections."""
    matched_keys = set(matched_sections or {})
    missing = []

    for key, section_data in source_diff_dict.items():
        if section_data.get("operation") != "modified":
            continue
        if key in matched_keys:
            continue

        missing.append(
            {
                "key": key,
                "line": section_data.get("new_line_number", "?"),
                "title": extract_source_diff_section_title(section_data),
            }
        )

    return missing


def format_unmatched_modified_sections_failure(file_path, missing_sections):
    """Build a one-line reason suitable for translation failure reports."""
    details = "; ".join(
        f"{item['key']} (source line {item['line']}): {item['title']}"
        for item in missing_sections
    )
    return (
        f"Target file {file_path} is missing or could not map "
        f"{len(missing_sections)} modified source section(s), so translation was skipped. "
        f"Missing sections: {details}. "
        "Please add or sync these sections in the target branch first, then rerun translation for this file."
    )


def get_pr_diff(pr_url, github_client):
    """Get the diff content from a GitHub PR (from auto-sync-pr-changes.py)"""
    try:
        from diff_analyzer import parse_pr_url
        owner, repo, pr_number = parse_pr_url(pr_url)
        repository = github_client.get_repo(f"{owner}/{repo}")
        pr = repository.get_pull(pr_number)
        
        # Get files and their patches
        files = pr.get_files()
        diff_content = []
        
        for file in files:
            if file.filename.endswith('.md') and file.patch:
                diff_content.append(f"File: {file.filename}")
                diff_content.append(file.patch)
                diff_content.append("-" * 80)
        
        return "\n".join(diff_content)
        
    except Exception as e:
        thread_safe_print(f"   ❌ Error getting PR diff: {sanitize_exception_message(e)}")
        return None

def filter_diff_by_operation_type(pr_diff, operation_type, target_sections=None):
    """Filter PR diff to only include changes relevant to specific operation type"""
    
    if not pr_diff:
        return ""
    
    if operation_type == "modified":
        # For modified sections, we want the full diff but focus on changed content
        return pr_diff
    elif operation_type == "added":
        # For added sections, we want to show what was added
        filtered_lines = []
        for line in pr_diff.split('\n'):
            if line.startswith('+') and not line.startswith('+++'):
                filtered_lines.append(line)
            elif line.startswith('@@') or line.startswith('File:'):
                filtered_lines.append(line)
        return '\n'.join(filtered_lines)
    elif operation_type == "deleted":
        # For deleted sections, we want to show what was removed
        filtered_lines = []
        for line in pr_diff.split('\n'):
            if line.startswith('-') and not line.startswith('---'):
                filtered_lines.append(line)
            elif line.startswith('@@') or line.startswith('File:'):
                filtered_lines.append(line)
        return '\n'.join(filtered_lines)
    
    return pr_diff

def filter_diff_for_target_file(pr_diff, target_file, source_diff_dict):
    """Extract file-specific diff from the complete PR diff based on source files that map to the target file"""
    if not pr_diff or not source_diff_dict:
        return pr_diff
    
    # Extract source files that contribute to this target file
    source_files = set()
    for key, section_data in source_diff_dict.items():
        if isinstance(section_data, dict):
            source_file = section_data.get('source_file', '')
            if source_file:
                source_files.add(source_file)
    
    if not source_files:
        thread_safe_print(f"   ⚠️  No source files found in source_diff_dict, using complete PR diff")
        return pr_diff
    
    thread_safe_print(f"   📄 Source files contributing to {target_file}: {list(source_files)}")
    
    # Filter PR diff to only include changes from these source files
    filtered_lines = []
    current_file = None
    include_section = False
    
    for line in pr_diff.split('\n'):
        if line.startswith('File: '):
            current_file = line.replace('File: ', '').strip()
            include_section = current_file in source_files
            if include_section:
                filtered_lines.append(line)
        elif line.startswith('-' * 80):
            if include_section:
                filtered_lines.append(line)
        elif include_section:
            filtered_lines.append(line)
    
    file_specific_diff = '\n'.join(filtered_lines)
    thread_safe_print(f"   📊 Filtered diff: {len(file_specific_diff)} chars (from {len(pr_diff)} chars)")
    
    return file_specific_diff if file_specific_diff.strip() else pr_diff

def extract_file_diff_from_pr(pr_diff, source_file_path):
    """Extract diff content for a specific source file from the complete PR diff"""
    if not pr_diff:
        return ""
    
    filtered_lines = []
    current_file = None
    include_section = False
    
    for line in pr_diff.split('\n'):
        if line.startswith('File: '):
            current_file = line.replace('File: ', '').strip()
            include_section = (current_file == source_file_path)
            if include_section:
                filtered_lines.append(line)
        elif line.startswith('-' * 80):
            if include_section:
                filtered_lines.append(line)
                include_section = False  # End of this file's section
        elif include_section:
            filtered_lines.append(line)
    
    return '\n'.join(filtered_lines)

def determine_file_processing_type(source_file_path, file_sections, special_files=None, ignore_files=None):
    """Determine how to process a file based on operation type and file characteristics"""
    basename = os.path.basename(source_file_path)
    ignore_files = PR_MODE_IGNORE_FILES if ignore_files is None else ignore_files
    
    # Check if this is a special file (like TOC.md or keywords.md)
    if is_toc_file_name(source_file_path, ignore_files):
        return "special_file_toc"

    if special_files and basename in special_files:
        if basename == "keywords.md":
            if isinstance(file_sections, dict) and file_sections.get("keyword_regular_only"):
                return "regular_modified"
            return "special_file_keyword"
    
    # For all other modified files, use regular processing
    return "regular_modified"

def process_regular_modified_file(source_file_path, file_sections, file_diff, source_context_or_pr_url, github_client, ai_client, repo_config, max_sections, glossary_matcher=None, return_details=False):
    """Process a regular markdown file that has been modified"""
    def finish(success, reason=""):
        if return_details:
            return success, reason
        return success

    try:
        thread_safe_print(f"   📝 Processing as regular modified file: {source_file_path}")
        
        # Extract the actual sections from the file_sections structure
        # file_sections contains: {'sections': {...}, 'original_hierarchy': {...}, 'current_hierarchy': {...}}
        if isinstance(file_sections, dict) and 'sections' in file_sections:
            actual_sections = file_sections['sections']
        else:
            # Fallback: assume file_sections is already the sections dict
            actual_sections = file_sections
        
        thread_safe_print(f"   📊 Extracted sections: {len(actual_sections)} sections")
        
        # CRITICAL: Load the source-diff-dict.json and perform matching
        import json
        import os
        from section_matcher import match_source_diff_to_target
        from diff_analyzer import get_target_hierarchy_and_content
        
        # Load source-diff-dict.json with file prefix
        temp_dir = ensure_temp_output_dir()
        file_prefix = source_file_path.replace('/', '-').replace('.md', '')
        source_diff_dict_file = os.path.join(temp_dir, f"{file_prefix}-source-diff-dict.json")
        if os.path.exists(source_diff_dict_file):
            with open(source_diff_dict_file, 'r', encoding='utf-8') as f:
                source_diff_dict = json.load(f)
            thread_safe_print(f"   📂 Loaded source diff dict with {len(source_diff_dict)} sections from {source_diff_dict_file}")
            
            # Check source token limit before proceeding with processing
            thread_safe_print(f"   🔍 Checking source token limit...")
            within_limit, total_tokens, token_limit = check_source_token_limit(source_diff_dict_file)
            if not within_limit:
                thread_safe_print(f"   🚫 Skipping file processing: source content exceeds token limit")
                thread_safe_print(f"      📊 Total tokens: {total_tokens:,} > Limit: {token_limit:,}")
                thread_safe_print(f"      ⏭️  File {source_file_path} will not be processed")
                return finish(
                    False,
                    f"Source content exceeds token limit ({total_tokens:,} > {token_limit:,})",
                )
                
        else:
            thread_safe_print(f"   ❌ {source_diff_dict_file} not found")
            return finish(False, f"{source_diff_dict_file} not found")
        
        # Get target file hierarchy and content
        target_repo = repo_config['target_repo']
        target_hierarchy, target_lines = get_target_hierarchy_and_content(
            source_file_path,
            github_client,
            target_repo,
            repo_config.get('target_local_path'),
            repo_config.get('prefer_local_target_for_read', False),
            repo_config.get('target_ref'),
        )
        
        if not target_hierarchy or not target_lines:
            thread_safe_print(f"   ❌ Could not get target file content for {source_file_path}")
            return finish(False, f"Could not get target file content for {source_file_path}")
        
        thread_safe_print(f"   📖 Target file: {len(target_hierarchy)} sections, {len(target_lines)} lines")
        
        # Perform source diff to target matching
        thread_safe_print(f"   🔗 Matching source diff to target...")
        enhanced_sections = match_source_diff_to_target(
            source_diff_dict, 
            target_hierarchy, 
            target_lines, 
            ai_client, 
            repo_config, 
            max_sections,
            AI_MAX_TOKENS
        )
        
        if not enhanced_sections:
            thread_safe_print(f"   ❌ No sections matched")
            return finish(False, "No sections matched")
        
        thread_safe_print(f"   ✅ Matched {len(enhanced_sections)} sections")
        
        # Save the match result for reference
        match_file = os.path.join(temp_dir, f"{source_file_path.replace('/', '-').replace('.md', '')}-match_source_diff_to_target.json")
        with open(match_file, 'w', encoding='utf-8') as f:
            json.dump(enhanced_sections, f, ensure_ascii=False, indent=2)
        thread_safe_print(f"   💾 Saved match result to: {match_file}")

        missing_modified_sections = get_unmatched_modified_source_sections(
            source_diff_dict,
            enhanced_sections,
        )
        if missing_modified_sections:
            failure_reason = format_unmatched_modified_sections_failure(
                source_file_path,
                missing_modified_sections,
            )
            thread_safe_print(
                f"   ❌ Skipping {source_file_path}: "
                f"{len(missing_modified_sections)} modified source section(s) were not found in target"
            )
            for item in missing_modified_sections:
                thread_safe_print(
                    f"      - {item['key']} (source line {item['line']}): {item['title']}"
                )
            return finish(False, failure_reason)
        
        # Step 2: Get AI translation for the matched sections
        thread_safe_print(f"   🤖 Getting AI translation for matched sections...")
        
        # Create file data structure with enhanced matching info
        # Wrap enhanced_sections in the expected format for process_single_file
        file_data = {
            source_file_path: {
                'type': 'enhanced_sections',
                'sections': enhanced_sections
            }
        }
        
        # Call the existing process_modified_sections function to get AI translation
        results = process_modified_sections(file_data, file_diff, source_context_or_pr_url, github_client, ai_client, repo_config, max_sections, glossary_matcher=glossary_matcher)
        
        # Step 3: Update match_source_diff_to_target.json with AI results
        if results and len(results) > 0:
            file_path, success, ai_updated_sections = results[0]  # Get first result
            if success and isinstance(ai_updated_sections, dict):
                thread_safe_print(f"   📝 Step 3: Updating {match_file} with AI results...")
                
                # Load current match_source_diff_to_target.json
                with open(match_file, 'r', encoding='utf-8') as f:
                    match_data = json.load(f)
                
                # Add target_new_content field to each section based on AI results
                updated_count = 0
                for key, section_data in match_data.items():
                    operation = section_data.get('source_operation', '')
                    
                    if operation == 'deleted':
                        # For deleted sections, set target_new_content to null
                        section_data['target_new_content'] = None
                    elif key in ai_updated_sections:
                        # For modified/added sections with AI translation
                        section_data['target_new_content'] = ai_updated_sections[key]
                        updated_count += 1
                    else:
                        # For sections not translated, keep original content
                        section_data['target_new_content'] = section_data.get('target_content', '')
                
                # Save updated match_source_diff_to_target.json
                with open(match_file, 'w', encoding='utf-8') as f:
                    json.dump(match_data, f, ensure_ascii=False, indent=2)
                
                thread_safe_print(f"   ✅ Updated {updated_count} sections with AI translations in {match_file}")
                
                # Abort before writing if any chunk translations failed
                chunk_failures = getattr(ai_updated_sections, "failures", [])
                if chunk_failures:
                    thread_safe_print(f"   ⚠️  Skipping file update due to chunk translation failures")
                    return finish(False, "; ".join(chunk_failures))

                # Step 4: Apply updates to target document using update_target_document_from_match_data
                thread_safe_print(f"   📝 Step 4: Applying updates to target document...")
                from file_updater import update_target_document_from_match_data
                
                success = update_target_document_from_match_data(match_file, repo_config['target_local_path'], source_file_path)
                if success:
                    thread_safe_print(f"   🎉 Target document successfully updated!")
                    return finish(True)
                else:
                    thread_safe_print(f"   ❌ Failed to update target document")
                    return finish(False, f"Failed to update target document for {source_file_path}")
                    
            else:
                thread_safe_print(f"   ⚠️  AI translation failed or returned invalid results")
                failure_reason = "AI translation failed or returned invalid results"
                if hasattr(ai_updated_sections, "failures") and ai_updated_sections.failures:
                    failure_reason = "; ".join(ai_updated_sections.failures)
                return finish(False, failure_reason)
        else:
            thread_safe_print(f"   ⚠️  No results from process_modified_sections")
            return finish(False, "No results from process_modified_sections")
        
    except Exception as e:
        thread_safe_print(
            f"   ❌ Error processing regular modified file {source_file_path}: {sanitize_exception_message(e)}"
        )
        return finish(False, f"Error processing regular modified file {source_file_path}: {sanitize_exception_message(e)}")


def _process_single_modified_file_for_pr(
    source_file_path,
    file_sections,
    pr_diff,
    source_context_or_pr_url,
    github_client,
    ai_client,
    repo_config,
    glossary_matcher,
):
    thread_safe_print(f"\n📄 Processing modified file: {source_file_path}")

    thread_safe_print(f"   🔍 Extracting file-specific diff for: {source_file_path}")
    file_specific_diff = extract_file_diff_from_pr(pr_diff, source_file_path) if pr_diff else ""

    if not file_specific_diff:
        if pr_diff:
            return make_task_result("skipped", "No file-specific diff found")
        return make_task_result("skipped", "No markdown patch text available for section-level translation")

    thread_safe_print(f"   📊 File-specific diff: {len(file_specific_diff)} chars")

    file_type = determine_file_processing_type(source_file_path, file_sections, SPECIAL_FILES, PR_MODE_IGNORE_FILES)
    thread_safe_print(f"   🔍 File processing type: {file_type}")

    if file_type == "special_file_toc":
        return make_task_result("skipped", "Special file already processed in Step 3.3")
    if file_type == "special_file_keyword":
        return make_task_result("skipped", "Keyword file already processed in Step 3.3b")
    if file_type != "regular_modified":
        return make_task_result("failure", f"Unknown file processing type: {file_type}")

    success, failure_reason = process_regular_modified_file(
        source_file_path,
        file_sections,
        file_specific_diff,
        source_context_or_pr_url,
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


def is_under_exclude_folder(file_path, folder_name):
    """Check if a file path is under the given folder"""
    return file_path.startswith(folder_name + "/") or file_path == folder_name

def filter_docs_by_folder(folder_name, added_sections, modified_sections, deleted_sections,
                          added_files, deleted_files, toc_files, keyword_files,
                          added_images, modified_images, deleted_images,
                          label=None):
    """Remove all entries under the given folder from every result category.
    Returns the filtered versions of all inputs."""
    if label is None:
        label = folder_name

    def filter_dict(d):
        return {k: v for k, v in d.items() if not is_under_exclude_folder(k, folder_name)}

    def filter_list(lst):
        return [item for item in lst if not is_under_exclude_folder(item, folder_name)]

    skipped = []
    for d in (added_sections, modified_sections, deleted_sections, added_files, toc_files, keyword_files):
        for k in list(d.keys()):
            if is_under_exclude_folder(k, folder_name):
                skipped.append(k)
    for lst in (deleted_files, added_images, modified_images, deleted_images):
        for item in lst:
            if is_under_exclude_folder(item, folder_name):
                skipped.append(item)

    if skipped:
        thread_safe_print(f"\n🚫 Skipping {len(skipped)} {label} doc entries under '{folder_name}/':")
        for s in skipped:
            thread_safe_print(f"   ⏭️  {s}")

    return (
        filter_dict(added_sections),
        filter_dict(modified_sections),
        filter_dict(deleted_sections),
        filter_dict(added_files),
        filter_list(deleted_files),
        filter_dict(toc_files),
        filter_dict(keyword_files),
        filter_list(added_images),
        filter_list(modified_images),
        filter_list(deleted_images),
    )

def get_workflow_repo_config(pr_url, repo_configs):
    """Get repository configuration for workflow environment"""
    from diff_analyzer import parse_pr_url
    
    owner, repo, pr_number = parse_pr_url(pr_url)
    source_repo = f"{owner}/{repo}"
    
    if source_repo not in repo_configs:
        raise ValueError(f"Unsupported source repository: {source_repo}. Supported: {list(repo_configs.keys())}")
    
    config = repo_configs[source_repo].copy()
    config['source_repo'] = source_repo
    config['pr_number'] = pr_number
    
    return config

def main():
    """Main function - orchestrates the entire workflow for GitHub Actions"""
    
    # Validate environment variables
    if not all([SOURCE_PR_URL, TARGET_PR_URL, GITHUB_TOKEN, TARGET_REPO_PATH]):
        thread_safe_print("❌ Missing required environment variables:")
        thread_safe_print(f"   SOURCE_PR_URL: {SOURCE_PR_URL}")
        thread_safe_print(f"   TARGET_PR_URL: {TARGET_PR_URL}")
        thread_safe_print(f"   GITHUB_TOKEN: {'Set' if GITHUB_TOKEN else 'Not set'}")
        thread_safe_print(f"   TARGET_REPO_PATH: {TARGET_REPO_PATH}")
        return
    
    thread_safe_print(f"🔧 Auto PR Sync Tool (GitHub Workflow Version)")
    thread_safe_print(f"📍 Source PR URL: {SOURCE_PR_URL}")
    thread_safe_print(f"📍 Target PR URL: {TARGET_PR_URL}")
    thread_safe_print(f"🤖 AI Provider: {AI_PROVIDER}")
    thread_safe_print(f"📁 Target Repo Path: {TARGET_REPO_PATH}")
    
    # Clean and prepare temp_output directory
    clean_temp_output_dir()
    
    # Get repository configuration using workflow config
    try:
        repo_configs = get_workflow_repo_configs()
        repo_config = get_workflow_repo_config(SOURCE_PR_URL, repo_configs)
        thread_safe_print(f"📁 Source Repo: {repo_config['source_repo']} ({repo_config['source_language']})")
        thread_safe_print(f"📁 Target Repo: {repo_config['target_repo']} ({repo_config['target_language']})")
        thread_safe_print(f"📁 Target Path: {repo_config['target_local_path']}")
    except ValueError as e:
        thread_safe_print(f"❌ {sanitize_exception_message(e)}")
        return
    
    # Initialize clients
    auth = Auth.Token(GITHUB_TOKEN)
    github_client = Github(auth=auth)
    
    # Initialize unified AI client
    try:
        ai_client = UnifiedAIClient(provider=AI_PROVIDER)
        thread_safe_print(f"🤖 AI Provider: {AI_PROVIDER.upper()} ({ai_client.model})")
    except Exception as e:
        thread_safe_print(f"❌ Failed to initialize AI client: {sanitize_exception_message(e)}")
        return
    
    # Load glossary and create matcher for term-aware translation
    terms_path = TERMS_PATH
    if not terms_path and TARGET_REPO_PATH:
        candidate = os.path.join(TARGET_REPO_PATH, "resources", "terms.md")
        if os.path.exists(candidate):
            terms_path = candidate
    thread_safe_print(f"\n📚 Loading glossary from: {terms_path or '(not configured)'}")
    glossary = load_glossary(terms_path) if terms_path else []
    glossary_matcher = create_glossary_matcher(glossary)
    
    thread_safe_print(f"\n🚀 Starting auto-sync for PR: {SOURCE_PR_URL}")
    
    # Step 1: Get PR diff
    thread_safe_print(f"\n📋 Step 1: Getting PR diff...")
    pr_diff = get_pr_diff(SOURCE_PR_URL, github_client)
    if pr_diff is None:
        thread_safe_print("❌ Could not get PR diff")
        return
    if pr_diff:
        thread_safe_print(f"✅ Got PR diff: {len(pr_diff)} characters")
    else:
        thread_safe_print("⚠️  No markdown patch text found in this PR, continuing with file/image processing only")
    
    # Build list of folders to exclude early (before expensive per-file processing)
    exclude_folders = []
    if repo_config.get('target_language') == 'Chinese':
        exclude_folders = PR_MODE_IGNORE_FOLDERS
    
    # Step 2: Analyze source changes with operation categorization
    thread_safe_print(f"\n📊 Step 2: Analyzing source changes...")
    added_sections, modified_sections, deleted_sections, added_files, deleted_files, toc_files, keyword_files, added_images, modified_images, deleted_images = analyze_source_changes(
        SOURCE_PR_URL, github_client, 
        special_files=SPECIAL_FILES, 
        ignore_files=PR_MODE_IGNORE_FILES,
        repo_configs=repo_configs,
        max_non_system_sections=MAX_NON_SYSTEM_SECTIONS_FOR_AI,
        pr_diff=pr_diff,
        exclude_folders=exclude_folders,
    )
    diff_file_count = count_unique_file_paths(
        added_sections,
        modified_sections,
        deleted_sections,
        added_files,
        deleted_files,
        toc_files,
        keyword_files,
        added_images,
        modified_images,
        deleted_images,
    )
    parallel_file_processing = should_parallelize_file_processing(diff_file_count)
    if parallel_file_processing:
        thread_safe_print(f"⚡ Diff has {diff_file_count} files; file-level translation will use parallel chunks.")
    
    # Step 3: Process different types of files based on operation type
    thread_safe_print(f"\n📋 Step 3: Processing files based on operation type...")
    
    # Step 3.1: Process deleted files (file-level deletions)
    if deleted_files:
        thread_safe_print(f"\n🗑️  Step 3.1: Processing {len(deleted_files)} deleted files...")
        process_deleted_files(deleted_files, github_client, repo_config)
        thread_safe_print(f"   ✅ Deleted files processed")
        git_add_changes(TARGET_REPO_PATH)
    
    # Step 3.2: Process added files (file-level additions) one by one
    if added_files:
        thread_safe_print(f"\n📄 Step 3.2: Processing {len(added_files)} added files...")

        added_tasks = []
        for file_path, file_content in added_files.items():
            def run_added_file(path=file_path, content=file_content):
                success = process_added_files(
                    {path: content},
                    SOURCE_PR_URL,
                    github_client,
                    ai_client,
                    repo_config,
                    glossary_matcher=glossary_matcher,
                )
                if success:
                    return make_task_result("success")
                return make_task_result("failure", "Added file processor returned failure")

            added_tasks.append(make_file_task(file_path, run_added_file))

        added_results = run_file_tasks(added_tasks, "added files", parallel_file_processing)
        for result in added_results:
            file_path = result["file_path"]
            task_result = result["result"] or {}
            if result["ok"] and task_result.get("status") == "success":
                thread_safe_print(f"   ✅ Successfully processed added file {file_path}")
            else:
                reason = result["error"] or task_result.get("reason") or "Added file processor returned failure"
                thread_safe_print(f"   ❌ Failed to process added file {file_path}: {reason}")
        git_add_successful_task_changes(added_results, TARGET_REPO_PATH)
        thread_safe_print(f"   ✅ Added files processed")
    
    # Step 3.3: Process special files (TOC.md and similar)
    if toc_files:
        thread_safe_print(f"\n📋 Step 3.3: Processing {len(toc_files)} special files (TOC)...")

        toc_tasks = []
        for file_path, toc_data in toc_files.items():
            def run_toc_file(path=file_path, data=toc_data):
                if data.get("type") != "toc":
                    return make_task_result("failure", f"Unknown TOC data type: {data.get('type')}")
                success = process_toc_file(path, data, SOURCE_PR_URL, github_client, ai_client, repo_config)
                if success:
                    return make_task_result("success")
                return make_task_result("failure", "TOC processor returned failure")

            toc_tasks.append(make_file_task(file_path, run_toc_file))

        toc_results = run_file_tasks(toc_tasks, "TOC files", parallel_file_processing)
        for result in toc_results:
            file_path = result["file_path"]
            task_result = result["result"] or {}
            if result["ok"] and task_result.get("status") == "success":
                thread_safe_print(f"   ✅ Successfully processed TOC file {file_path}")
            else:
                reason = result["error"] or task_result.get("reason") or "TOC processor returned failure"
                thread_safe_print(f"   ❌ Failed to process TOC file {file_path}: {reason}")
        thread_safe_print(f"   ✅ Special files processed")
        git_add_successful_task_changes(toc_results, TARGET_REPO_PATH)
    
    # Step 3.3b: Process keyword files (keywords.md)
    if keyword_files:
        thread_safe_print(f"\n📋 Step 3.3b: Processing {len(keyword_files)} keyword files...")
        keyword_tasks = []
        for file_path, keyword_data in keyword_files.items():
            def run_keyword_file(path=file_path, data=keyword_data):
                if data.get("type") != "keyword":
                    return make_task_result("failure", f"Unknown keyword data type: {data.get('type')}")
                success = process_keyword_file(path, data, SOURCE_PR_URL, github_client, ai_client, repo_config)
                if success:
                    return make_task_result("success")
                return make_task_result("failure", "Keyword processor returned failure")

            keyword_tasks.append(make_file_task(file_path, run_keyword_file))

        keyword_results = run_file_tasks(keyword_tasks, "keyword files", parallel_file_processing)
        keyword_success = all(
            result["ok"] and result["result"] and result["result"].get("status") == "success"
            for result in keyword_results
        )
        if not keyword_success:
            for result in keyword_results:
                if result["ok"] and result["result"] and result["result"].get("status") == "success":
                    continue
                reason = result["error"] or (result["result"] or {}).get("reason") or "Keyword processor returned failure"
                thread_safe_print(f"   ❌ Failed to process keyword file {result['file_path']}: {reason}")
            thread_safe_print("   ❌ Keyword files processing failed, exiting workflow")
            return
        thread_safe_print(f"   ✅ Keyword files processed")
        git_add_successful_task_changes(keyword_results, TARGET_REPO_PATH)
    
    # Step 3.4: Process modified files (section-level modifications)
    if modified_sections:
        thread_safe_print(f"\n📝 Step 3.4: Processing {len(modified_sections)} modified files...")

        modified_tasks = []
        for source_file_path, file_sections in modified_sections.items():
            def run_modified_file(path=source_file_path, sections=file_sections):
                return _process_single_modified_file_for_pr(
                    path,
                    sections,
                    pr_diff,
                    SOURCE_PR_URL,
                    github_client,
                    ai_client,
                    repo_config,
                    glossary_matcher,
                )

            modified_tasks.append(
                make_file_task(
                    source_file_path,
                    run_modified_file,
                    resource_key=source_file_path.replace('/', '-').replace('.md', ''),
                )
            )

        modified_results = run_file_tasks(modified_tasks, "modified files", parallel_file_processing)
        for result in modified_results:
            source_file_path = result["file_path"]
            if not result["ok"]:
                thread_safe_print(f"   ❌ Failed to process {source_file_path}: {result['error']}")
                continue

            task_result = result["result"] or {}
            status = task_result.get("status", "failure")
            reason = task_result.get("reason", "")
            if status == "success":
                thread_safe_print(f"   ✅ Successfully processed {source_file_path}")
            elif status == "skipped":
                thread_safe_print(f"   ⏭️  Skipped {source_file_path}: {reason}")
            else:
                thread_safe_print(f"   ❌ Failed to process {source_file_path}: {reason}")

        git_add_successful_task_changes(modified_results, TARGET_REPO_PATH)
    
    # Step 3.5: Process images (added, modified, deleted)
    if added_images or modified_images or deleted_images:
        thread_safe_print(f"\n🖼️  Step 3.5: Processing images...")
        process_all_images(added_images, modified_images, deleted_images, SOURCE_PR_URL, github_client, repo_config)
        thread_safe_print(f"   ✅ Images processed")
        git_add_changes(TARGET_REPO_PATH)
    
    # Final summary
    thread_safe_print(f"\n" + "="*80)
    thread_safe_print(f"📊 Final Summary:")
    thread_safe_print(f"="*80)
    thread_safe_print(f"   📄 Added files: {len(added_files)} processed")
    thread_safe_print(f"   🗑️  Deleted files: {len(deleted_files)} processed")
    thread_safe_print(f"   📋 TOC files: {len(toc_files)} processed")
    thread_safe_print(f"   📋 Keyword files: {len(keyword_files)} processed")
    thread_safe_print(f"   📝 Modified files: {len(modified_sections)} processed")
    thread_safe_print(f"   🖼️  Added images: {len(added_images)} processed")
    thread_safe_print(f"   🖼️  Modified images: {len(modified_images)} processed")
    thread_safe_print(f"   🖼️  Deleted images: {len(deleted_images)} processed")
    thread_safe_print(f"="*80)
    thread_safe_print(f"🎉 Workflow completed successfully!")

if __name__ == "__main__":
    main()
