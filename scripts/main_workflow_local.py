"""
Main Entry Point
Orchestrates the entire auto-sync workflow
"""

SOURCE_PR_URL = "https://github.com/pingcap/docs-cn/pull/21036"
AI_PROVIDER = "gemini"  # Options: "deepseek", "gemini"
zh_doc_local_path = "/Users/grcai/Documents/GitHub/docs-cn"
en_doc_local_path = "/Users/grcai/Documents/GitHub/docs"

import sys
import os
import json
import threading
import tiktoken
from github import Github
#from google import genai

# Conditional import for Gemini
try:
    from google import genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

# Import all modules
from pr_analyzer import analyze_source_changes, get_repo_config, get_target_hierarchy_and_content
from file_adder import process_added_files
from file_deleter import process_deleted_files
from file_updater import process_files_in_batches, process_added_sections, process_modified_sections, process_deleted_sections
from toc_processor import process_toc_files
from section_matcher import match_source_diff_to_target
from image_processor import process_all_images

# extract the repo owner from the SOURCE_PR_URL
REPO_OWNER = SOURCE_PR_URL.split("/")[3]

# AI configuration
# To switch AI providers, change AI_PROVIDER to "deepseek" or "gemini"
# For Gemini: Set GEMINI_API_TOKEN environment variable
# For DeepSeek: Set DEEPSEEK_API_TOKEN environment variable
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_TOKEN")
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
GEMINI_API_KEY = os.getenv("GEMINI_API_TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GEMINI_MODEL_NAME = "gemini-2.0-flash"

# Processing limit configuration
MAX_NON_SYSTEM_SECTIONS_FOR_AI = 120
SOURCE_TOKEN_LIMIT = 5000  # Maximum tokens for source new_content before skipping file processing

# AI configuration
DEFAULT_MAX_TOKENS = 8000  # Default max tokens for AI translation requests
PROVIDER_MAX_TOKENS = {
    "deepseek": 20000,  # DeepSeek specific max tokens
    "gemini": DEFAULT_MAX_TOKENS  # Gemini uses default max tokens
}
AI_MAX_TOKENS = PROVIDER_MAX_TOKENS.get(AI_PROVIDER, DEFAULT_MAX_TOKENS)  # Set based on provider

# Special file configuration
SPECIAL_FILES = ["TOC.md"]
IGNORE_FILES = ["TOC-tidb-cloud.md","TOC-tidb-cloud-starter.md","TOC-tidb-cloud-essential.md","TOC-tidb-cloud-premium.md"]

# Repository configuration
REPO_CONFIGS = {
    f"{REPO_OWNER}/docs": {
        "target_repo": f"{REPO_OWNER}/docs-cn",
        "target_local_path": zh_doc_local_path,
        "source_language": "English",
        "target_language": "Chinese"
    },
    f"{REPO_OWNER}/docs-cn": {
        "target_repo": f"{REPO_OWNER}/docs",
        "target_local_path": en_doc_local_path,
        "source_language": "Chinese",
        "target_language": "English"
    }
}

# Thread-safe printing function
print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

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
            print(f"🧹 Cleaned existing temp_output directory")
        else:
            # Remove file if it exists
            os.remove(temp_dir)
            print(f"🧹 Removed existing temp_output file")
    os.makedirs(temp_dir, exist_ok=True)
    print(f"📁 Created temp_output directory: {temp_dir}")
    return temp_dir

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
        thread_safe_print(f"   ⚠️  Tiktoken encoding failed: {e}, using character approximation")
        return len(text) // 4

def print_token_estimation(prompt_text, context="AI translation"):
    """Print accurate token consumption for a request"""
    actual_tokens = estimate_tokens(prompt_text)
    char_count = len(prompt_text)
    thread_safe_print(f"   💰 {context}")
    thread_safe_print(f"      📝 Input: {char_count:,} characters")
    thread_safe_print(f"      🔢 Actual tokens: {actual_tokens:,} (using tiktoken cl100k_base)")
    return actual_tokens

class UnifiedAIClient:
    """Unified interface for different AI providers"""
    
    def __init__(self, provider="deepseek"):
        self.provider = provider
        if provider == "deepseek":
            from openai import OpenAI
            self.client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
            self.model = "deepseek-chat"
        elif provider == "gemini":
            if not GEMINI_AVAILABLE:
                raise ImportError("google.generativeai package not installed. Run: pip install google-generativeai")
            if not GEMINI_API_KEY:
                raise ValueError("GEMINI_API_TOKEN environment variable must be set")
            self.client = genai.Client(api_key=GEMINI_API_KEY)
            self.model = GEMINI_MODEL_NAME
        else:
            raise ValueError(f"Unsupported AI provider: {provider}")
    
    def chat_completion(self, messages, temperature=0.1, max_tokens=None):
        """Unified chat completion interface"""
        if self.provider == "deepseek":
            # DeepSeek has a max limit of 8192 tokens
            actual_max_tokens = min(max_tokens or 8192, 8192)
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temperature,
                max_tokens=actual_max_tokens
            )
            return response.choices[0].message.content.strip()
        elif self.provider == "gemini":
            try:
                # Convert OpenAI-style messages to Gemini format
                prompt = self._convert_messages_to_prompt(messages)
                thread_safe_print(f"   🔄 Calling Gemini API...")
                
                # Use the correct Gemini API call format (based on your reference file)
                response = self.client.models.generate_content(
                    model=self.model, 
                    contents=prompt
                )
                
                if response and response.text:
                    thread_safe_print(f"   ✅ Gemini response received")
                    return response.text.strip()
                else:
                    thread_safe_print(f"   ⚠️  Gemini response was empty or blocked")
                    return "No response from Gemini"
                    
            except Exception as e:
                thread_safe_print(f"   ❌ Gemini API error: {str(e)}")
                # Fallback: suggest switching to DeepSeek
                thread_safe_print(f"   💡 Consider switching to DeepSeek in main.py: AI_PROVIDER = 'deepseek'")
                raise e
    
    def _convert_messages_to_prompt(self, messages):
        """Convert OpenAI-style messages to a single prompt for Gemini"""
        prompt_parts = []
        for message in messages:
            role = message.get("role", "user")
            content = message.get("content", "")
            if role == "user":
                prompt_parts.append(content)
            elif role == "system":
                prompt_parts.append(f"System: {content}")
        return "\n\n".join(prompt_parts)

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
        thread_safe_print(f"   ❌ Error checking token limit for {source_diff_dict_file}: {e}")
        return True, 0, 0  # Allow processing on error to avoid blocking

def get_pr_diff(pr_url, github_client):
    """Get the diff content from a GitHub PR (from auto-sync-pr-changes.py)"""
    try:
        from pr_analyzer import parse_pr_url
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
        thread_safe_print(f"   ❌ Error getting PR diff: {e}")
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
        print(f"   ⚠️  No source files found in source_diff_dict, using complete PR diff")
        return pr_diff
    
    print(f"   📄 Source files contributing to {target_file}: {list(source_files)}")
    
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
    print(f"   📊 Filtered diff: {len(file_specific_diff)} chars (from {len(pr_diff)} chars)")
    
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

def determine_file_processing_type(source_file_path, file_sections, special_files=None):
    """Determine how to process a file based on operation type and file characteristics"""
    
    # Check if this is a special file (like TOC.md)
    if special_files and os.path.basename(source_file_path) in special_files:
        return "special_file_toc"
    
    # For all other modified files, use regular processing
    return "regular_modified"

def process_regular_modified_file(source_file_path, file_sections, file_diff, pr_url, github_client, ai_client, repo_config, max_sections):
    """Process a regular markdown file that has been modified"""
    try:
        print(f"   📝 Processing as regular modified file: {source_file_path}")
        
        # Extract the actual sections from the file_sections structure
        # file_sections contains: {'sections': {...}, 'original_hierarchy': {...}, 'current_hierarchy': {...}}
        if isinstance(file_sections, dict) and 'sections' in file_sections:
            actual_sections = file_sections['sections']
        else:
            # Fallback: assume file_sections is already the sections dict
            actual_sections = file_sections
        
        print(f"   📊 Extracted sections: {len(actual_sections)} sections")
        
        # CRITICAL: Load the source-diff-dict.json and perform matching
        import json
        import os
        from section_matcher import match_source_diff_to_target
        from pr_analyzer import get_target_hierarchy_and_content
        
        # Load source-diff-dict.json with file prefix
        temp_dir = ensure_temp_output_dir()
        file_prefix = source_file_path.replace('/', '-').replace('.md', '')
        source_diff_dict_file = os.path.join(temp_dir, f"{file_prefix}-source-diff-dict.json")
        if os.path.exists(source_diff_dict_file):
            with open(source_diff_dict_file, 'r', encoding='utf-8') as f:
                source_diff_dict = json.load(f)
            print(f"   📂 Loaded source diff dict with {len(source_diff_dict)} sections from {source_diff_dict_file}")
            
            # Check source token limit before proceeding with processing
            print(f"   🔍 Checking source token limit...")
            within_limit, total_tokens, token_limit = check_source_token_limit(source_diff_dict_file)
            if not within_limit:
                print(f"   🚫 Skipping file processing: source content exceeds token limit")
                print(f"      📊 Total tokens: {total_tokens:,} > Limit: {token_limit:,}")
                print(f"      ⏭️  File {source_file_path} will not be processed")
                return False
                
        else:
            print(f"   ❌ {source_diff_dict_file} not found")
            return False
        
        # Get target file hierarchy and content
        target_repo = repo_config['target_repo']
        target_hierarchy, target_lines = get_target_hierarchy_and_content(source_file_path, github_client, target_repo)
        
        if not target_hierarchy or not target_lines:
            print(f"   ❌ Could not get target file content for {source_file_path}")
            return False
        
        print(f"   📖 Target file: {len(target_hierarchy)} sections, {len(target_lines)} lines")
        
        # Perform source diff to target matching
        print(f"   🔗 Matching source diff to target...")
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
            print(f"   ❌ No sections matched")
            return False
        
        print(f"   ✅ Matched {len(enhanced_sections)} sections")
        
        # Save the match result for reference
        match_file = os.path.join(temp_dir, f"{source_file_path.replace('/', '-').replace('.md', '')}-match_source_diff_to_target.json")
        with open(match_file, 'w', encoding='utf-8') as f:
            json.dump(enhanced_sections, f, ensure_ascii=False, indent=2)
        print(f"   💾 Saved match result to: {match_file}")
        
        # Step 2: Get AI translation for the matched sections
        print(f"   🤖 Getting AI translation for matched sections...")
        
        # Create file data structure with enhanced matching info
        # Wrap enhanced_sections in the expected format for process_single_file
        file_data = {
            source_file_path: {
                'type': 'enhanced_sections',
                'sections': enhanced_sections
            }
        }
        
        # Call the existing process_modified_sections function to get AI translation
        results = process_modified_sections(file_data, file_diff, pr_url, github_client, ai_client, repo_config, max_sections)
        
        # Step 3: Update match_source_diff_to_target.json with AI results
        if results and len(results) > 0:
            file_path, success, ai_updated_sections = results[0]  # Get first result
            if success and isinstance(ai_updated_sections, dict):
                print(f"   📝 Step 3: Updating {match_file} with AI results...")
                
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
                
                print(f"   ✅ Updated {updated_count} sections with AI translations in {match_file}")
                
                # Step 4: Apply updates to target document using update_target_document_from_match_data
                print(f"   📝 Step 4: Applying updates to target document...")
                from file_updater import update_target_document_from_match_data
                
                success = update_target_document_from_match_data(match_file, repo_config['target_local_path'], source_file_path)
                if success:
                    print(f"   🎉 Target document successfully updated!")
                    return True
                else:
                    print(f"   ❌ Failed to update target document")
                    return False
                    
            else:
                print(f"   ⚠️  AI translation failed or returned invalid results")
                return False
        else:
            print(f"   ⚠️  No results from process_modified_sections")
            return False
        
    except Exception as e:
        print(f"   ❌ Error processing regular modified file {source_file_path}: {e}")
        return False


def get_local_repo_config(pr_url):
    """Get repository configuration using local config"""
    from pr_analyzer import parse_pr_url
    
    owner, repo, pr_number = parse_pr_url(pr_url)
    source_repo = f"{owner}/{repo}"
    
    if source_repo not in REPO_CONFIGS:
        raise ValueError(f"Unsupported source repository: {source_repo}. Supported: {list(REPO_CONFIGS.keys())}")
    
    config = REPO_CONFIGS[source_repo].copy()
    config['source_repo'] = source_repo
    config['pr_number'] = pr_number
    
    return config

def main():
    """Main function - orchestrates the entire workflow"""
    pr_url = sys.argv[1] if len(sys.argv) > 1 else SOURCE_PR_URL
    
    print(f"🔧 Auto PR Sync Tool (Bidirectional Version)")
    print(f"📍 PR URL: {pr_url}")
    
    # Clean and prepare temp_output directory
    clean_temp_output_dir()
    
    # Get repository configuration using local config
    try:
        repo_config = get_local_repo_config(pr_url)
        print(f"📁 Source Repo: {repo_config['source_repo']} ({repo_config['source_language']})")
        print(f"📁 Target Repo: {repo_config['target_repo']} ({repo_config['target_language']})")
        print(f"📁 Target Path: {repo_config['target_local_path']}")
    except ValueError as e:
        print(f"❌ {e}")
        return
    
    # Use the new authentication method to avoid deprecation warning
    from github import Auth
    auth = Auth.Token(GITHUB_TOKEN)
    github_client = Github(auth=auth)
    
    # Initialize unified AI client
    try:
        ai_client = UnifiedAIClient(provider=AI_PROVIDER)
        thread_safe_print(f"🤖 AI Provider: {AI_PROVIDER.upper()} ({ai_client.model})")
    except Exception as e:
        thread_safe_print(f"❌ Failed to initialize AI client: {e}")
        return
    
    print(f"\n🚀 Starting auto-sync for PR: {pr_url}")
    
    # Step 1: Get PR diff
    print(f"\n📋 Step 1: Getting PR diff...")
    pr_diff = get_pr_diff(pr_url, github_client)
    if not pr_diff:
        print("❌ Could not get PR diff")
        return
    print(f"✅ Got PR diff: {len(pr_diff)} characters")
    
    # Step 2: Analyze source changes with operation categorization
    print(f"\n📊 Step 2: Analyzing source changes...")
    added_sections, modified_sections, deleted_sections, added_files, deleted_files, toc_files, added_images, modified_images, deleted_images = analyze_source_changes(
        pr_url, github_client, 
        special_files=SPECIAL_FILES, 
        ignore_files=IGNORE_FILES, 
        repo_configs=REPO_CONFIGS,
        max_non_system_sections=MAX_NON_SYSTEM_SECTIONS_FOR_AI,
        pr_diff=pr_diff  # Pass the PR diff to avoid re-fetching
    )
    

    
    # Step 3: Process different types of files based on operation type
    print(f"\n📋 Step 3: Processing files based on operation type...")
    
    # Import necessary functions
    from file_updater import process_modified_sections, update_target_document_from_match_data
    from toc_processor import process_toc_files
    
    # Step 3.1: Process deleted files (file-level deletions)
    if deleted_files:
        print(f"\n🗑️  Step 3.1: Processing {len(deleted_files)} deleted files...")
        process_deleted_files(deleted_files, github_client, repo_config)
        print(f"   ✅ Deleted files processed")
    
    # Step 3.2: Process added files (file-level additions)
    if added_files:
        print(f"\n📄 Step 3.2: Processing {len(added_files)} added files...")
        process_added_files(added_files, pr_url, github_client, ai_client, repo_config)
        print(f"   ✅ Added files processed")
    
    # Step 3.3: Process special files (TOC.md and similar)
    if toc_files:
        print(f"\n📋 Step 3.3: Processing {len(toc_files)} special files (TOC)...")
        process_toc_files(toc_files, pr_url, github_client, ai_client, repo_config)
        print(f"   ✅ Special files processed")
    
    # Step 3.4: Process modified files (section-level modifications)
    if modified_sections:
        print(f"\n📝 Step 3.4: Processing {len(modified_sections)} modified files...")
        
        # Process each modified file separately
        for source_file_path, file_sections in modified_sections.items():
            print(f"\n📄 Processing modified file: {source_file_path}")
            
            # Extract file-specific diff from the complete PR diff
            print(f"   🔍 Extracting file-specific diff for: {source_file_path}")
            file_specific_diff = extract_file_diff_from_pr(pr_diff, source_file_path)
            
            if not file_specific_diff:
                print(f"   ⚠️  No diff found for {source_file_path}, skipping...")
                continue
            
            print(f"   📊 File-specific diff: {len(file_specific_diff)} chars")
            
            # Determine file processing approach for modified files
            file_type = determine_file_processing_type(source_file_path, file_sections, SPECIAL_FILES)
            print(f"   🔍 File processing type: {file_type}")
            
            if file_type == "special_file_toc":
                # Special files should have been processed in Step 3.3, skip here
                print(f"   ⏭️  Special file already processed in Step 3.3, skipping...")
                continue
            
            elif file_type == "regular_modified":
                # Regular markdown files with modifications
                success = process_regular_modified_file(
                    source_file_path, 
                    file_sections, 
                    file_specific_diff,
                    pr_url, 
                    github_client, 
                    ai_client, 
                    repo_config, 
                    MAX_NON_SYSTEM_SECTIONS_FOR_AI
                )
                
                if success:
                    print(f"   ✅ Successfully processed {source_file_path}")
                else:
                    print(f"   ❌ Failed to process {source_file_path}")
            
            else:
                print(f"   ⚠️  Unknown file processing type: {file_type} for {source_file_path}, skipping...")
    
    # Step 3.5: Process images (added, modified, deleted)
    if added_images or modified_images or deleted_images:
        print(f"\n🖼️  Step 3.5: Processing images...")
        process_all_images(added_images, modified_images, deleted_images, pr_url, github_client, repo_config)
        print(f"   ✅ Images processed")
    
    # Final summary
    print(f"\n" + "="*80)
    print(f"📊 Final Summary:")
    print(f"="*80)
    print(f"   📄 Added files: {len(added_files)} processed")
    print(f"   🗑️  Deleted files: {len(deleted_files)} processed")
    print(f"   📋 TOC files: {len(toc_files)} processed")
    print(f"   📝 Modified files: {len(modified_sections)} processed")
    print(f"   🖼️  Added images: {len(added_images)} processed")
    print(f"   🖼️  Modified images: {len(modified_images)} processed")
    print(f"   🖼️  Deleted images: {len(deleted_images)} processed")
    print(f"="*80)

if __name__ == "__main__":
    main()
