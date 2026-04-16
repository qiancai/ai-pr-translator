"""
File Adder Module
Handles processing and translation of newly added files
"""

import os
import re
import json
import threading
from github import Github
from openai import OpenAI
from log_sanitizer import sanitize_exception_message

# Thread-safe printing
print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def create_section_batches(file_content, max_lines_per_batch=200):
    """Create batches of file content for translation, respecting section boundaries"""
    lines = file_content.split('\n')
    
    # Find all section headers
    section_starts = []
    for i, line in enumerate(lines):
        line = line.strip()
        if line.startswith('#'):
            match = re.match(r'^(#{1,10})\s+(.+)', line)
            if match:
                section_starts.append(i + 1)  # 1-based line numbers
    
    # If no sections found, just batch by line count
    if not section_starts:
        batches = []
        for i in range(0, len(lines), max_lines_per_batch):
            batch_lines = lines[i:i + max_lines_per_batch]
            batches.append('\n'.join(batch_lines))
        return batches
    
    # Create batches respecting section boundaries
    batches = []
    current_batch_start = 0
    
    for i, section_start in enumerate(section_starts):
        section_start_idx = section_start - 1  # Convert to 0-based
        
        # Check if adding this section would exceed the line limit
        if (section_start_idx - current_batch_start) > max_lines_per_batch:
            # Close current batch at the previous section boundary
            if current_batch_start < section_start_idx:
                batch_lines = lines[current_batch_start:section_start_idx]
                batches.append('\n'.join(batch_lines))
                current_batch_start = section_start_idx
        
        # If this is the last section, or the next section would create a batch too large
        if i == len(section_starts) - 1:
            # Add remaining content as final batch
            batch_lines = lines[current_batch_start:]
            batches.append('\n'.join(batch_lines))
        else:
            next_section_start = section_starts[i + 1] - 1  # 0-based
            if (next_section_start - current_batch_start) > max_lines_per_batch:
                # Close current batch at current section boundary
                batch_lines = lines[current_batch_start:section_start_idx]
                if batch_lines:  # Only add non-empty batches
                    batches.append('\n'.join(batch_lines))
                current_batch_start = section_start_idx
    
    # Clean up any empty batches
    batches = [batch for batch in batches if batch.strip()]
    
    return batches

def preprocess_added_file_batch_for_heading_anchor_stability(batch_content, source_language, target_language, source_mode=""):
    """Add prompt-only stability tweaks for commit-based English -> Chinese file additions."""
    if not batch_content:
        return batch_content

    if (source_language or "").lower() != "english" or (target_language or "").lower() != "chinese":
        return batch_content

    from file_updater import (
        add_heading_anchor_if_needed,
        preprocess_aliases_line_for_zh,
        preprocess_tidb_cloud_links_in_line,
        should_apply_tidb_cloud_link_rewrite,
    )

    enable_commit_only_preprocessing = (source_mode or "").lower() == "commit"
    enable_tidb_cloud_link_rewrite = should_apply_tidb_cloud_link_rewrite(
        source_language,
        target_language,
        source_mode=source_mode,
    )

    if not enable_commit_only_preprocessing and not enable_tidb_cloud_link_rewrite:
        return batch_content

    processed_lines = []
    for line in batch_content.splitlines():
        if enable_commit_only_preprocessing:
            line = add_heading_anchor_if_needed(line)
            line = preprocess_aliases_line_for_zh(line, diff_added_only=False)
        if enable_tidb_cloud_link_rewrite:
            line = preprocess_tidb_cloud_links_in_line(line, diff_added_only=False)
        processed_lines.append(line)
    return "\n".join(processed_lines)


def translate_file_batch(batch_content, ai_client, source_language="English", target_language="Chinese", glossary_matcher=None, source_mode=""):
    """Translate a single batch of file content using AI"""
    if not batch_content.strip():
        return batch_content
    
    thread_safe_print(f"   🤖 Translating batch ({len(batch_content.split())} words)...")
    prompt_batch_content = preprocess_added_file_batch_for_heading_anchor_stability(
        batch_content,
        source_language,
        target_language,
        source_mode=source_mode,
    )

    # Build glossary section for prompt if matcher is provided
    glossary_prompt_section = ""
    glossary_instruction = ""
    if glossary_matcher:
        from glossary import filter_terms_for_content, format_terms_for_prompt
        matched_terms = filter_terms_for_content(glossary_matcher, prompt_batch_content, source_language=source_language)
        if matched_terms:
            glossary_text = format_terms_for_prompt(matched_terms)
            glossary_prompt_section = f"\n{glossary_text}\n"
            glossary_instruction = "\n6. When translating terms listed in the glossary above, use the provided translations for consistency."
            thread_safe_print(f"   📚 Matched {len(matched_terms)} glossary terms for batch translation")

    doc_variable_example = "{{{ .starter }}}"

    prompt = f"""You are a professional technical writer. Please translate the following {source_language} content to {target_language}.

IMPORTANT INSTRUCTIONS:
1. Preserve ALL Markdown formatting (headers, links, code blocks, tables, etc.)
2. Do NOT translate:
   - Code examples, SQL queries, configuration values, and doc variables/placeholders such as {doc_variable_example}. Preserve doc variables exactly as they appear, including triple braces and when they appear inside HTML attributes or tab labels.
   - Technical terms like "TiDB", "TiKV", "PD", API names, etc.
   - File paths, URLs, and command line examples
   - Variable names and system configuration parameters
3. Translate only the descriptive text and explanations
4. Maintain the exact structure and indentation
5. Keep all special characters and formatting intact{glossary_instruction}
6. Preserve explicit heading anchors such as {{#example-test}} exactly as they appear.

Glossary for terms in {source_language} and {target_language}:
{glossary_prompt_section}

Content to translate:
{prompt_batch_content}

Please provide the translated content maintaining all formatting and structure."""

    # Add token estimation
    try:
        from main import print_token_estimation
        print_token_estimation(prompt, "File addition translation")
    except ImportError:
        # Fallback if import fails - use tiktoken
        try:
            import tiktoken
            enc = tiktoken.get_encoding("cl100k_base")
            tokens = enc.encode(prompt)
            actual_tokens = len(tokens)
            char_count = len(prompt)
            print(f"   💰 File addition translation")
            print(f"      📝 Input: {char_count:,} characters")
            print(f"      🔢 Actual tokens: {actual_tokens:,} (using tiktoken cl100k_base)")
        except Exception:
            # Final fallback to character approximation
            estimated_tokens = len(prompt) // 4
            char_count = len(prompt)
            print(f"   💰 File addition translation")
            print(f"      📝 Input: {char_count:,} characters")
            print(f"      🔢 Estimated tokens: ~{estimated_tokens:,} (fallback: 4 chars/token approximation)")
    
    try:
        translated_content = ai_client.chat_completion(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        thread_safe_print(f"   ✅ Batch translation completed")
        return translated_content
        
    except Exception as e:
        thread_safe_print(f"   ❌ Batch translation failed: {sanitize_exception_message(e)}")
        return batch_content  # Return original content if translation fails

def process_added_files(
    added_files,
    source_context_or_pr_url,
    github_client,
    ai_client,
    repo_config,
    glossary_matcher=None,
    return_details=False,
):
    """Process newly added files by translating and creating them in target repository.

    source_context_or_pr_url is currently unused here, but the shared signature keeps
    PR-based and commit-based call sites aligned.
    """
    if not added_files:
        thread_safe_print("\n📄 No new files to process")
        return (True, {}) if return_details else True
    
    thread_safe_print(f"\n📄 Processing {len(added_files)} newly added files...")
    all_success = True
    failure_reasons = {}
    
    target_local_path = repo_config['target_local_path']
    source_language = repo_config['source_language']
    target_language = repo_config['target_language']
    from file_updater import get_source_mode
    source_mode = get_source_mode(source_context_or_pr_url)
    
    for file_path, file_content in added_files.items():
        thread_safe_print(f"\n📝 Processing new file: {file_path}")
        
        # Create target file path
        target_file_path = os.path.join(target_local_path, file_path)
        target_dir = os.path.dirname(target_file_path)
        
        # Create directory if it doesn't exist
        if not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)
            thread_safe_print(f"   📁 Created directory: {target_dir}")
        
        # Check if file already exists
        if os.path.exists(target_file_path):
            reason = f"Target file already exists: {target_file_path}"
            thread_safe_print(f"   ⚠️  {reason}")
            failure_reasons[file_path] = reason
            all_success = False
            continue
        
        # Create section batches for translation
        batches = create_section_batches(file_content, max_lines_per_batch=200)
        thread_safe_print(f"   📦 Created {len(batches)} batches for translation")
        
        # Translate each batch
        translated_batches = []
        for i, batch in enumerate(batches):
            thread_safe_print(f"   🔄 Processing batch {i+1}/{len(batches)}")
            translated_batch = translate_file_batch(
                batch, 
                ai_client, 
                source_language, 
                target_language,
                glossary_matcher=glossary_matcher,
                source_mode=source_mode,
            )
            translated_batches.append(translated_batch)
        
        # Combine translated batches
        translated_content = '\n'.join(translated_batches)
        
        # Write translated content to target file
        try:
            with open(target_file_path, 'w', encoding='utf-8') as f:
                f.write(translated_content)
            
            thread_safe_print(f"   ✅ Created translated file: {target_file_path}")
            
        except Exception as e:
            reason = f"Error creating file {target_file_path}: {sanitize_exception_message(e)}"
            thread_safe_print(
                f"   ❌ {reason}"
            )
            failure_reasons[file_path] = reason
            all_success = False
    
    thread_safe_print(f"\n✅ Completed processing all new files")
    return (all_success, failure_reasons) if return_details else all_success
