"""
TOC Processor Module
Handles special processing logic for TOC.md files
"""

import os
import re
import json
import threading
from github import Github
from openai import OpenAI

# Thread-safe printing
print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def extract_toc_link_from_line(line):
    """Extract the link part (including parentheses) from a TOC line"""
    # Pattern to match [text](link) format
    pattern = r'\[([^\]]+)\]\(([^)]+)\)'
    match = re.search(pattern, line)
    if match:
        return f"({match.group(2)})"  # Return (link) including parentheses
    return None

def is_toc_translation_needed(line):
    """Check if a TOC line needs translation based on content in square brackets"""
    # Extract content within square brackets [content]
    pattern = r'\[([^\]]+)\]'
    match = re.search(pattern, line)
    if match:
        content = match.group(1)
        # Skip translation if content has no Chinese and no spaces
        has_chinese = bool(re.search(r'[\u4e00-\u9fff]', content))
        has_spaces = ' ' in content
        
        # Need translation if has Chinese OR has spaces
        # Skip translation only if it's alphanumeric/technical term without spaces
        return has_chinese or has_spaces
    return True  # Default to translate if can't parse

def find_best_toc_match(target_link, target_lines, source_line_num):
    """Find the best matching line in target TOC based on link content and line proximity"""
    matches = []
    
    for i, line in enumerate(target_lines):
        line_link = extract_toc_link_from_line(line.strip())
        if line_link and line_link == target_link:
            matches.append({
                'line_num': i + 1,  # Convert to 1-based
                'line': line.strip(),
                'distance': abs((i + 1) - source_line_num)
            })
    
    if not matches:
        return None
    
    # Sort by distance to source line number, choose the closest one
    matches.sort(key=lambda x: x['distance'])
    return matches[0]

def group_consecutive_lines(lines):
    """Group consecutive lines together"""
    if not lines:
        return []
    
    # Sort lines by line number
    sorted_lines = sorted(lines, key=lambda x: x['line_number'])
    
    groups = []
    current_group = [sorted_lines[0]]
    
    for i in range(1, len(sorted_lines)):
        current_line = sorted_lines[i]
        prev_line = sorted_lines[i-1]
        
        # Consider lines consecutive if they are within 2 lines of each other
        if current_line['line_number'] - prev_line['line_number'] <= 2:
            current_group.append(current_line)
        else:
            groups.append(current_group)
            current_group = [current_line]
    
    groups.append(current_group)
    return groups

def process_toc_operations(file_path, operations, source_lines, target_lines, target_local_path):
    """Process TOC.md file operations with special logic"""
    thread_safe_print(f"\n📋 Processing TOC.md with special logic...")
    
    results = {
        'added': [],
        'modified': [],
        'deleted': []
    }
    
    # Process deleted lines first
    for deleted_line in operations['deleted_lines']:
        if not deleted_line['is_header']:  # TOC lines are not headers
            deleted_content = deleted_line['content']
            deleted_link = extract_toc_link_from_line(deleted_content)
            
            if deleted_link:
                thread_safe_print(f"   🗑️  Processing deleted TOC line with link: {deleted_link}")
                
                # Find matching line in target
                match = find_best_toc_match(deleted_link, target_lines, deleted_line['line_number'])
                if match:
                    thread_safe_print(f"      ✅ Found target line {match['line_num']}: {match['line']}")
                    results['deleted'].append({
                        'source_line': deleted_line['line_number'],
                        'target_line': match['line_num'],
                        'content': deleted_content
                    })
                else:
                    thread_safe_print(f"      ❌ No matching line found for {deleted_link}")
    
    # Process added lines
    added_groups = group_consecutive_lines(operations['added_lines'])
    for group in added_groups:
        if group:  # Skip empty groups
            first_added_line = group[0]
            thread_safe_print(f"   ➕ Processing added TOC group starting at line {first_added_line['line_number']}")
            
            # Find the previous line in source to determine insertion point
            previous_line_num = first_added_line['line_number'] - 1
            if previous_line_num > 0 and previous_line_num <= len(source_lines):
                previous_line_content = source_lines[previous_line_num - 1]
                previous_link = extract_toc_link_from_line(previous_line_content)
                
                if previous_link:
                    thread_safe_print(f"      📍 Previous line link: {previous_link}")
                    
                    # Find matching previous line in target
                    match = find_best_toc_match(previous_link, target_lines, previous_line_num)
                    if match:
                        thread_safe_print(f"      ✅ Found target insertion point after line {match['line_num']}")
                        
                        # Process each line in the group
                        for added_line in group:
                            added_content = added_line['content']
                            if is_toc_translation_needed(added_content):
                                results['added'].append({
                                    'source_line': added_line['line_number'],
                                    'target_insertion_after': match['line_num'],
                                    'content': added_content,
                                    'needs_translation': True
                                })
                                thread_safe_print(f"         📝 Added for translation: {added_content.strip()}")
                            else:
                                results['added'].append({
                                    'source_line': added_line['line_number'],
                                    'target_insertion_after': match['line_num'],
                                    'content': added_content,
                                    'needs_translation': False
                                })
                                thread_safe_print(f"         ⏭️  Added without translation: {added_content.strip()}")
                    else:
                        thread_safe_print(f"      ❌ No target insertion point found for {previous_link}")
                else:
                    thread_safe_print(f"      ❌ No link found in previous line: {previous_line_content.strip()}")
    
    # Process modified lines  
    modified_groups = group_consecutive_lines(operations['modified_lines'])
    for group in modified_groups:
        if group:  # Skip empty groups
            first_modified_line = group[0]
            thread_safe_print(f"   ✏️  Processing modified TOC group starting at line {first_modified_line['line_number']}")
            
            # Find the previous line in source to determine target location
            previous_line_num = first_modified_line['line_number'] - 1
            if previous_line_num > 0 and previous_line_num <= len(source_lines):
                previous_line_content = source_lines[previous_line_num - 1]
                previous_link = extract_toc_link_from_line(previous_line_content)
                
                if previous_link:
                    thread_safe_print(f"      📍 Previous line link: {previous_link}")
                    
                    # Find matching previous line in target
                    match = find_best_toc_match(previous_link, target_lines, previous_line_num)
                    if match:
                        # Process each line in the group
                        for modified_line in group:
                            modified_content = modified_line['content']
                            if is_toc_translation_needed(modified_content):
                                results['modified'].append({
                                    'source_line': modified_line['line_number'],
                                    'target_line_context': match['line_num'],
                                    'content': modified_content,
                                    'needs_translation': True
                                })
                                thread_safe_print(f"         📝 Modified for translation: {modified_content.strip()}")
                            else:
                                results['modified'].append({
                                    'source_line': modified_line['line_number'],
                                    'target_line_context': match['line_num'],
                                    'content': modified_content,
                                    'needs_translation': False
                                })
                                thread_safe_print(f"         ⏭️  Modified without translation: {modified_content.strip()}")
                    else:
                        thread_safe_print(f"      ❌ No target context found for {previous_link}")
                else:
                    thread_safe_print(f"      ❌ No link found in previous line: {previous_line_content.strip()}")
    
    return results

def find_toc_modification_line(mod_op, target_lines):
    """Find the actual line number to modify in target TOC based on context"""
    # This function helps find the exact line to modify in target TOC
    # based on the modification operation context
    
    target_line_context = mod_op.get('target_line_context', 0)
    
    # Look for the line after the context line that should be modified
    # This is a simplified approach - in practice, you might need more sophisticated logic
    
    if target_line_context > 0 and target_line_context < len(target_lines):
        # Check if the next line is the one to modify
        return target_line_context + 1
    
    return target_line_context

def translate_toc_lines(toc_operations, ai_client, repo_config):
    """Translate multiple TOC lines at once"""
    lines_to_translate = []
    
    # Collect all lines that need translation
    for op in toc_operations:
        if op.get('needs_translation', False):
            lines_to_translate.append({
                'operation_type': 'added' if 'target_insertion_after' in op else 'modified',
                'content': op['content'],
                'source_line': op['source_line']
            })
    
    if not lines_to_translate:
        thread_safe_print(f"   ⏭️  No TOC lines need translation")
        return {}
    
    thread_safe_print(f"   🤖 Translating {len(lines_to_translate)} TOC lines...")
    
    # Prepare content for AI translation
    content_dict = {}
    for i, line_info in enumerate(lines_to_translate):
        content_dict[f"line_{i}"] = line_info['content']
    
    source_lang = repo_config['source_language']
    target_lang = repo_config['target_language']
    
    prompt = f"""You are a professional translator. Please translate the following TOC (Table of Contents) lines from {source_lang} to {target_lang}.

IMPORTANT INSTRUCTIONS:
1. Preserve ALL formatting, indentation, spaces, and dashes exactly as they appear
2. Only translate the text content within square brackets [text]
3. Keep all markdown links, parentheses, and special characters unchanged
4. Maintain the exact same indentation and spacing structure

Input lines to translate:
{json.dumps(content_dict, indent=2, ensure_ascii=False)}

Please return the translated lines in the same JSON format, preserving all formatting and only translating the text within square brackets.

Return format:
{{
  "line_0": "translated line with preserved formatting",
  "line_1": "translated line with preserved formatting"
}}"""

    #print(prompt) #DEBUG
    # Add token estimation
    try:
        from main import print_token_estimation
        print_token_estimation(prompt, "TOC translation")
    except ImportError:
        # Fallback if import fails - use tiktoken
        try:
            import tiktoken
            enc = tiktoken.get_encoding("cl100k_base")
            tokens = enc.encode(prompt)
            actual_tokens = len(tokens)
            char_count = len(prompt)
            print(f"   💰 TOC translation")
            print(f"      📝 Input: {char_count:,} characters")
            print(f"      🔢 Actual tokens: {actual_tokens:,} (using tiktoken cl100k_base)")
        except Exception:
            # Final fallback to character approximation
            estimated_tokens = len(prompt) // 4
            char_count = len(prompt)
            print(f"   💰 TOC translation")
            print(f"      📝 Input: {char_count:,} characters")
            print(f"      🔢 Estimated tokens: ~{estimated_tokens:,} (fallback: 4 chars/token approximation)")
    
    try:
        ai_response = ai_client.chat_completion(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        #print(ai_response) #DEBUG
        thread_safe_print(f"   📝 AI translation response received")
        
        # Parse AI response
        try:
            json_start = ai_response.find('{')
            json_end = ai_response.rfind('}') + 1
            
            if json_start != -1 and json_end > json_start:
                json_str = ai_response[json_start:json_end]
                translated_lines = json.loads(json_str)
                
                # Map back to original operations
                translation_mapping = {}
                for i, line_info in enumerate(lines_to_translate):
                    key = f"line_{i}"
                    if key in translated_lines:
                        translation_mapping[line_info['source_line']] = translated_lines[key]
                
                thread_safe_print(f"   ✅ Successfully translated {len(translation_mapping)} TOC lines")
                return translation_mapping
                
        except json.JSONDecodeError as e:
            thread_safe_print(f"   ❌ Failed to parse AI translation response: {e}")
            return {}
            
    except Exception as e:
        thread_safe_print(f"   ❌ AI translation failed: {e}")
        return {}

def process_toc_file(file_path, toc_data, pr_url, github_client, ai_client, repo_config):
    """Process a single TOC.md file with special logic"""
    thread_safe_print(f"\n📋 Processing TOC file: {file_path}")
    
    try:
        target_local_path = repo_config['target_local_path']
        target_file_path = os.path.join(target_local_path, file_path)
        
        # Read current target file
        with open(target_file_path, 'r', encoding='utf-8') as f:
            target_content = f.read()
        
        target_lines = target_content.split('\n')
        operations = toc_data['operations']
        
        # Separate operations by type
        deleted_ops = [op for op in operations if 'target_line' in op]
        added_ops = [op for op in operations if 'target_insertion_after' in op]
        modified_ops = [op for op in operations if 'target_line_context' in op]
        
        thread_safe_print(f"   📊 TOC operations: {len(deleted_ops)} deleted, {len(added_ops)} added, {len(modified_ops)} modified")
        
        # Process deletions first (work backwards to maintain line numbers)
        if deleted_ops:
            thread_safe_print(f"   🗑️  Processing {len(deleted_ops)} deletions...")
            deleted_ops.sort(key=lambda x: x['target_line'], reverse=True)
            
            for del_op in deleted_ops:
                target_line_num = del_op['target_line'] - 1  # Convert to 0-based
                if 0 <= target_line_num < len(target_lines):
                    thread_safe_print(f"      ❌ Deleting line {del_op['target_line']}: {target_lines[target_line_num].strip()}")
                    del target_lines[target_line_num]
        
        # Process modifications
        if modified_ops:
            thread_safe_print(f"   ✏️  Processing {len(modified_ops)} modifications...")
            
            # Get translations for operations that need them
            translations = translate_toc_lines(modified_ops, ai_client, repo_config)
            
            for mod_op in modified_ops:
                target_line_num = find_toc_modification_line(mod_op, target_lines) - 1  # Convert to 0-based
                
                if 0 <= target_line_num < len(target_lines):
                    if mod_op.get('needs_translation', False) and mod_op['source_line'] in translations:
                        new_content = translations[mod_op['source_line']]
                        thread_safe_print(f"      ✏️  Modifying line {target_line_num + 1} with translation")
                    else:
                        new_content = mod_op['content']
                        thread_safe_print(f"      ✏️  Modifying line {target_line_num + 1} without translation")
                    
                    target_lines[target_line_num] = new_content
        
        # Process additions last
        if added_ops:
            thread_safe_print(f"   ➕ Processing {len(added_ops)} additions...")
            
            # Get translations for operations that need them
            translations = translate_toc_lines(added_ops, ai_client, repo_config)
            
            # Group additions by insertion point and process in reverse order
            added_ops.sort(key=lambda x: x['target_insertion_after'], reverse=True)
            
            for add_op in added_ops:
                insertion_after = add_op['target_insertion_after']
                
                if add_op.get('needs_translation', False) and add_op['source_line'] in translations:
                    new_content = translations[add_op['source_line']]
                    thread_safe_print(f"      ➕ Inserting after line {insertion_after} with translation")
                else:
                    new_content = add_op['content']
                    thread_safe_print(f"      ➕ Inserting after line {insertion_after} without translation")
                
                # Insert the new line
                if insertion_after < len(target_lines):
                    target_lines.insert(insertion_after, new_content)
                else:
                    target_lines.append(new_content)
        
        # Write updated content back to file
        updated_content = '\n'.join(target_lines)
        with open(target_file_path, 'w', encoding='utf-8') as f:
            f.write(updated_content)
        
        thread_safe_print(f"   ✅ TOC file updated: {file_path}")
        
    except Exception as e:
        thread_safe_print(f"   ❌ Error processing TOC file {file_path}: {e}")

def process_toc_files(toc_files, pr_url, github_client, ai_client, repo_config):
    """Process all TOC files"""
    if not toc_files:
        return
    
    thread_safe_print(f"\n📋 Processing {len(toc_files)} TOC files...")
    
    for file_path, toc_data in toc_files.items():
        if toc_data['type'] == 'toc':
            process_toc_file(file_path, toc_data, pr_url, github_client, ai_client, repo_config)
        else:
            thread_safe_print(f"   ⚠️  Unknown TOC data type: {toc_data['type']} for {file_path}")
    
    thread_safe_print(f"   ✅ All TOC files processed")
