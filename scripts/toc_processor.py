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
from log_sanitizer import sanitize_exception_message

# Thread-safe printing
print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)


TOC_LIST_TEXT_LINE_RE = re.compile(r'^(\s*-\s+)(.*)$')
TOC_HEADING_LINE_RE = re.compile(r'^(#{1,10}\s+)(.+)$')


def find_matching_bracket(text, open_index):
    depth = 0
    index = open_index
    while index < len(text):
        char = text[index]
        if char == "\\":
            index += 2
            continue
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return -1


def parse_toc_markdown_link_line(line):
    prefix_match = re.match(r'^(\s*-\s*)', line)
    if not prefix_match:
        return None

    prefix = prefix_match.group(1)
    rest = line[len(prefix):]
    if not rest.startswith("["):
        return None

    close_bracket_index = find_matching_bracket(rest, 0)
    if close_bracket_index == -1:
        return None

    if close_bracket_index + 1 >= len(rest) or rest[close_bracket_index + 1] != "(":
        return None

    close_paren_index = rest.find(")", close_bracket_index + 2)
    if close_paren_index == -1:
        return None

    return {
        "type": "link",
        "prefix": prefix,
        "text": rest[1:close_bracket_index],
        "link": rest[close_bracket_index + 2:close_paren_index],
        "suffix": rest[close_paren_index + 1:],
    }


def parse_toc_line(line):
    """Parse a TOC line into the small subset we need for stable syncing."""
    link_entry = parse_toc_markdown_link_line(line)
    if link_entry:
        return link_entry

    list_match = TOC_LIST_TEXT_LINE_RE.match(line)
    if list_match:
        return {
            "type": "list_text",
            "prefix": list_match.group(1),
            "text": list_match.group(2),
        }

    heading_match = TOC_HEADING_LINE_RE.match(line)
    if heading_match:
        return {
            "type": "heading",
            "prefix": heading_match.group(1),
            "text": heading_match.group(2),
        }

    return {"type": "other", "text": line}


def compose_toc_line_with_target_text(source_line, target_line):
    """Use source structure while preserving the translated target display text."""
    source_entry = parse_toc_line(source_line)
    target_entry = parse_toc_line(target_line)

    if source_entry["type"] == "link" and target_entry["type"] == "link":
        return (
            f"{source_entry['prefix']}[{target_entry['text']}]"
            f"({source_entry['link']}){source_entry['suffix']}"
        )

    if source_entry["type"] == "list_text" and target_entry["type"] == "list_text":
        return f"{source_entry['prefix']}{target_entry['text']}"

    if source_entry["type"] == "heading" and target_entry["type"] == "heading":
        return f"{source_entry['prefix']}{target_entry['text']}"

    return target_line


def plain_translation_key(entry):
    if entry["type"] == "heading":
        return ("heading", entry["prefix"].strip(), entry["text"])
    if entry["type"] == "list_text":
        return ("list_text", entry["text"])
    return None


def build_toc_translation_memory(source_base_lines, target_lines):
    """Build link and plain-line translation memory from the current target TOC."""
    base_link_entries = {}
    target_link_lines = {}

    for line in source_base_lines:
        entry = parse_toc_line(line)
        if entry["type"] == "link":
            base_link_entries.setdefault(entry["link"], entry)

    for line in target_lines:
        entry = parse_toc_line(line)
        if entry["type"] == "link":
            target_link_lines.setdefault(entry["link"], line)

    base_plain_entries = []
    target_plain_lines = []

    for line in source_base_lines:
        entry = parse_toc_line(line)
        if plain_translation_key(entry):
            base_plain_entries.append((entry, line))

    for line in target_lines:
        entry = parse_toc_line(line)
        if plain_translation_key(entry):
            target_plain_lines.append((entry, line))

    plain_line_map = {}
    for (base_entry, _), (target_entry, target_line) in zip(base_plain_entries, target_plain_lines):
        if base_entry["type"] != target_entry["type"]:
            continue
        key = plain_translation_key(base_entry)
        if key:
            plain_line_map.setdefault(key, target_line)

    return {
        "base_link_entries": base_link_entries,
        "target_link_lines": target_link_lines,
        "plain_line_map": plain_line_map,
    }


def plan_synced_toc_lines(source_base_content, source_head_content, target_content, source_added_line_numbers=None):
    """Plan a full TOC rewrite that follows source head structure.

    Existing target translations are reused when the source display text did not
    change. New or renamed TOC rows are returned for batched AI translation.
    """
    source_base_lines = source_base_content.split("\n")
    source_head_lines = source_head_content.split("\n")
    target_lines = target_content.split("\n")
    memory = build_toc_translation_memory(source_base_lines, target_lines)
    source_added_line_numbers = set(source_added_line_numbers or [])

    planned_lines = []
    lines_to_translate = []

    for source_line_num, source_line in enumerate(source_head_lines, 1):
        entry = parse_toc_line(source_line)

        if entry["type"] == "link":
            base_entry = memory["base_link_entries"].get(entry["link"])
            target_line = memory["target_link_lines"].get(entry["link"])
            if target_line and base_entry and base_entry["text"] == entry["text"]:
                planned_lines.append(compose_toc_line_with_target_text(source_line, target_line))
            else:
                planned_lines.append(None)
                lines_to_translate.append((len(planned_lines) - 1, source_line))
            continue

        key = plain_translation_key(entry)
        if key and source_line_num in source_added_line_numbers:
            planned_lines.append(None)
            lines_to_translate.append((len(planned_lines) - 1, source_line))
        elif key and key in memory["plain_line_map"]:
            planned_lines.append(compose_toc_line_with_target_text(source_line, memory["plain_line_map"][key]))
        elif entry["type"] in ("heading", "list_text"):
            planned_lines.append(None)
            lines_to_translate.append((len(planned_lines) - 1, source_line))
        else:
            planned_lines.append(source_line)

    return planned_lines, lines_to_translate


def extract_toc_link_from_line(line):
    """Extract the link part (including parentheses) from a TOC line"""
    entry = parse_toc_line(line)
    if entry["type"] == "link":
        return f"({entry['link']})"
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
            thread_safe_print(
                f"   ❌ Failed to parse AI translation response: {sanitize_exception_message(e)}"
            )
            return {}
            
    except Exception as e:
        thread_safe_print(f"   ❌ AI translation failed: {sanitize_exception_message(e)}")
        return {}


def translate_toc_full_lines(lines_to_translate, ai_client, repo_config):
    """Translate full TOC lines while preserving list structure and links."""
    if not lines_to_translate:
        return {}

    thread_safe_print(f"   🤖 Translating {len(lines_to_translate)} new or renamed TOC lines...")

    content_dict = {
        f"line_{i}": line
        for i, (_, line) in enumerate(lines_to_translate)
    }
    source_lang = repo_config["source_language"]
    target_lang = repo_config["target_language"]

    prompt = f"""You are a professional translator. Please translate the following Table of Contents lines from {source_lang} to {target_lang}.

IMPORTANT INSTRUCTIONS:
1. Preserve indentation, list markers, markdown links, image links, URLs, anchors, and trailing markers exactly.
2. Translate only human-readable TOC text.
3. Keep template placeholders such as {{{{ .dedicated }}}} unchanged.
4. Keep product names and technical terms unchanged when they should not be localized.
5. Return valid JSON only.

Input lines to translate:
{json.dumps(content_dict, indent=2, ensure_ascii=False)}

Return format:
{{
  "line_0": "translated line with preserved formatting",
  "line_1": "translated line with preserved formatting"
}}"""

    try:
        ai_response = ai_client.chat_completion(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
        json_start = ai_response.find("{")
        json_end = ai_response.rfind("}") + 1
        if json_start == -1 or json_end <= json_start:
            return {}

        translated_lines = json.loads(ai_response[json_start:json_end])
        translation_mapping = {}
        for i, (line_index, _) in enumerate(lines_to_translate):
            key = f"line_{i}"
            if key in translated_lines:
                translation_mapping[line_index] = translated_lines[key]

        thread_safe_print(f"   ✅ Successfully translated {len(translation_mapping)} TOC lines")
        return translation_mapping
    except Exception as e:
        thread_safe_print(f"   ❌ TOC full-line translation failed: {sanitize_exception_message(e)}")
        return {}


def process_toc_file_by_source_snapshot(file_path, toc_data, ai_client, repo_config, target_file_path):
    """Rewrite target TOC to mirror source head structure with target translations."""
    with open(target_file_path, "r", encoding="utf-8") as f:
        target_content = f.read()

    planned_lines, lines_to_translate = plan_synced_toc_lines(
        toc_data["source_base_content"],
        toc_data["source_head_content"],
        target_content,
        source_added_line_numbers=toc_data.get("source_added_line_numbers", []),
    )
    translations = translate_toc_full_lines(lines_to_translate, ai_client, repo_config)

    missing_translations = []
    for line_index, source_line in lines_to_translate:
        if line_index in translations:
            planned_lines[line_index] = translations[line_index]
        else:
            planned_lines[line_index] = source_line
            missing_translations.append(source_line.strip())

    if missing_translations:
        thread_safe_print(
            f"   ❌ {len(missing_translations)} TOC line(s) were not translated"
        )
        for missing_line in missing_translations[:10]:
            thread_safe_print(f"      - {missing_line}")
        if len(missing_translations) > 10:
            thread_safe_print(f"      ... and {len(missing_translations) - 10} more")
        return False

    with open(target_file_path, "w", encoding="utf-8") as f:
        f.write("\n".join(planned_lines))

    thread_safe_print(f"   ✅ TOC file synced from source snapshot: {file_path}")
    return True


def process_toc_file(file_path, toc_data, source_context_or_pr_url, github_client, ai_client, repo_config):
    """Process a single TOC.md file with special logic.

    Returns:
      bool: True if the TOC file is processed successfully, else False.
    """
    thread_safe_print(f"\n📋 Processing TOC file: {file_path}")
    
    try:
        target_local_path = repo_config['target_local_path']
        target_file_path = os.path.join(target_local_path, file_path)

        if toc_data.get("source_base_content") and toc_data.get("source_head_content"):
            return process_toc_file_by_source_snapshot(
                file_path,
                toc_data,
                ai_client,
                repo_config,
                target_file_path,
            )
        
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
        return True
        
    except Exception as e:
        thread_safe_print(
            f"   ❌ Error processing TOC file {file_path}: {sanitize_exception_message(e)}"
        )
        return False

def process_toc_files(toc_files, source_context_or_pr_url, github_client, ai_client, repo_config):
    """Process all TOC files.

    Returns:
      bool: True if all TOC files are processed successfully, else False.
    """
    if not toc_files:
        return True
    
    thread_safe_print(f"\n📋 Processing {len(toc_files)} TOC files...")
    all_success = True
    
    for file_path, toc_data in toc_files.items():
        if toc_data['type'] == 'toc':
            success = process_toc_file(file_path, toc_data, source_context_or_pr_url, github_client, ai_client, repo_config)
            if not success:
                all_success = False
        else:
            thread_safe_print(f"   ⚠️  Unknown TOC data type: {toc_data['type']} for {file_path}")
            all_success = False
    
    if all_success:
        thread_safe_print(f"   ✅ All TOC files processed")
    else:
        thread_safe_print(f"   ⚠️  Some TOC files failed to process")
    return all_success
