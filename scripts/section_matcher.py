"""
Section Matcher Module
Handles section hierarchy matching including direct matching and AI matching
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

def clean_title_for_matching(title):
    """Clean title for matching by removing markdown formatting and span elements"""
    if not title:
        return ""
    
    # Remove span elements like <span class="version-mark">New in v5.0</span>
    title = re.sub(r'<span[^>]*>.*?</span>', '', title)
    
    # Remove markdown header prefix (# ## ### etc.)
    title = re.sub(r'^#{1,6}\s*', '', title.strip())
    
    # Remove backticks
    title = title.replace('`', '')
    
    # Strip whitespace
    title = title.strip()
    
    return title

def is_system_variable_or_config(title):
    """Check if a title represents a system variable or configuration item"""
    cleaned_title = clean_title_for_matching(title)
    
    if not cleaned_title:
        return False
    
    # Check if original title had backticks (indicating code/config item)
    original_has_backticks = '`' in title
    
    # System variables and config items are typically:
    # 1. Alphanumeric characters with underscores, hyphens, dots, or percent signs
    # 2. No spaces in the middle
    # 3. Often contain underscores, hyphens, dots, or percent signs
    # 4. May contain uppercase letters (like alert rule names)
    # 5. Single words wrapped in backticks (like `capacity`, `engine`)
    
    # Check if it contains only allowed characters (including % for metrics/alerts)
    allowed_chars = re.match(r'^[a-zA-Z0-9_\-\.%]+$', cleaned_title)
    
    # Check if it contains at least one separator (common in system vars/config/alerts)
    has_separator = ('_' in cleaned_title or '-' in cleaned_title or 
                    '.' in cleaned_title or '%' in cleaned_title)
    
    # Check if it doesn't contain spaces (spaces would indicate it's likely a regular title)
    no_spaces = ' ' not in cleaned_title
    
    # Additional patterns for alert rules and metrics
    is_alert_rule = (cleaned_title.startswith('PD_') or 
                    cleaned_title.startswith('TiDB_') or
                    cleaned_title.startswith('TiKV_') or
                    cleaned_title.endswith('_alert') or
                    '%' in cleaned_title)
    
    # NEW: Check if it's a single word in backticks (config/variable name)
    # Examples: `capacity`, `engine`, `enable`, `dirname` etc.
    is_single_backticked_word = (original_has_backticks and 
                                allowed_chars and 
                                no_spaces and 
                                len(cleaned_title.split()) == 1)
    
    return bool(allowed_chars and (has_separator or is_alert_rule or is_single_backticked_word) and no_spaces)

def find_toplevel_title_matches(source_sections, target_lines):
    """Find matches for top-level titles (# Level) by direct pattern matching"""
    matched_dict = {}
    failed_matches = []
    skipped_sections = []
    
    thread_safe_print(f"🔍 Searching for top-level title matches")
    
    for source_line_num, source_hierarchy in source_sections.items():
        # Extract the leaf title from hierarchy
        source_leaf_title = source_hierarchy.split(' > ')[-1] if ' > ' in source_hierarchy else source_hierarchy
        
        # Only process top-level titles
        if not source_leaf_title.startswith('# '):
            skipped_sections.append({
                'line_num': source_line_num,
                'hierarchy': source_hierarchy,
                'reason': 'Not a top-level title'
            })
            continue
        
        thread_safe_print(f"   📝 Looking for top-level match: {source_leaf_title}")
        
        # Find the first top-level title in target document
        target_match = None
        for line_num, line in enumerate(target_lines, 1):
            line = line.strip()
            if line.startswith('# '):
                target_match = {
                    'line_num': line_num,
                    'title': line,
                    'hierarchy_string': line[2:].strip()  # Remove '# ' prefix for hierarchy
                }
                thread_safe_print(f"      ✓ Found target top-level at line {line_num}: {line}")
                break
        
        if target_match:
            matched_dict[str(target_match['line_num'])] = target_match['hierarchy_string']
            thread_safe_print(f"      ✅ Top-level match: line {target_match['line_num']}")
        else:
            thread_safe_print(f"      ❌ No top-level title found in target")
            failed_matches.append({
                'line_num': source_line_num,
                'hierarchy': source_hierarchy,
                'reason': 'No top-level title found in target'
            })
    
    thread_safe_print(f"📊 Top-level matching result: {len(matched_dict)} matches found")
    if failed_matches:
        thread_safe_print(f"⚠️  {len(failed_matches)} top-level sections failed to match:")
        for failed in failed_matches:
            thread_safe_print(f"      ❌ Line {failed['line_num']}: {failed['hierarchy']} - {failed['reason']}")
    
    return matched_dict, failed_matches, skipped_sections


def find_direct_matches_for_special_files(source_sections, target_hierarchy, target_lines):
    """Find direct matches for system variables/config items without using AI"""
    matched_dict = {}
    failed_matches = []
    skipped_sections = []
    
    # Build target headers with hierarchy paths
    target_headers = {}
    for line_num, line in enumerate(target_lines, 1):
        line = line.strip()
        if line.startswith('#'):
            match = re.match(r'^(#{1,10})\s+(.+)', line)
            if match:
                level = len(match.group(1))
                title = match.group(2).strip()
                target_headers[line_num] = {
                    'level': level,
                    'title': title,
                    'line': line
                }
    
    thread_safe_print(f"   🔍 Searching for direct matches among {len(target_headers)} target headers")
    
    for source_line_num, source_hierarchy in source_sections.items():
        # Extract the leaf title from hierarchy
        source_leaf_title = source_hierarchy.split(' > ')[-1] if ' > ' in source_hierarchy else source_hierarchy
        source_clean_title = clean_title_for_matching(source_leaf_title)
        
        thread_safe_print(f"   📝 Looking for match: {source_clean_title}")
        
        if not is_system_variable_or_config(source_leaf_title):
            thread_safe_print(f"      ⚠️  Not a system variable/config, skipping direct match")
            skipped_sections.append({
                'line_num': source_line_num,
                'hierarchy': source_hierarchy,
                'reason': 'Not a system variable or config item'
            })
            continue
        
        # Find potential matches in target
        potential_matches = []
        for target_line_num, target_header in target_headers.items():
            target_clean_title = clean_title_for_matching(target_header['title'])
            
            if source_clean_title == target_clean_title:
                # Build hierarchy path for this target header
                hierarchy_path = build_hierarchy_path(target_lines, target_line_num, target_headers)
                potential_matches.append({
                    'line_num': target_line_num,
                    'header': target_header,
                    'hierarchy_path': hierarchy_path,
                    'hierarchy_string': ' > '.join([f"{'#' * h['level']} {h['title']}" for h in hierarchy_path if h['level'] > 1 or len(hierarchy_path) == 1])
                })
                thread_safe_print(f"      ✓ Found potential match at line {target_line_num}: {target_header['title']}")
        
        if len(potential_matches) == 1:
            # Single match found
            match = potential_matches[0]
            matched_dict[str(match['line_num'])] = match['hierarchy_string']
            thread_safe_print(f"      ✅ Direct match: line {match['line_num']}")
        elif len(potential_matches) > 1:
            # Multiple matches, need to use parent hierarchy to disambiguate
            thread_safe_print(f"      🔀 Multiple matches found ({len(potential_matches)}), using parent hierarchy")
            
            # Extract parent hierarchy from source
            source_parts = source_hierarchy.split(' > ')
            if len(source_parts) > 1:
                source_parent_titles = [clean_title_for_matching(part) for part in source_parts[:-1]]
                
                best_match = None
                best_score = -1
                
                for match in potential_matches:
                    # Compare parent hierarchy
                    target_parent_titles = [clean_title_for_matching(h['title']) for h in match['hierarchy_path'][:-1]]
                    
                    # Calculate similarity score
                    score = 0
                    min_len = min(len(source_parent_titles), len(target_parent_titles))
                    
                    for i in range(min_len):
                        if i < len(source_parent_titles) and i < len(target_parent_titles):
                            if source_parent_titles[-(i+1)] == target_parent_titles[-(i+1)]:  # Compare from end
                                score += 1
                            else:
                                break
                    
                    thread_safe_print(f"        📊 Match at line {match['line_num']} score: {score}")
                    
                    if score > best_score:
                        best_score = score
                        best_match = match
                
                if best_match and best_score > 0:
                    matched_dict[str(best_match['line_num'])] = best_match['hierarchy_string']
                    thread_safe_print(f"      ✅ Best match: line {best_match['line_num']} (score: {best_score})")
                else:
                    thread_safe_print(f"      ❌ No good parent hierarchy match found")
                    failed_matches.append({
                        'line_num': source_line_num,
                        'hierarchy': source_hierarchy,
                        'reason': 'Multiple matches found but no good parent hierarchy match'
                    })
            else:
                thread_safe_print(f"      ⚠️  No parent hierarchy in source, cannot disambiguate")
                failed_matches.append({
                    'line_num': source_line_num,
                    'hierarchy': source_hierarchy,
                    'reason': 'Multiple matches found but no parent hierarchy to disambiguate'
                })
        else:
            thread_safe_print(f"      ❌ No matches found for: {source_clean_title}")
            # Try fuzzy matching for similar titles (e.g., --host vs --hosts)
            fuzzy_matched = False
            source_clean_lower = source_clean_title.lower()
            for target_header in target_headers:
                # Handle both dict and tuple formats
                if isinstance(target_header, dict):
                    target_clean = clean_title_for_matching(target_header['title'])
                elif isinstance(target_header, (list, tuple)) and len(target_header) >= 2:
                    target_clean = clean_title_for_matching(target_header[1])  # title is at index 1
                else:
                    continue  # Skip invalid entries
                target_clean_lower = target_clean.lower()
                # Check for similar titles (handle plural/singular and minor differences)
                # Case 1: One is substring of another (e.g., --host vs --hosts)
                # Case 2: Small character difference (1-2 characters)
                len_diff = abs(len(source_clean_lower) - len(target_clean_lower))
                if (len_diff <= 2 and 
                    (source_clean_lower in target_clean_lower or 
                     target_clean_lower in source_clean_lower)):
                        thread_safe_print(f"      ≈ Fuzzy match found: {source_clean_title} ≈ {target_clean}")
                        if isinstance(target_header, dict):
                            matched_dict[str(target_header['line_num'])] = target_header['hierarchy_string']
                            thread_safe_print(f"      ✅ Fuzzy match: line {target_header['line_num']}")
                        elif isinstance(target_header, (list, tuple)) and len(target_header) >= 3:
                            matched_dict[str(target_header[0])] = target_header[2]  # line_num at index 0, hierarchy at index 2
                            thread_safe_print(f"      ✅ Fuzzy match: line {target_header[0]}")
                        fuzzy_matched = True
                        break
            
            if not fuzzy_matched:
                failed_matches.append({
                    'line_num': source_line_num,
                    'hierarchy': source_hierarchy,
                    'reason': 'No matching section found in target'
                })
    
    thread_safe_print(f"   📊 Direct matching result: {len(matched_dict)} matches found")
    
    if failed_matches:
        thread_safe_print(f"   ⚠️  {len(failed_matches)} sections failed to match:")
        for failed in failed_matches:
            thread_safe_print(f"      ❌ Line {failed['line_num']}: {failed['hierarchy']} - {failed['reason']}")
    
    if skipped_sections:
        thread_safe_print(f"   ℹ️  {len(skipped_sections)} sections skipped (not system variables/config):")
        for skipped in skipped_sections:
            thread_safe_print(f"      ⏭️  Line {skipped['line_num']}: {skipped['hierarchy']} - {skipped['reason']}")
    
    return matched_dict, failed_matches, skipped_sections

def filter_non_system_sections(target_hierarchy):
    """Filter out system variable/config sections from target hierarchy for AI mapping"""
    filtered_hierarchy = {}
    system_sections_count = 0
    
    for line_num, hierarchy in target_hierarchy.items():
        # Extract the leaf title from hierarchy
        leaf_title = hierarchy.split(' > ')[-1] if ' > ' in hierarchy else hierarchy
        
        if is_system_variable_or_config(leaf_title):
            system_sections_count += 1
        else:
            filtered_hierarchy[line_num] = hierarchy
    
    thread_safe_print(f"   🔧 Filtered target hierarchy: {len(filtered_hierarchy)} non-system sections (removed {system_sections_count} system sections)")
    
    return filtered_hierarchy

def get_corresponding_sections(source_sections, target_sections, ai_client, source_language, target_language, max_tokens=20000):
    """Use AI to find corresponding sections between different languages"""
    
    # Format source sections
    source_text = "\n".join(source_sections)
    target_text = "\n".join(target_sections)
    number_of_sections = len(source_sections)
    
    prompt = f"""I am aligning the {source_language} and {target_language} documentation for TiDB. I have modified the following {number_of_sections} sections in the {source_language} file:

{source_text}

Here is the section structure of the corresponding {target_language} file. Please select the corresponding {number_of_sections} sections in {target_language} from the following list that I should modify. Do not output any other text, return the Markdown code block enclosed in three backticks.

{target_text}"""

    thread_safe_print(f"\n   📤 AI Mapping Prompt ({source_language} → {target_language}):")
    thread_safe_print(f"   " + "="*80)
    thread_safe_print(f"   {prompt}")
    thread_safe_print(f"   " + "="*80)

    # Import token estimation function from main
    try:
        from main import print_token_estimation
        print_token_estimation(prompt, f"Section mapping ({source_language} → {target_language})")
    except ImportError:
        # Fallback if import fails - use tiktoken
        try:
            import tiktoken
            enc = tiktoken.get_encoding("cl100k_base")
            tokens = enc.encode(prompt)
            actual_tokens = len(tokens)
            char_count = len(prompt)
            thread_safe_print(f"   💰 Section mapping ({source_language} → {target_language})")
            thread_safe_print(f"      📝 Input: {char_count:,} characters")
            thread_safe_print(f"      🔢 Actual tokens: {actual_tokens:,} (using tiktoken cl100k_base)")
        except Exception:
            # Final fallback to character approximation
            estimated_tokens = len(prompt) // 4
            char_count = len(prompt)
            thread_safe_print(f"   💰 Section mapping ({source_language} → {target_language})")
            thread_safe_print(f"      📝 Input: {char_count:,} characters")
            thread_safe_print(f"      🔢 Estimated tokens: ~{estimated_tokens:,} (fallback: 4 chars/token approximation)")

    try:
        ai_response = ai_client.chat_completion(
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=max_tokens
        )
        
        thread_safe_print(f"\n   📥 AI Mapping Response:")
        thread_safe_print(f"   " + "-"*80)
        thread_safe_print(f"   {ai_response}")
        thread_safe_print(f"   " + "-"*80)
        
        return ai_response
    except Exception as e:
        print(f"   ❌ AI mapping error: {e}")
        return None

def parse_ai_response(ai_response):
    """Parse AI response to extract section names"""
    sections = []
    lines = ai_response.split('\n')
    
    for line in lines:
        line = line.strip()
        # Skip markdown code block markers and empty lines
        if line and not line.startswith('```'):
            # Remove leading "## " if present and clean up
            if line.startswith('## '):
                sections.append(line)
            elif line.startswith('- '):
                # Handle cases where AI returns a list
                sections.append(line[2:].strip())
    
    return sections

def find_matching_line_numbers(ai_sections, target_hierarchy_dict):
    """Find line numbers in target hierarchy dict that match AI sections"""
    matched_dict = {}
    
    for ai_section in ai_sections:
        # Look for exact matches first
        found = False
        for line_num, hierarchy in target_hierarchy_dict.items():
            if hierarchy == ai_section:
                matched_dict[str(line_num)] = hierarchy
                found = True
                break
        
        if not found:
            # Look for partial matches (in case of slight differences)
            for line_num, hierarchy in target_hierarchy_dict.items():
                # Remove common variations and compare
                ai_clean = ai_section.replace('### ', '').replace('## ', '').strip()
                hierarchy_clean = hierarchy.replace('### ', '').replace('## ', '').strip()
                
                if ai_clean in hierarchy_clean or hierarchy_clean in ai_clean:
                    matched_dict[str(line_num)] = hierarchy
                    thread_safe_print(f"      ≈ Partial match found at line {line_num}: {hierarchy}")
                    found = True
                    break
        
        if not found:
            thread_safe_print(f"      ✗ No match found for: {ai_section}")
    
    return matched_dict

def build_hierarchy_path(lines, line_num, all_headers):
    """Build the full hierarchy path for a header at given line (from auto-sync-pr-changes.py)"""
    if line_num not in all_headers:
        return []
    
    current_header = all_headers[line_num]
    current_level = current_header['level']
    hierarchy_path = []
    
    # Find all parent headers
    for check_line in sorted(all_headers.keys()):
        if check_line >= line_num:
            break
        
        header = all_headers[check_line]
        if header['level'] < current_level:
            # This is a potential parent
            # Remove any headers at same or deeper level
            while hierarchy_path and hierarchy_path[-1]['level'] >= header['level']:
                hierarchy_path.pop()
            hierarchy_path.append(header)
    
    # Add current header
    hierarchy_path.append(current_header)
    
    return hierarchy_path

def map_insertion_points_to_target(insertion_points, target_hierarchy, target_lines, file_path, pr_url, github_client, ai_client, repo_config, max_non_system_sections=120):
    """Map source insertion points to target language locations"""
    target_insertion_points = {}
    
    thread_safe_print(f"   📍 Mapping {len(insertion_points)} insertion points to target...")
    
    for group_key, point_info in insertion_points.items():
        previous_section_hierarchy = point_info['previous_section_hierarchy']
        thread_safe_print(f"      🔍 Finding target location for: {previous_section_hierarchy}")
        
        # Extract title for system variable checking
        if ' > ' in previous_section_hierarchy:
            title = previous_section_hierarchy.split(' > ')[-1]
        else:
            title = previous_section_hierarchy
        
        # Check if this is a system variable/config that can be directly matched
        cleaned_title = clean_title_for_matching(title)
        if is_system_variable_or_config(cleaned_title):
            thread_safe_print(f"         🎯 Direct matching for system var/config: {cleaned_title}")
            
            # Direct matching for system variables
            temp_source = {point_info['previous_section_line']: previous_section_hierarchy}
            matched_dict, failed_matches, skipped_sections = find_direct_matches_for_special_files(
                temp_source, target_hierarchy, target_lines
            )
            
            if matched_dict:
                # Get the first (and should be only) matched target line
                target_line = list(matched_dict.keys())[0]
                
                # Find the end of this section
                target_line_num = int(target_line)
                insertion_after_line = find_section_end_line(target_line_num, target_hierarchy, target_lines)
                
                target_insertion_points[group_key] = {
                    'insertion_after_line': insertion_after_line,
                    'target_hierarchy': target_hierarchy.get(str(target_line_num), ''),
                    'insertion_type': point_info['insertion_type'],
                    'new_sections': point_info['new_sections']
                }
                thread_safe_print(f"         ✅ Direct match found, insertion after line {insertion_after_line}")
                continue
        
        # If not a system variable or direct matching failed, use AI
        thread_safe_print(f"         🤖 Using AI mapping for: {cleaned_title}")
        
        # Filter target hierarchy for AI (remove system sections)
        filtered_target_hierarchy = filter_non_system_sections(target_hierarchy)
        
        # Check if filtered hierarchy is too large for AI
        # Use provided max_non_system_sections parameter
        if len(filtered_target_hierarchy) > max_non_system_sections:
            thread_safe_print(f"         ❌ Target hierarchy too large for AI: {len(filtered_target_hierarchy)} > {max_non_system_sections}")
            continue
        
        # Prepare source for AI mapping
        temp_source = {str(point_info['previous_section_line']): previous_section_hierarchy}
        
        # Get AI mapping
        ai_response = get_corresponding_sections(
            list(temp_source.values()), 
            list(filtered_target_hierarchy.values()), 
            ai_client, 
            repo_config['source_language'], 
            repo_config['target_language'],
            max_tokens=20000  # Use default value since this function doesn't accept max_tokens yet
        )
        
        if ai_response:
            # Parse AI response and find matching line numbers
            ai_sections = parse_ai_response(ai_response)
            ai_matched = find_matching_line_numbers(ai_sections, target_hierarchy)
            
            if ai_matched and len(ai_matched) > 0:
                # Get the first match (we only have one source section)
                target_line = list(ai_matched.keys())[0]
                target_line_num = int(target_line)
                
                # Find the end of this section
                insertion_after_line = find_section_end_line(target_line_num, target_hierarchy, target_lines)
                
                target_insertion_points[group_key] = {
                    'insertion_after_line': insertion_after_line,
                    'target_hierarchy': target_hierarchy.get(target_line, ''),
                    'insertion_type': point_info['insertion_type'],
                    'new_sections': point_info['new_sections']
                }
                thread_safe_print(f"         ✅ AI match found, insertion after line {insertion_after_line}")
            else:
                thread_safe_print(f"         ❌ No AI matching sections found for: {previous_section_hierarchy}")
        else:
            thread_safe_print(f"         ❌ No AI response received for: {previous_section_hierarchy}")
    
    return target_insertion_points

def extract_hierarchies_from_diff_dict(source_diff_dict):
    """Extract original_hierarchy from source_diff_dict for section matching"""
    extracted_hierarchies = {}
    
    for key, diff_info in source_diff_dict.items():
        operation = diff_info.get('operation', '')
        original_hierarchy = diff_info.get('original_hierarchy', '')
        
        # Process all sections: modified, deleted, and added
        if operation in ['modified', 'deleted', 'added'] and original_hierarchy:
            # Use the key as the identifier for the hierarchy
            extracted_hierarchies[key] = original_hierarchy
    
    thread_safe_print(f"📄 Extracted {len(extracted_hierarchies)} hierarchies from source diff dict:")
    for key, hierarchy in extracted_hierarchies.items():
        thread_safe_print(f"   {key}: {hierarchy}")
    
    return extracted_hierarchies

def match_source_diff_to_target(source_diff_dict, target_hierarchy, target_lines, ai_client, repo_config, max_non_system_sections=120, max_tokens=20000):
    """
    Match source_diff_dict original_hierarchy to target file sections
    Uses direct matching for system variables/config and AI matching for others
    
    Returns:
        dict: Matched sections with enhanced information including:
            - target_line: Line number in target file
            - target_hierarchy: Target section hierarchy 
            - insertion_type: For added sections only
            - source_original_hierarchy: Original hierarchy from source
            - source_operation: Operation type (modified/added/deleted)
            - source_old_content: Old content from source diff
            - source_new_content: New content from source diff
    """
    thread_safe_print(f"🔗 Starting source diff to target matching...")
    
    # Extract hierarchies from source diff dict
    source_hierarchies = extract_hierarchies_from_diff_dict(source_diff_dict)
    
    if not source_hierarchies:
        thread_safe_print(f"⚠️  No hierarchies to match")
        return {}
    
    # Process sections in original order to maintain consistency
    # Initialize final matching results with ordered dict to preserve order
    from collections import OrderedDict
    all_matched_sections = OrderedDict()
    
    # Categorize sections for processing strategy but maintain order
    direct_match_sections = OrderedDict()
    ai_match_sections = OrderedDict()
    added_sections = OrderedDict()
    bottom_sections = OrderedDict()  # New category for bottom sections
    
    for key, hierarchy in source_hierarchies.items():
        # Check if this is a bottom section (no matching needed)
        if hierarchy.startswith('bottom-'):
            bottom_sections[key] = hierarchy
        # Check if this is an added section
        elif key.startswith('added_'):
            added_sections[key] = hierarchy
        else:
            # Extract the leaf title from hierarchy for checking
            leaf_title = hierarchy.split(' > ')[-1] if ' > ' in hierarchy else hierarchy
            
            # Check if this is suitable for direct matching
            if (hierarchy == "frontmatter" or 
                leaf_title.startswith('# ') or  # Top-level titles
                is_system_variable_or_config(leaf_title)):  # System variables/config
                direct_match_sections[key] = hierarchy
            else:
                ai_match_sections[key] = hierarchy
    
    thread_safe_print(f"📊 Section categorization:")
    thread_safe_print(f"   🎯 Direct matching: {len(direct_match_sections)} sections")
    thread_safe_print(f"   🤖 AI matching: {len(ai_match_sections)} sections")
    thread_safe_print(f"   ➕ Added sections: {len(added_sections)} sections")
    thread_safe_print(f"   🔚 Bottom sections: {len(bottom_sections)} sections (no matching needed)")
    
    # Process each section in original order
    thread_safe_print(f"\n🔄 Processing sections in original order...")
    
    for key, hierarchy in source_hierarchies.items():
        thread_safe_print(f"   🔍 Processing {key}: {hierarchy}")
        
        # Determine processing strategy based on section type and content
        if hierarchy.startswith('bottom-'):
            # Bottom section - no matching needed, append to end
            thread_safe_print(f"      🔚 Bottom section - append to end of document")
            result = {
                "target_line": "-1",  # Special marker for bottom sections
                "target_hierarchy": hierarchy  # Keep the bottom-xxx marker
            }
        elif key.startswith('added_'):
            # Added section - find insertion point
            thread_safe_print(f"      ➕ Added section - finding insertion point")
            result = process_added_section(key, hierarchy, target_hierarchy, target_lines, ai_client, repo_config, max_non_system_sections, max_tokens)
        else:
            # Modified or deleted section - find matching section
            operation = source_diff_dict[key].get('operation', 'unknown')
            thread_safe_print(f"      {operation.capitalize()} section - finding target match")
            result = process_modified_or_deleted_section(key, hierarchy, target_hierarchy, target_lines, ai_client, repo_config, max_non_system_sections, max_tokens)
        
        if result:
            # Add source language information from source_diff_dict
            source_info = source_diff_dict.get(key, {})
            
            # Extract target content from target_lines
            target_line = result.get('target_line', 'unknown')
            target_content = ""
            if target_line != 'unknown' and target_line != '0':
                try:
                    target_line_num = int(target_line)
                    # For ALL operations, only extract direct content (no sub-sections)
                    # This avoids duplication when both parent and child sections have operations
                    target_content = extract_section_direct_content(target_line_num, target_lines)
                except (ValueError, IndexError):
                    target_content = ""
            elif target_line == '0':
                # For frontmatter, extract content from beginning to first header
                target_content = extract_frontmatter_content(target_lines)
            
            enhanced_result = {
                **result,  # Include existing target matching info
                'target_content': target_content,  # Add target section content
                'source_original_hierarchy': source_info.get('original_hierarchy', ''),
                'source_operation': source_info.get('operation', ''),
                'source_old_content': source_info.get('old_content', ''),
                'source_new_content': source_info.get('new_content', '')
            }
            all_matched_sections[key] = enhanced_result
            thread_safe_print(f"      ✅ {key}: -> line {target_line}")
        else:
            thread_safe_print(f"      ❌ {key}: matching failed")
    
    thread_safe_print(f"\n📊 Final matching results: {len(all_matched_sections)} total matches")
    return all_matched_sections

def process_modified_or_deleted_section(key, hierarchy, target_hierarchy, target_lines, ai_client, repo_config, max_non_system_sections, max_tokens=20000):
    """Process modified or deleted sections to find target matches"""
    # Extract the leaf title from hierarchy for checking
    leaf_title = hierarchy.split(' > ')[-1] if ' > ' in hierarchy else hierarchy
    
    # Check if this is suitable for direct matching
    if (hierarchy == "frontmatter" or 
        leaf_title.startswith('# ') or  # Top-level titles
        is_system_variable_or_config(leaf_title)):  # System variables/config
        
        if hierarchy == "frontmatter":
            return {"target_line": "0", "target_hierarchy": "frontmatter"}
            
        elif leaf_title.startswith('# '):
            # Top-level title matching
            temp_sections = {key: hierarchy}
            matched_dict, failed_matches, skipped_sections = find_toplevel_title_matches(
                temp_sections, target_lines
            )
            if matched_dict:
                target_line = list(matched_dict.keys())[0]
                # For top-level titles, add # prefix to the hierarchy
                return {
                    "target_line": target_line, 
                    "target_hierarchy": f"# {matched_dict[target_line]}"
                }
                
        else:
            # System variable/config matching
            temp_sections = {key: hierarchy}
            matched_dict, failed_matches, skipped_sections = find_direct_matches_for_special_files(
                temp_sections, target_hierarchy, target_lines
            )
            if matched_dict:
                target_line = list(matched_dict.keys())[0]
                target_hierarchy_str = list(matched_dict.values())[0]
                
                # Extract the leaf title and add # prefix, remove top-level title from hierarchy
                if ' > ' in target_hierarchy_str:
                    # Remove top-level title and keep only the leaf with ## prefix
                    leaf_title = target_hierarchy_str.split(' > ')[-1]
                    formatted_hierarchy = f"## {leaf_title}"
                else:
                    # Single level, add ## prefix
                    formatted_hierarchy = f"## {target_hierarchy_str}"
                
                return {
                    "target_line": target_line,
                    "target_hierarchy": formatted_hierarchy
                }
    else:
        # AI matching for non-system sections
        filtered_target_hierarchy = filter_non_system_sections(target_hierarchy)
        
        if len(filtered_target_hierarchy) <= max_non_system_sections:
            temp_sections = {key: hierarchy}
            
            ai_response = get_corresponding_sections(
                list(temp_sections.values()),
                list(filtered_target_hierarchy.values()),
                ai_client,
                repo_config['source_language'],
                repo_config['target_language'],
                max_tokens
            )
            
            if ai_response:
                ai_sections = parse_ai_response(ai_response)
                ai_matched = find_matching_line_numbers(ai_sections, target_hierarchy)
                
                if ai_matched:
                    target_line = list(ai_matched.keys())[0]
                    target_hierarchy_str = list(ai_matched.values())[0]
                    
                    # Format AI matched hierarchy with # prefix and remove top-level title
                    formatted_hierarchy = format_target_hierarchy(target_hierarchy_str)
                    
                    return {
                        "target_line": target_line,
                        "target_hierarchy": formatted_hierarchy
                    }
    
    return None

def format_target_hierarchy(target_hierarchy_str):
    """Format target hierarchy to preserve complete hierarchy structure"""
    if target_hierarchy_str.startswith('##') or target_hierarchy_str.startswith('#'):
        # Already formatted, return as is
        return target_hierarchy_str
    elif ' > ' in target_hierarchy_str:
        # Keep complete hierarchy structure, just ensure proper formatting
        return target_hierarchy_str
    else:
        # Single level, add ## prefix for compatibility
        return f"## {target_hierarchy_str}"

def process_added_section(key, reference_hierarchy, target_hierarchy, target_lines, ai_client, repo_config, max_non_system_sections, max_tokens=20000):
    """Process added sections to find insertion points"""
    # For added sections, hierarchy points to the next section (where to insert before)
    reference_leaf = reference_hierarchy.split(' > ')[-1] if ' > ' in reference_hierarchy else reference_hierarchy
    
    if (reference_hierarchy == "frontmatter" or 
        reference_leaf.startswith('# ') or 
        is_system_variable_or_config(reference_leaf)):
        
        # Use direct matching for the reference section
        temp_reference = {f"ref_{key}": reference_hierarchy}
        
        if reference_hierarchy == "frontmatter":
            return {
                "target_line": "0",
                "target_hierarchy": "frontmatter",
                "insertion_type": "before_reference"
            }
            
        elif reference_leaf.startswith('# '):
            matched_dict, failed_matches, skipped_sections = find_toplevel_title_matches(
                temp_reference, target_lines
            )
            if matched_dict:
                target_line = list(matched_dict.keys())[0]
                formatted_hierarchy = f"# {matched_dict[target_line]}"
                return {
                    "target_line": target_line,
                    "target_hierarchy": formatted_hierarchy,
                    "insertion_type": "before_reference"
                }
                
        else:
            # System variable/config
            matched_dict, failed_matches, skipped_sections = find_direct_matches_for_special_files(
                temp_reference, target_hierarchy, target_lines
            )
            if matched_dict:
                target_line = list(matched_dict.keys())[0]
                target_hierarchy_str = list(matched_dict.values())[0]
                formatted_hierarchy = format_target_hierarchy(target_hierarchy_str)
                return {
                    "target_line": target_line,
                    "target_hierarchy": formatted_hierarchy,
                    "insertion_type": "before_reference"
                }
    else:
        # Use AI matching for the reference section
        filtered_target_hierarchy = filter_non_system_sections(target_hierarchy)
        
        if len(filtered_target_hierarchy) <= max_non_system_sections:
            temp_reference = {f"ref_{key}": reference_hierarchy}
            
            ai_response = get_corresponding_sections(
                list(temp_reference.values()),
                list(filtered_target_hierarchy.values()),
                ai_client,
                repo_config['source_language'],
                repo_config['target_language'],
                max_tokens
            )
            
            if ai_response:
                ai_sections = parse_ai_response(ai_response)
                ai_matched = find_matching_line_numbers(ai_sections, target_hierarchy)
                
                if ai_matched:
                    target_line = list(ai_matched.keys())[0]
                    target_hierarchy_str = list(ai_matched.values())[0]
                    formatted_hierarchy = format_target_hierarchy(target_hierarchy_str)
                    return {
                        "target_line": target_line,
                        "target_hierarchy": formatted_hierarchy,
                        "insertion_type": "before_reference"
                    }
    
    return None

def extract_target_section_content(target_line_num, target_lines):
    """Extract target section content from target_lines (includes sub-sections)"""
    if target_line_num >= len(target_lines):
        return ""
    
    start_line = target_line_num - 1  # Convert to 0-based index
    
    # Find the end of the section by looking for the next header
    current_line = target_lines[start_line].strip()
    if not current_line.startswith('#'):
        return current_line
    
    current_level = len(current_line.split()[0])  # Count # characters
    end_line = len(target_lines)  # Default to end of file
    
    # For top-level headers (# level 1), stop at first sublevel (## level 2)
    # For other headers, stop at same or higher level
    if current_level == 1:
        # Top-level header: stop at first ## (level 2) or higher
        for i in range(start_line + 1, len(target_lines)):
            line = target_lines[i].strip()
            if line.startswith('#'):
                line_level = len(line.split()[0])
                if line_level >= 2:  # Stop at ## or higher level
                    end_line = i
                    break
    else:
        # Sub-level header: stop at same or higher level (traditional behavior)
        for i in range(start_line + 1, len(target_lines)):
            line = target_lines[i].strip()
            if line.startswith('#'):
                line_level = len(line.split()[0])
                if line_level <= current_level:
                    end_line = i
                    break
    
    # Extract content from start_line to end_line
    section_content = '\n'.join(target_lines[start_line:end_line])
    return section_content.strip()

def extract_section_direct_content(target_line_num, target_lines):
    """Extract ONLY the direct content of a section (excluding sub-sections)"""
    if target_line_num >= len(target_lines):
        return ""
    
    start_line = target_line_num - 1  # Convert to 0-based index
    
    # Find the end of the section by looking for the next header
    current_line = target_lines[start_line].strip()
    if not current_line.startswith('#'):
        return current_line
    
    current_level = len(current_line.split()[0])  # Count # characters
    end_line = len(target_lines)  # Default to end of file
    
    # Only extract until the first header (any level)
    # This means we stop at ANY header - whether it's a sub-section OR same/higher level
    for i in range(start_line + 1, len(target_lines)):
        line = target_lines[i].strip()
        if line.startswith('#'):
            # Stop at ANY header to get only direct content
            end_line = i
            break
    
    # Extract content from start_line to end_line
    section_content = '\n'.join(target_lines[start_line:end_line])
    return section_content.strip()

def extract_frontmatter_content(target_lines):
    """Extract frontmatter content from beginning to first header"""
    if not target_lines:
        return ""
    
    frontmatter_lines = []
    for i, line in enumerate(target_lines):
        line_stripped = line.strip()
        # Stop when we hit the first top-level header
        if line_stripped.startswith('# '):
            break
        frontmatter_lines.append(line.rstrip())
    
    return '\n'.join(frontmatter_lines)

def find_section_end_line(section_start_line, target_hierarchy, target_lines):
    """Find the end line of a section to determine insertion point (from auto-sync-pr-changes.py)"""
    
    # Get the current section's level
    current_section_line = target_lines[section_start_line - 1].strip()
    current_level = len(current_section_line.split()[0]) if current_section_line.startswith('#') else 5
    
    # Find the next section at the same level or higher (lower number)
    next_section_line = None
    for line_num_str in sorted(target_hierarchy.keys(), key=int):
        line_num = int(line_num_str)
        if line_num > section_start_line:
            # Check the level of this section
            section_line = target_lines[line_num - 1].strip()
            if section_line.startswith('#'):
                section_level = len(section_line.split()[0])
                if section_level <= current_level:
                    next_section_line = line_num
                    break
    
    if next_section_line:
        # Insert before the next same-level or higher-level section
        return next_section_line - 1
    else:
        # This is the last section at this level, insert at the end of the file
        return len(target_lines)
