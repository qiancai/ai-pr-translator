"""
File Updater Module
Handles processing and translation of updated files and sections
"""

import os
import re
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from github import Github
from openai import OpenAI

# Thread-safe printing
print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def get_updated_sections_from_ai(pr_diff, target_sections, source_old_content_dict, ai_client, source_language, target_language, target_file_name=None):
    """Use AI to update target sections based on source old content, PR diff, and target sections"""
    if not source_old_content_dict or not target_sections:
        return {}
    
    # Filter out deleted sections and prepare source sections from old content
    source_sections = {}
    for key, old_content in source_old_content_dict.items():
        # Skip deleted sections
        if 'deleted' in key:
            continue
        
        # Handle null values by using empty string
        content = old_content if old_content is not None else ""
        source_sections[key] = content

    # Keep the original order from match_source_diff_to_target.json (no sorting needed)
    formatted_source_sections = json.dumps(source_sections, ensure_ascii=False, indent=2)
    formatted_target_sections = json.dumps(target_sections, ensure_ascii=False, indent=2)
    
    thread_safe_print(f"   📊 Source sections: {len(source_sections)} sections")
    thread_safe_print(f"   📊 Target sections: {len(target_sections)} sections")
    
    # Calculate total content size
    total_source_chars = sum(len(str(content)) for content in source_sections.values())
    total_target_chars = sum(len(str(content)) for content in target_sections.values())
    thread_safe_print(f"   📏 Content size: Source={total_source_chars:,} chars, Target={total_target_chars:,} chars")

    thread_safe_print(f"   🤖 Getting AI translation for {len(source_sections)} sections...")

    diff_content = source_sections
    
    prompt = f"""You are a professional technical writer in the Database domain. I will provide you with:

1. Source sections in {source_language}:
{formatted_source_sections}

2. GitHub PR changes (Diff):
{pr_diff}

3. Current target sections in {target_language}:
{formatted_target_sections}

Task: Update the target sections in {target_language} according to the diff in {source_language}.

Instructions:
1. Carefully analyze the PR diff to understand what changes were made (additions, deletions, modifications)
2. Find the corresponding positions in the {target_language} sections and make the same changes. Do not change any content that is not modified in the diff, especially the format.
3. Keep the JSON structure unchanged, only modify the section content
4. Ensure the updated {target_language} content is logically consistent with the {source_language} changes
5. Maintain proper technical writing style and terminology in {target_language}. If a sentence in the diff is unchanged in content but only reordered in {source_language}, reuse its existing translation in {target_language}.

Please return the complete updated JSON in the same format as target sections, without any additional explanatory text."""

    # Save prompt to file for reference with target file prefix
    target_file_prefix = "unknown"
    if target_file_name:
        # Use provided target file name
        target_file_prefix = target_file_name.replace('/', '_').replace('.md', '')
    elif target_sections:
        # Try to extract filename from the first section key or content
        first_key = next(iter(target_sections.keys()), "")
        if "_" in first_key:
            # If key contains underscore, it might have target file info
            parts = first_key.split("_")
            if len(parts) > 1:
                target_file_prefix = parts[0]
    
    # Ensure temp_output directory exists
    script_dir = os.path.dirname(os.path.abspath(__file__))
    temp_dir = os.path.join(script_dir, "temp_output")
    os.makedirs(temp_dir, exist_ok=True)
    
    prompt_file = os.path.join(temp_dir, f"{target_file_prefix}_prompt-for-ai-translation.txt")
    with open(prompt_file, 'w', encoding='utf-8') as f:
        f.write(prompt)
    
    thread_safe_print(f"\n💾 Prompt saved to {prompt_file}")
    thread_safe_print(f"📝 Prompt length: {len(prompt)} characters")
    thread_safe_print(f"📊 Source sections: {len(source_sections)}")
    thread_safe_print(f"📊 Target sections: {len(target_sections)}")
    thread_safe_print(f"🤖 Sending prompt to AI...")

    thread_safe_print(f"\n   📤 AI Update Prompt ({source_language} → {target_language}):")
    thread_safe_print(f"   " + "="*80)
    thread_safe_print(f"   Source Sections: {formatted_source_sections[:500]}...")
    thread_safe_print(f"   PR Diff (first 500 chars): {pr_diff[:500]}...")
    thread_safe_print(f"   Target Sections: {formatted_target_sections[:500]}...")
    thread_safe_print(f"   " + "="*80)

    try:
        from main import print_token_estimation
        print_token_estimation(prompt, f"Document translation ({source_language} → {target_language})")
    except ImportError:
        # Fallback if import fails - use tiktoken
        try:
            import tiktoken
            enc = tiktoken.get_encoding("cl100k_base")
            tokens = enc.encode(prompt)
            actual_tokens = len(tokens)
            char_count = len(prompt)
            thread_safe_print(f"   💰 Document translation ({source_language} → {target_language})")
            thread_safe_print(f"      📝 Input: {char_count:,} characters")
            thread_safe_print(f"      🔢 Actual tokens: {actual_tokens:,} (using tiktoken cl100k_base)")
        except Exception:
            # Final fallback to character approximation
            estimated_tokens = len(prompt) // 4
            char_count = len(prompt)
            thread_safe_print(f"   💰 Document translation ({source_language} → {target_language})")
            thread_safe_print(f"      📝 Input: {char_count:,} characters")
            thread_safe_print(f"      🔢 Estimated tokens: ~{estimated_tokens:,} (fallback: 4 chars/token approximation)")
    
    try:
        ai_response = ai_client.chat_completion(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        thread_safe_print(f"   📝 AI translation response received")
        thread_safe_print(f"   📋 AI response (first 500 chars): {ai_response[:500]}...")
        
        result = parse_updated_sections(ai_response)
        thread_safe_print(f"   📊 Parsed {len(result)} sections from AI response")
        
        # Save AI results to file with target file prefix
        ai_results_file = os.path.join(temp_dir, f"{target_file_prefix}_updated_sections_from_ai.json")
        with open(ai_results_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        thread_safe_print(f"   💾 AI results saved to {ai_results_file}")
        return result
        
    except Exception as e:
        thread_safe_print(f"   ❌ AI translation failed: {e}")
        return {}

def parse_updated_sections(ai_response):
    """Parse AI response and extract JSON (from get-updated-target-sections.py)"""
    # Ensure temp_output directory exists for debug files
    script_dir = os.path.dirname(os.path.abspath(__file__))
    temp_dir = os.path.join(script_dir, "temp_output")
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        print(f"\n   🔧 Parsing AI response...")
        print(f"   Raw response length: {len(ai_response)} characters")
        
        # Try to extract JSON from AI response
        cleaned_response = ai_response.strip()
        
        # Remove markdown code blocks if present
        if cleaned_response.startswith('```json'):
            cleaned_response = cleaned_response[7:]
            print(f"   📝 Removed '```json' prefix")
        elif cleaned_response.startswith('```'):
            cleaned_response = cleaned_response[3:]
            print(f"   📝 Removed '```' prefix")
        
        if cleaned_response.endswith('```'):
            cleaned_response = cleaned_response[:-3]
            print(f"   📝 Removed '```' suffix")
        
        cleaned_response = cleaned_response.strip()
        
        print(f"   📝 Cleaned response length: {len(cleaned_response)} characters")
        print(f"   📝 First 200 chars: {cleaned_response[:200]}...")
        print(f"   📝 Last 200 chars: ...{cleaned_response[-200:]}")
        
        # Try to find JSON content between curly braces
        start_idx = cleaned_response.find('{')
        end_idx = cleaned_response.rfind('}')
        
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            json_content = cleaned_response[start_idx:end_idx+1]
            print(f"   📝 Extracted JSON content length: {len(json_content)} characters")
            
            try:
                # Parse JSON
                updated_sections = json.loads(json_content)
                print(f"   ✅ Successfully parsed JSON with {len(updated_sections)} sections")
                return updated_sections
            except json.JSONDecodeError as e:
                print(f"   ⚠️  JSON seems incomplete, trying to fix...")
                
                # Try to fix incomplete JSON by finding the last complete entry
                lines = json_content.split('\n')
                fixed_lines = []
                in_value = False
                quote_count = 0
                
                for line in lines:
                    if '"' in line:
                        quote_count += line.count('"')
                    
                    fixed_lines.append(line)
                    
                    # If we have an even number of quotes, we might have a complete entry
                    if quote_count % 2 == 0 and (line.strip().endswith(',') or line.strip().endswith('"')):
                        # Try to parse up to this point
                        potential_json = '\n'.join(fixed_lines)
                        if not potential_json.rstrip().endswith('}'):
                            # Remove trailing comma and add closing brace
                            if potential_json.rstrip().endswith(','):
                                potential_json = potential_json.rstrip()[:-1] + '\n}'
                            else:
                                potential_json += '\n}'
                        
                        try:
                            partial_sections = json.loads(potential_json)
                            print(f"   🔧 Fixed JSON with {len(partial_sections)} sections")
                            return partial_sections
                        except:
                            continue
                
                # If all else fails, return the original error
                raise e
        else:
            print(f"   ❌ Could not find valid JSON structure in response")
            return None
        
    except json.JSONDecodeError as e:
        print(f"   ❌ Error parsing AI response as JSON: {e}")
        print(f"   📝 Error at position: {e.pos if hasattr(e, 'pos') else 'unknown'}")
        
        # Save debug info
        debug_file = os.path.join(temp_dir, f"ai_response_debug_{os.getpid()}.txt")
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write("Original AI Response:\n")
            f.write("="*80 + "\n")
            f.write(ai_response)
            f.write("\n" + "="*80 + "\n")
            f.write("Cleaned Response:\n")
            f.write("-"*80 + "\n")
            f.write(cleaned_response if 'cleaned_response' in locals() else "Not available")
        
        print(f"   📁 Debug info saved to: {debug_file}")
        return None
    except Exception as e:
        print(f"   ❌ Unexpected error parsing AI response: {e}")
        return None


def replace_frontmatter_content(lines, new_content):
    """Replace content from beginning of file to first top-level header"""
    # Find the first top-level header
    first_header_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith('# '):
            first_header_idx = i
            break
    
    if first_header_idx is None:
        # No top-level header found, replace entire content
        return new_content.split('\n')
    
    # Replace content from start to before first header
    new_lines = new_content.split('\n')
    return new_lines + lines[first_header_idx:]


def replace_toplevel_section_content(lines, target_line_num, new_content):
    """Replace content from top-level header to first next-level header"""
    start_idx = target_line_num - 1  # Convert to 0-based index
    
    # Find the end of top-level section (before first ## header)
    end_idx = len(lines)
    for i in range(start_idx + 1, len(lines)):
        line = lines[i].strip()
        if line.startswith('##'):  # Found first next-level header
            end_idx = i
            break
    
    # Replace the top-level section content (from start_idx to end_idx)
    new_lines = new_content.split('\n')
    return lines[:start_idx] + new_lines + lines[end_idx:]


def update_local_document(file_path, updated_sections, hierarchy_dict, target_local_path):
    """Update local document using hierarchy-based section identification (from update-target-doc-v2.py)"""
    local_path = os.path.join(target_local_path, file_path)
    
    if not os.path.exists(local_path):
        print(f"   ❌ Local file not found: {local_path}")
        return False
    
    try:
        # Read document content
        with open(local_path, 'r', encoding='utf-8') as f:
            document_content = f.read()
        
        lines = document_content.split('\n')
        
        replacements_made = []
        
        # Use a unified approach: build a complete replacement plan first, then execute it
        # This avoids line number shifts during the replacement process
        
        # Find section boundaries for ALL sections
        section_boundaries = find_section_boundaries(lines, hierarchy_dict)
        
        # Create a comprehensive replacement plan
        replacement_plan = []
        
        for line_num, new_content in updated_sections.items():
            if line_num == "0":
                # Special handling for frontmatter
                first_header_idx = None
                for i, line in enumerate(lines):
                    if line.strip().startswith('# '):
                        first_header_idx = i
                        break
                
                replacement_plan.append({
                    'type': 'frontmatter',
                    'start': 0,
                    'end': first_header_idx if first_header_idx else len(lines),
                    'new_content': new_content,
                    'line_num': line_num
                })
                
            elif line_num in hierarchy_dict:
                hierarchy = hierarchy_dict[line_num]
                if ' > ' not in hierarchy:  # Top-level section
                    # Special handling for top-level sections
                    start_idx = int(line_num) - 1
                    end_idx = len(lines)
                    for i in range(start_idx + 1, len(lines)):
                        line = lines[i].strip()
                        if line.startswith('##'):
                            end_idx = i
                            break
                    
                    replacement_plan.append({
                        'type': 'toplevel',
                        'start': start_idx,
                        'end': end_idx,
                        'new_content': new_content,
                        'line_num': line_num
                    })
                else:
                    # Regular section
                    if line_num in section_boundaries:
                        boundary = section_boundaries[line_num]
                        replacement_plan.append({
                            'type': 'regular',
                            'start': boundary['start'],
                            'end': boundary['end'],
                            'new_content': new_content,
                            'line_num': line_num,
                            'hierarchy': boundary['hierarchy']
                        })
                    else:
                        print(f"      ⚠️  Section at line {line_num} not found in hierarchy")
        
        # Sort replacement plan: process from bottom to top of the document to avoid line shifts
        # Sort by start line in reverse order (highest line number first)
        replacement_plan.sort(key=lambda x: -x['start'])
        
        # Execute replacements in the planned order (from bottom to top)
        print(f"      📋 Executing {len(replacement_plan)} replacements from bottom to top:")
        for i, replacement in enumerate(replacement_plan):
            print(f"      {i+1}. {replacement['type']} (line {replacement.get('line_num', '0')}, start: {replacement['start']})")
        
        for replacement in replacement_plan:
            start = replacement['start']
            end = replacement['end']
            new_content = replacement['new_content']
            new_lines = new_content.split('\n')
            
            # Replace the content
            lines = lines[:start] + new_lines + lines[end:]
            
            # Record the replacement
            original_line_count = end - start
            line_diff = len(new_lines) - original_line_count
            
            replacements_made.append({
                'type': replacement['type'],
                'line_num': replacement.get('line_num', 'N/A'),
                'hierarchy': replacement.get('hierarchy', 'N/A'),
                'start': start,
                'end': end,
                'original_lines': original_line_count,
                'new_lines': len(new_lines),
                'line_diff': line_diff
            })
            
            print(f"      ✅ Updated {replacement['type']} section: {replacement.get('line_num', 'frontmatter')}")
        
        # Save updated document
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        
        print(f"   ✅ Updated {len(replacements_made)} sections")
        for replacement in replacements_made:
            print(f"      📝 Line {replacement['line_num']}: {replacement['hierarchy']}")
        
        return True
        
    except Exception as e:
        thread_safe_print(f"   ❌ Error updating file: {e}")
        return False

def find_section_boundaries(lines, hierarchy_dict):
    """Find the start and end line for each section based on hierarchy (from update-target-doc-v2.py)"""
    section_boundaries = {}
    
    # Sort sections by line number
    sorted_sections = sorted(hierarchy_dict.items(), key=lambda x: int(x[0]))
    
    for i, (line_num, hierarchy) in enumerate(sorted_sections):
        start_line = int(line_num) - 1  # Convert to 0-based index
        
        # Find end line (start of next section at same or higher level)
        end_line = len(lines)  # Default to end of document
        
        if start_line >= len(lines):
            continue
            
        # Get current section level
        current_line = lines[start_line].strip()
        if not current_line.startswith('#'):
            continue
            
        current_level = len(current_line.split()[0])  # Count # characters
        
        # Look for next section at same or higher level
        for j in range(start_line + 1, len(lines)):
            line = lines[j].strip()
            if line.startswith('#'):
                line_level = len(line.split()[0]) if line.split() else 0
                if line_level <= current_level:
                    end_line = j
                    break
        
        section_boundaries[line_num] = {
            'start': start_line,
            'end': end_line,
            'hierarchy': hierarchy,
            'level': current_level
        }
    
    return section_boundaries

def insert_sections_into_document(file_path, translated_sections, target_insertion_points, target_local_path):
    """Insert translated sections into the target document at specified points"""
    
    if not translated_sections or not target_insertion_points:
        thread_safe_print(f"   ⚠️  No sections or insertion points provided")
        return False
    
    local_path = os.path.join(target_local_path, file_path)
    
    if not os.path.exists(local_path):
        thread_safe_print(f"   ❌ Local file not found: {local_path}")
        return False
    
    try:
        # Read document content
        with open(local_path, 'r', encoding='utf-8') as f:
            document_content = f.read()
        
        lines = document_content.split('\n')
        thread_safe_print(f"   📄 Document has {len(lines)} lines")
        
        # Sort insertion points by line number in descending order to avoid position shifts
        sorted_insertions = sorted(
            target_insertion_points.items(), 
            key=lambda x: x[1]['insertion_after_line'], 
            reverse=True
        )
        
        insertions_made = []
        
        for group_id, point_data in sorted_insertions:
            insertion_after_line = point_data['insertion_after_line']
            new_sections = point_data['new_sections']
            insertion_type = point_data['insertion_type']
            
            thread_safe_print(f"     📌 Inserting {len(new_sections)} sections after line {insertion_after_line}")
            
            # Convert 1-based line number to 0-based index for insertion point
            # insertion_after_line is 1-based, so insertion_index should be insertion_after_line - 1
            insertion_index = insertion_after_line - 1
            
            # Prepare new content to insert
            new_content_lines = []
            
            # Add an empty line before the new sections if not already present
            if insertion_index < len(lines) and lines[insertion_index].strip():
                new_content_lines.append("")
            
            # Add each translated section
            for section_line_num in new_sections:
                # Find the corresponding translated content
                section_hierarchy = None
                section_content = None
                
                # Search for the section in translated_sections by line number or hierarchy
                for hierarchy, content in translated_sections.items():
                    # Try to match by hierarchy or find the content
                    if str(section_line_num) in hierarchy or content:  # This is a simplified matching
                        section_hierarchy = hierarchy
                        section_content = content
                        break
                
                if section_content:
                    # Split content into lines and add to insertion
                    content_lines = section_content.split('\n')
                    new_content_lines.extend(content_lines)
                    
                    # Add spacing between sections
                    if section_line_num != new_sections[-1]:  # Not the last section
                        new_content_lines.append("")
                    
                    thread_safe_print(f"       ✅ Added section: {section_hierarchy}")
                else:
                    thread_safe_print(f"       ⚠️  Could not find translated content for section at line {section_line_num}")
            
            # Add an empty line after the new sections if not already present
            # Check if the new content already ends with an empty line
            if new_content_lines and not new_content_lines[-1].strip():
                # Content already ends with empty line, don't add another
                pass
            elif insertion_index + 1 < len(lines) and lines[insertion_index + 1].strip():
                # Next line has content and our content doesn't end with empty line, add one
                new_content_lines.append("")
            
            # Insert the new content (insert after insertion_index line, before the next line)
            # If insertion_after_line is 251, we want to insert at position 252 (0-based index 251)
            lines = lines[:insertion_index + 1] + new_content_lines + lines[insertion_index + 1:]
            
            insertions_made.append({
                'group_id': group_id,
                'insertion_after_line': insertion_after_line,
                'sections_count': len(new_sections),
                'lines_added': len(new_content_lines),
                'insertion_type': insertion_type
            })
        
        # Save updated document
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        
        thread_safe_print(f"   ✅ Successfully inserted {len(insertions_made)} section groups")
        for insertion in insertions_made:
            thread_safe_print(f"      📝 {insertion['group_id']}: {insertion['sections_count']} sections, {insertion['lines_added']} lines after line {insertion['insertion_after_line']}")
        
        return True
        
    except Exception as e:
        thread_safe_print(f"   ❌ Error inserting sections: {e}")
        return False

def process_modified_sections(modified_sections, pr_diff, pr_url, github_client, ai_client, repo_config, max_non_system_sections=120):
    """Process modified sections with full data structure support"""
    results = []
    
    for file_path, file_data in modified_sections.items():
        thread_safe_print(f"\n📄 Processing {file_path}")
        
        try:
            # Call process_single_file with the complete data structure
            success, message = process_single_file(
                file_path, 
                file_data,  # Pass the complete data structure (includes 'sections', 'original_hierarchy', etc.)
                pr_diff, 
                pr_url, 
                github_client, 
                ai_client, 
                repo_config, 
                max_non_system_sections
            )
            
            if success:
                thread_safe_print(f"   ✅ Successfully processed {file_path}")
                results.append((file_path, True, message))
            else:
                thread_safe_print(f"   ❌ Failed to process {file_path}: {message}")
                results.append((file_path, False, message))
                
        except Exception as e:
            thread_safe_print(f"   ❌ Error processing {file_path}: {e}")
            results.append((file_path, False, f"Error processing {file_path}: {e}"))
    
    return results

def process_deleted_sections(deleted_sections, pr_url, github_client, ai_client, repo_config, max_non_system_sections=120):
    """Process deleted sections with full data structure support"""
    results = []
    
    for file_path, source_sections in deleted_sections.items():
        thread_safe_print(f"\n🗑️  Processing deleted sections in {file_path}")
        
        try:
            # Call process_single_file_deletion with the complete data structure
            success, message = process_single_file_deletion(
                file_path, 
                source_sections, 
                pr_url, 
                github_client, 
                ai_client, 
                repo_config, 
                max_non_system_sections
            )
            
            if success:
                thread_safe_print(f"   ✅ Successfully processed deletions in {file_path}")
                results.append((file_path, True, message))
            else:
                thread_safe_print(f"   ❌ Failed to process deletions in {file_path}: {message}")
                results.append((file_path, False, message))
                
        except Exception as e:
            thread_safe_print(f"   ❌ Error processing deletions in {file_path}: {e}")
            results.append((file_path, False, f"Error processing deletions in {file_path}: {e}"))
    
    return results

def process_single_file_deletion(file_path, source_sections, pr_url, github_client, ai_client, repo_config, max_non_system_sections=120):
    """Process deletion of sections in a single file"""
    
    # Import needed functions
    from pr_analyzer import get_target_hierarchy_and_content
    from section_matcher import (
        find_direct_matches_for_special_files, 
        filter_non_system_sections, 
        get_corresponding_sections,
        is_system_variable_or_config,
        clean_title_for_matching,
        parse_ai_response,
        find_matching_line_numbers
    )
    
    # Get target file hierarchy and content
    target_hierarchy, target_lines = get_target_hierarchy_and_content(
        file_path, github_client, repo_config['target_repo']
    )
    
    if not target_hierarchy:
        return False, f"Could not get target hierarchy for {file_path}"
    
    # Separate system variables from regular sections for hybrid mapping
    system_sections = {}
    regular_sections = {}
    
    for line_num, hierarchy in source_sections.items():
        # Extract title for checking
        if ' > ' in hierarchy:
            title = hierarchy.split(' > ')[-1]
        else:
            title = hierarchy
        
        cleaned_title = clean_title_for_matching(title)
        if is_system_variable_or_config(cleaned_title):
            system_sections[line_num] = hierarchy
        else:
            regular_sections[line_num] = hierarchy
    
    sections_to_delete = []
    
    # Process system variables with direct matching
    if system_sections:
        thread_safe_print(f"   🎯 Direct matching for {len(system_sections)} system sections...")
        matched_dict, failed_matches, skipped_sections = find_direct_matches_for_special_files(
            system_sections, target_hierarchy, target_lines
        )
        
        for target_line_num, hierarchy_string in matched_dict.items():
            sections_to_delete.append(int(target_line_num))
            thread_safe_print(f"      ✅ Marked system section for deletion: line {target_line_num}")
        
        if failed_matches:
            thread_safe_print(f"      ❌ Failed to match {len(failed_matches)} system sections")
            for failed_line in failed_matches:
                thread_safe_print(f"         - Line {failed_line}: {system_sections[failed_line]}")
    
    # Process regular sections with AI matching
    if regular_sections:
        thread_safe_print(f"   🤖 AI matching for {len(regular_sections)} regular sections...")
        
        # Filter target hierarchy for AI
        filtered_target_hierarchy = filter_non_system_sections(target_hierarchy)
        
        # Check if filtered hierarchy is reasonable for AI
        if len(filtered_target_hierarchy) > max_non_system_sections:
            thread_safe_print(f"      ❌ Target hierarchy too large for AI: {len(filtered_target_hierarchy)} > {max_non_system_sections}")
        else:
            # Get AI mapping (convert dict values to lists as expected by the function)
            source_list = list(regular_sections.values())
            target_list = list(filtered_target_hierarchy.values())
            
            ai_mapping = get_corresponding_sections(
                source_list, 
                target_list, 
                ai_client,
                repo_config['source_language'], 
                repo_config['target_language'],
                max_tokens=20000  # Use default value for now, can be made configurable later
            )
            
            if ai_mapping:
                # Parse AI response and find matching line numbers
                ai_sections = parse_ai_response(ai_mapping)
                ai_matched = find_matching_line_numbers(ai_sections, target_hierarchy)
                
                for source_line, target_line in ai_matched.items():
                    try:
                        sections_to_delete.append(int(target_line))
                        thread_safe_print(f"      ✅ Marked regular section for deletion: line {target_line}")
                    except ValueError as e:
                        thread_safe_print(f"      ❌ Error converting target_line to int: {target_line}, error: {e}")
                        # If target_line is not a number, try to find it in target_hierarchy
                        for line_num, hierarchy in target_hierarchy.items():
                            if target_line in hierarchy or hierarchy in target_line:
                                sections_to_delete.append(int(line_num))
                                thread_safe_print(f"      ✅ Found matching section at line {line_num}: {hierarchy}")
                                break
    
    # Delete the sections from local document
    if sections_to_delete:
        success = delete_sections_from_document(file_path, sections_to_delete, repo_config['target_local_path'])
        if success:
            return True, f"Successfully deleted {len(sections_to_delete)} sections from {file_path}"
        else:
            return False, f"Failed to delete sections from {file_path}"
    else:
        return False, f"No sections to delete in {file_path}"

def delete_sections_from_document(file_path, sections_to_delete, target_local_path):
    """Delete specified sections from the local document"""
    target_file_path = os.path.join(target_local_path, file_path)
    
    if not os.path.exists(target_file_path):
        thread_safe_print(f"   ❌ Target file not found: {target_file_path}")
        return False
    
    try:
        # Read current file content
        with open(target_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        lines = content.split('\n')
        
        # Import needed function
        from pr_analyzer import build_hierarchy_dict
        
        # Build hierarchy to understand section boundaries
        target_hierarchy = build_hierarchy_dict(content)
        
        # Sort sections to delete in reverse order to maintain line numbers
        sections_to_delete.sort(reverse=True)
        
        thread_safe_print(f"   🗑️  Deleting {len(sections_to_delete)} sections from {file_path}")
        
        for section_line in sections_to_delete:
            section_start = section_line - 1  # Convert to 0-based index
            
            if section_start < 0 or section_start >= len(lines):
                thread_safe_print(f"      ❌ Invalid section line: {section_line}")
                continue
            
            # Find section end
            section_end = len(lines) - 1  # Default to end of file
            
            # Look for next header at same or higher level
            current_line = lines[section_start].strip()
            if current_line.startswith('#'):
                current_level = len(current_line.split('#')[1:])  # Count # characters
                
                for i in range(section_start + 1, len(lines)):
                    line = lines[i].strip()
                    if line.startswith('#'):
                        line_level = len(line.split('#')[1:])
                        if line_level <= current_level:
                            section_end = i - 1
                            break
            
            # Delete section (from section_start to section_end inclusive)
            thread_safe_print(f"      🗑️  Deleting lines {section_start + 1} to {section_end + 1}")
            del lines[section_start:section_end + 1]
        
        # Write updated content back to file
        updated_content = '\n'.join(lines)
        with open(target_file_path, 'w', encoding='utf-8') as f:
            f.write(updated_content)
        
        thread_safe_print(f"   ✅ Updated file: {target_file_path}")
        return True
        
    except Exception as e:
        thread_safe_print(f"   ❌ Error deleting sections from {target_file_path}: {e}")
        return False

def process_single_file(file_path, source_sections, pr_diff, pr_url, github_client, ai_client, repo_config, max_non_system_sections=120):
    """Process a single file - thread-safe function for parallel processing"""
    thread_id = threading.current_thread().name
    thread_safe_print(f"\n📄 [{thread_id}] Processing {file_path}")
    
    try:
        # Check if this is a TOC file with special operations
        if isinstance(source_sections, dict) and 'type' in source_sections and source_sections['type'] == 'toc':
            from toc_processor import process_toc_file
            return process_toc_file(file_path, source_sections, pr_url, github_client, ai_client, repo_config)
        
        # Check if this is enhanced sections
        if isinstance(source_sections, dict) and 'sections' in source_sections:
            if source_sections.get('type') == 'enhanced_sections':
                # Skip all the matching logic and directly extract data
                thread_safe_print(f"   [{thread_id}] 🚀 Using enhanced sections data, skipping matching logic")
                enhanced_sections = source_sections['sections']
                
                # Extract target sections and source old content from enhanced sections
                # Maintain the exact order from match_source_diff_to_target.json
                from collections import OrderedDict
                target_sections = OrderedDict()
                source_old_content_dict = OrderedDict()
                
                # Process in the exact order they appear in enhanced_sections (which comes from match_source_diff_to_target.json)
                for key, section_info in enhanced_sections.items():
                    if isinstance(section_info, dict):
                        operation = section_info.get('source_operation', '')
                        
                        # Skip deleted sections - they shouldn't be in the enhanced_sections anyway
                        if operation == 'deleted':
                            continue
                        
                        # For source sections: use old_content for modified, new_content for added
                        if operation == 'added':
                            source_content = section_info.get('source_new_content', '')
                        else:  # modified
                            source_content = section_info.get('source_old_content', '')
                        
                        # For target sections: use target_content for modified, empty string for added
                        if operation == 'added':
                            target_content = ""  # Added sections have no existing target content
                        else:  # modified
                            target_content = section_info.get('target_content', '')
                        
                        # Add to both dictionaries using the same key from match_source_diff_to_target.json
                        if source_content is not None:
                            source_old_content_dict[key] = source_content
                        target_sections[key] = target_content
                
                thread_safe_print(f"   [{thread_id}] 📊 Extracted: {len(target_sections)} target sections, {len(source_old_content_dict)} source old content entries")
                
                # Update sections with AI (get-updated-target-sections.py logic)
                thread_safe_print(f"   [{thread_id}] 🤖 Getting updated sections from AI...")
                updated_sections = get_updated_sections_from_ai(pr_diff, target_sections, source_old_content_dict, ai_client, repo_config['source_language'], repo_config['target_language'], file_path)
                if not updated_sections:
                    thread_safe_print(f"   [{thread_id}] ⚠️  Could not get AI update")
                    return False, f"Could not get AI update for {file_path}"
                
                # Return the AI results for further processing
                thread_safe_print(f"   [{thread_id}] ✅ Successfully got AI translation results for {file_path}")
                return True, updated_sections  # Return the actual AI results
                    
            else:
                # New format: complete data structure
                actual_sections = source_sections['sections']
        
        # Regular file processing continues here for old format
        # Get target hierarchy and content (get-target-affected-hierarchy.py logic)
        from pr_analyzer import get_target_hierarchy_and_content
        target_hierarchy, target_lines = get_target_hierarchy_and_content(file_path, github_client, repo_config['target_repo'])
        if not target_hierarchy:
            thread_safe_print(f"   [{thread_id}] ⚠️  Could not get target content")
            return False, f"Could not get target content for {file_path}"
        else:
            # Old format: direct dict
            actual_sections = source_sections
            
        # Only do mapping if we don't have enhanced sections
        if 'enhanced_sections' not in locals() or not enhanced_sections:
            # Separate different types of sections
            from section_matcher import is_system_variable_or_config
            system_var_sections = {}
            toplevel_sections = {}
            frontmatter_sections = {}
            regular_sections = {}
            
            for line_num, hierarchy in actual_sections.items():
                if line_num == "0" and hierarchy == "frontmatter":
                    # Special handling for frontmatter
                    frontmatter_sections[line_num] = hierarchy
                else:
                    # Extract the leaf title from hierarchy
                    leaf_title = hierarchy.split(' > ')[-1] if ' > ' in hierarchy else hierarchy
                    
                    if is_system_variable_or_config(leaf_title):
                        system_var_sections[line_num] = hierarchy
                    elif leaf_title.startswith('# '):
                        # Top-level titles need special handling
                        toplevel_sections[line_num] = hierarchy
                    else:
                        regular_sections[line_num] = hierarchy
        
        thread_safe_print(f"   [{thread_id}] 📊 Found {len(system_var_sections)} system variable/config, {len(toplevel_sections)} top-level, {len(frontmatter_sections)} frontmatter, and {len(regular_sections)} regular sections")
        
        target_affected = {}
        
        # Process frontmatter sections with special handling
        if frontmatter_sections:
            thread_safe_print(f"   [{thread_id}] 📄 Processing frontmatter section...")
            # For frontmatter, we simply map it to line 0 in target
            for line_num, hierarchy in frontmatter_sections.items():
                target_affected[line_num] = hierarchy
            thread_safe_print(f"   [{thread_id}] ✅ Mapped {len(frontmatter_sections)} frontmatter section")
        
        # Process top-level titles with special matching
        if toplevel_sections:
            thread_safe_print(f"   [{thread_id}] 🔝 Top-level title matching for {len(toplevel_sections)} sections...")
            from section_matcher import find_toplevel_title_matches
            toplevel_matched, toplevel_failed, toplevel_skipped = find_toplevel_title_matches(toplevel_sections, target_lines)
            
            if toplevel_matched:
                target_affected.update(toplevel_matched)
                thread_safe_print(f"   [{thread_id}] ✅ Top-level matched {len(toplevel_matched)} sections")
            
            if toplevel_failed:
                thread_safe_print(f"   [{thread_id}] ⚠️  {len(toplevel_failed)} top-level sections failed matching")
                for failed in toplevel_failed:
                    thread_safe_print(f"       ❌ {failed['hierarchy']}: {failed['reason']}")
        
        # Process system variables/config sections with direct matching
        if system_var_sections:
            thread_safe_print(f"   [{thread_id}] 🎯 Direct matching {len(system_var_sections)} system variable/config sections...")
            from section_matcher import find_direct_matches_for_special_files
            direct_matched, failed_matches, skipped_sections = find_direct_matches_for_special_files(system_var_sections, target_hierarchy, target_lines)
            
            if direct_matched:
                target_affected.update(direct_matched)
                thread_safe_print(f"   [{thread_id}] ✅ Direct matched {len(direct_matched)} system variable/config sections")
            
            if failed_matches:
                thread_safe_print(f"   [{thread_id}] ⚠️  {len(failed_matches)} system variable/config sections failed direct matching")
                for failed in failed_matches:
                    thread_safe_print(f"       ❌ {failed['hierarchy']}: {failed['reason']}")
        
        # Process regular sections with AI mapping using filtered target hierarchy
        if regular_sections:
            thread_safe_print(f"   [{thread_id}] 🤖 AI mapping {len(regular_sections)} regular sections...")
            
            # Filter target hierarchy to only include non-system sections for AI mapping
            from section_matcher import filter_non_system_sections
            filtered_target_hierarchy = filter_non_system_sections(target_hierarchy)
            
            # Check if filtered target hierarchy exceeds the maximum allowed for AI mapping
            MAX_NON_SYSTEM_SECTIONS_FOR_AI = 120
            if len(filtered_target_hierarchy) > MAX_NON_SYSTEM_SECTIONS_FOR_AI:
                thread_safe_print(f"   [{thread_id}] ❌ Too many non-system sections ({len(filtered_target_hierarchy)} > {MAX_NON_SYSTEM_SECTIONS_FOR_AI})")
                thread_safe_print(f"   [{thread_id}] ⚠️  Skipping AI mapping for regular sections to avoid complexity")
                
                # If no system sections were matched either, return error
                if not target_affected:
                    error_message = f"File {file_path} has too many non-system sections ({len(filtered_target_hierarchy)} > {MAX_NON_SYSTEM_SECTIONS_FOR_AI}) and no system variable sections were matched"
                    return False, error_message
                
                # Continue with only system variable matches if available
                thread_safe_print(f"   [{thread_id}] ✅ Proceeding with {len(target_affected)} system variable/config sections only")
            else:
                # Proceed with AI mapping using filtered hierarchy
                source_list = list(regular_sections.values())
                target_list = list(filtered_target_hierarchy.values())
                
                from section_matcher import get_corresponding_sections
                ai_response = get_corresponding_sections(source_list, target_list, ai_client, repo_config['source_language'], repo_config['target_language'], max_tokens=20000)
                if ai_response:
                    # Parse AI response and find matching line numbers in the original (unfiltered) hierarchy
                    from section_matcher import parse_ai_response, find_matching_line_numbers
                    ai_sections = parse_ai_response(ai_response)
                    ai_matched = find_matching_line_numbers(ai_sections, target_hierarchy)  # Use original hierarchy for line number lookup
                    
                    if ai_matched:
                        target_affected.update(ai_matched)
                        thread_safe_print(f"   [{thread_id}] ✅ AI mapped {len(ai_matched)} regular sections")
                    else:
                        thread_safe_print(f"   [{thread_id}] ⚠️  AI mapping failed for regular sections")
                else:
                    thread_safe_print(f"   [{thread_id}] ⚠️  Could not get AI response for regular sections")
        
        # Summary of mapping results
        thread_safe_print(f"   [{thread_id}] 📊 Total mapped: {len(target_affected)} out of {len(actual_sections)} sections")
        
        if not target_affected:
            thread_safe_print(f"   [{thread_id}] ⚠️  Could not map sections")
            return False, f"Could not map sections for {file_path}"
        
        thread_safe_print(f"   [{thread_id}] ✅ Mapped {len(target_affected)} sections")
        
        # Extract target sections (get-target-affected-sections.py logic)
        thread_safe_print(f"   [{thread_id}] 📝 Extracting target sections...")
        from pr_analyzer import extract_affected_sections
        target_sections = extract_affected_sections(target_affected, target_lines)
        
        # Extract source old content from the enhanced data structure
        thread_safe_print(f"   [{thread_id}] 📖 Extracting source old content...")
        source_old_content_dict = {}
        
        # Handle different data structures for source_sections
        if isinstance(source_sections, dict) and 'sections' in source_sections:
            # New format: complete data structure with enhanced matching info
            for key, section_info in source_sections.items():
                if isinstance(section_info, dict) and 'source_old_content' in section_info:
                    source_old_content_dict[key] = section_info['source_old_content']
        else:
            # Fallback: if we don't have the enhanced structure, we need to get it differently
            thread_safe_print(f"   [{thread_id}] ⚠️  Source sections missing enhanced structure, using fallback")
            # For now, create empty dict to avoid errors - this should be addressed in the calling code
            source_old_content_dict = {}
        
        # Update sections with AI (get-updated-target-sections.py logic)
        thread_safe_print(f"   [{thread_id}] 🤖 Getting updated sections from AI...")
        updated_sections = get_updated_sections_from_ai(pr_diff, target_sections, source_old_content_dict, ai_client, repo_config['source_language'], repo_config['target_language'], file_path)
        if not updated_sections:
            thread_safe_print(f"   [{thread_id}] ⚠️  Could not get AI update")
            return False, f"Could not get AI update for {file_path}"
        
        # Update local document (update-target-doc-v2.py logic)
        thread_safe_print(f"   [{thread_id}] 💾 Updating local document...")
        success = update_local_document(file_path, updated_sections, target_affected, repo_config['target_local_path'])
        
        if success:
            thread_safe_print(f"   [{thread_id}] 🎉 Successfully updated {file_path}")
            return True, f"Successfully updated {file_path}"
        else:
            thread_safe_print(f"   [{thread_id}] ❌ Failed to update {file_path}")
            return False, f"Failed to update {file_path}"
            
    except Exception as e:
        thread_safe_print(f"   [{thread_id}] ❌ Error processing {file_path}: {e}")
        return False, f"Error processing {file_path}: {e}"

def process_added_sections(added_sections, pr_diff, pr_url, github_client, ai_client, repo_config, max_non_system_sections=120):
    """Process added sections by translating and inserting them"""
    if not added_sections:
        thread_safe_print("\n➕ No added sections to process")
        return
    
    thread_safe_print(f"\n➕ Processing added sections from {len(added_sections)} files...")
    
    # Import needed functions
    from section_matcher import map_insertion_points_to_target
    from pr_analyzer import get_target_hierarchy_and_content
    
    for file_path, section_data in added_sections.items():
        thread_safe_print(f"\n➕ Processing added sections in {file_path}")
        
        source_sections = section_data['sections']
        insertion_points = section_data['insertion_points']
        
        # Get target file hierarchy and content
        target_hierarchy, target_lines = get_target_hierarchy_and_content(
            file_path, github_client, repo_config['target_repo']
        )
        
        if not target_hierarchy:
            thread_safe_print(f"   ❌ Could not get target hierarchy for {file_path}")
            continue
        
        # Map insertion points to target language
        target_insertion_points = map_insertion_points_to_target(
            insertion_points, target_hierarchy, target_lines, file_path, pr_url, github_client, ai_client, repo_config, max_non_system_sections
        )
        
        if not target_insertion_points:
            thread_safe_print(f"   ❌ No insertion points mapped for {file_path}")
            continue
        
        # Use AI to translate/update new sections (similar to modified sections)
        # Since we're now using source_old_content, we need to extract it from the added sections
        source_old_content_dict = {}
        for key, content in source_sections.items():
            # For added sections, source_old_content is typically None or empty
            # We use the new content (from the source file) as the content to translate
            source_old_content_dict[key] = content if content is not None else ""
        
        # Get target sections (empty for new sections, but we need the structure)
        target_sections = {}  # New sections don't have existing target content
        
        # Use the same AI function to translate the new sections
        translated_sections = get_updated_sections_from_ai(
            pr_diff, 
            target_sections, 
            source_old_content_dict, 
            ai_client,
            repo_config['source_language'], 
            repo_config['target_language'],
            file_path
        )
        
        if translated_sections:
            # Insert translated sections into document
            insert_sections_into_document(file_path, translated_sections, target_insertion_points, repo_config['target_local_path'])
            thread_safe_print(f"   ✅ Successfully inserted {len(translated_sections)} sections in {file_path}")
        else:
            thread_safe_print(f"   ⚠️  No sections were translated for {file_path}")

def process_files_in_batches(source_changes, pr_diff, pr_url, github_client, ai_client, repo_config, operation_type="modified", batch_size=5, max_non_system_sections=120):
    """Process files in parallel batches"""
    # Handle different data formats
    if isinstance(source_changes, dict):
        files = []
        for path, data in source_changes.items():
            if isinstance(data, dict):
                if 'type' in data and data['type'] == 'toc':
                    # TOC file with special operations
                    files.append((path, data))
                elif 'sections' in data:
                    # New format: extract sections for processing
                    files.append((path, data['sections']))
                else:
                    # Old format: direct dict
                    files.append((path, data))
            else:
                # Old format: direct dict
                files.append((path, data))
    else:
        files = list(source_changes.items())
    
    total_files = len(files)
    
    if total_files == 0:
        return []
    
    thread_safe_print(f"\n🔄 Processing {total_files} files in batches of {batch_size}")
    
    results = []
    
    # Process files in batches
    for i in range(0, total_files, batch_size):
        batch = files[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total_files + batch_size - 1) // batch_size
        
        thread_safe_print(f"\n📦 Batch {batch_num}/{total_batches}: Processing {len(batch)} files")
        
        # Process current batch in parallel
        with ThreadPoolExecutor(max_workers=len(batch), thread_name_prefix=f"Batch{batch_num}") as executor:
            # Submit all files in current batch
            future_to_file = {}
            for file_path, source_sections in batch:
                future = executor.submit(
                    process_single_file, 
                    file_path, 
                    source_sections, 
                    pr_diff, 
                    pr_url, 
                    github_client, 
                    ai_client,
                    repo_config,
                    max_non_system_sections
                )
                future_to_file[future] = file_path
            
            # Collect results as they complete
            from concurrent.futures import as_completed
            batch_results = []
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    success, message = future.result()
                    batch_results.append((file_path, success, message))
                except Exception as e:
                    batch_results.append((file_path, False, f"Exception in thread: {e}"))
            
            results.extend(batch_results)
        
        # Brief pause between batches to avoid overwhelming the APIs
        if i + batch_size < total_files:
            thread_safe_print(f"   ⏸️  Waiting 2 seconds before next batch...")
            import time
            time.sleep(2)
    
    return results

def update_target_document_from_match_data(match_file_path, target_local_path, target_file_name=None):
    """
    Update target document using data from match_source_diff_to_target.json
    This integrates the logic from test_target_update.py
    
    Args:
        match_file_path: Path to the match_source_diff_to_target.json file
        target_local_path: Local path to the target repository 
        target_file_name: Optional target file name (if not provided, will be extracted from match_file_path)
    """
    import json
    import os
    from pathlib import Path
    
    # Load match data
    if not os.path.exists(match_file_path):
        thread_safe_print(f"❌ {match_file_path} file does not exist")
        return False
    
    with open(match_file_path, 'r', encoding='utf-8') as f:
        match_data = json.load(f)
    
    thread_safe_print(f"✅ Loaded {len(match_data)} section matching data from {match_file_path}")
    thread_safe_print(f"   Reading translation results directly from target_new_content field")
    
    if not match_data:
        thread_safe_print("❌ No matching data found")
        return False
    
    # Sort sections by target_line from large to small (modify from back to front)
    sections_with_line = []
    
    for key, section_data in match_data.items():
        operation = section_data.get('source_operation', '')
        target_new_content = section_data.get('target_new_content')
        
        # For deleted sections, target_new_content should be null
        if operation == 'deleted':
            if target_new_content is not None:
                thread_safe_print(f"   ⚠️  Deleted section {key} has non-null target_new_content, should be fixed")
            thread_safe_print(f"   🗑️  Including deleted section: {key}")
        elif not target_new_content:
            thread_safe_print(f"   ⚠️  Skipping section without target_new_content: {key}")
            continue
        
        target_line = section_data.get('target_line')
        if target_line and target_line != 'unknown':
            try:
                # Handle special case for bottom sections
                if target_line == "-1":
                    line_num = -1  # Special marker for bottom sections
                else:
                    line_num = int(target_line)
                sections_with_line.append((key, section_data, line_num))
            except ValueError:
                thread_safe_print(f"⚠️  Skipping invalid target_line: {target_line} for {key}")
    
    # Separate sections into different processing groups
    bottom_modified_sections = []  # Process first: modify existing content at document end
    regular_sections = []          # Process second: normal operations from back to front
    bottom_added_sections = []     # Process last: append new content to document end
    
    for key, section_data, line_num in sections_with_line:
        target_hierarchy = section_data.get('target_hierarchy', '')
        
        if target_hierarchy.startswith('bottom-modified-'):
            bottom_modified_sections.append((key, section_data, line_num))
        elif target_hierarchy.startswith('bottom-added-'):
            bottom_added_sections.append((key, section_data, line_num))
        else:
            regular_sections.append((key, section_data, line_num))
    
    # Sort each group appropriately
    def get_source_line_num(item):
        key, section_data, line_num = item
        if '_' in key and key.split('_')[1].isdigit():
            return int(key.split('_')[1])
        return 0
    
    # Bottom modified: sort by source line number (large to small)
    bottom_modified_sections.sort(key=lambda x: -get_source_line_num(x))
    
    # Regular sections: sort by target_line (large to small), then by source line number
    regular_sections.sort(key=lambda x: (-x[2], -get_source_line_num(x)))
    
    # Bottom added: sort by source line number (small to large) for proper document order
    bottom_added_sections.sort(key=lambda x: get_source_line_num(x))
    
    # Combine all sections in processing order
    all_sections = bottom_modified_sections + regular_sections + bottom_added_sections
    
    thread_safe_print(f"\n📊 Processing order: bottom-modified -> regular -> bottom-added")
    thread_safe_print(f"   📋 Bottom modified sections: {len(bottom_modified_sections)}")
    thread_safe_print(f"   📋 Regular sections: {len(regular_sections)}")  
    thread_safe_print(f"   📋 Bottom added sections: {len(bottom_added_sections)}")
    
    if not all_sections:
        thread_safe_print("❌ No valid sections found for update")
        return False
    
    thread_safe_print(f"\n📊 Detailed processing order:")
    for i, (key, section_data, line_num) in enumerate(all_sections, 1):
        operation = section_data.get('source_operation', '')
        hierarchy = section_data.get('target_hierarchy', '')
        insertion_type = section_data.get('insertion_type', '')
        
        # Extract source line number for display
        source_line_num = int(key.split('_')[1]) if '_' in key and key.split('_')[1].isdigit() else 'N/A'
        
        # Display target_line with special handling for bottom sections
        target_display = "END" if line_num == -1 else str(line_num)
        
        # Determine section group
        if hierarchy.startswith('bottom-modified-'):
            group = "BotMod"
        elif hierarchy.startswith('bottom-added-'):
            group = "BotAdd"
        else:
            group = "Regular"
        
        if operation == 'deleted':
            action = "delete"
        elif insertion_type == "before_reference":
            action = "insert"
        elif line_num == -1:
            action = "append"
        else:
            action = "replace"
        
        thread_safe_print(f"   {i:2}. [{group:7}] Target:{target_display:>3} Src:{source_line_num:3} | {key:15} ({operation:8}) | {action:7} | {hierarchy}")
    
    # Determine target file name
    if target_file_name is None:
        # Extract target file name from match file path
        # e.g., "tikv-configuration-file-match_source_diff_to_target.json" -> "tikv-configuration-file.md"
        match_filename = os.path.basename(match_file_path)
        if match_filename.endswith('-match_source_diff_to_target.json'):
            extracted_name = match_filename[:-len('-match_source_diff_to_target.json')] + '.md'
            target_file_name = extracted_name
            thread_safe_print(f"   📂 Extracted target file name from match file: {target_file_name}")
        else:
            # Fallback: try to determine from source hierarchy
            first_entry = next(iter(match_data.values()))
            source_hierarchy = first_entry.get('source_original_hierarchy', '')
            
            if 'TiFlash' in source_hierarchy or 'tiflash' in source_hierarchy.lower():
                target_file_name = "tiflash/tiflash-configuration.md"
            else:
                # Default to command-line flags for other cases
                target_file_name = "command-line-flags-for-tidb-configuration.md"
            thread_safe_print(f"   📂 Determined target file name from hierarchy: {target_file_name}")
    else:
        thread_safe_print(f"   📂 Using provided target file name: {target_file_name}")
    
    target_file_path = os.path.join(target_local_path, target_file_name)
    thread_safe_print(f"\n📄 Target file path: {target_file_path}")
    
    # Update target document
    thread_safe_print(f"\n🚀 Starting target document update, will modify {len(all_sections)} sections...")
    success = update_target_document_sections(all_sections, target_file_path)
    
    return success

def update_target_document_sections(all_sections, target_file_path):
    """
    Update target document sections - integrated from test_target_update.py
    """
    thread_safe_print(f"\n🚀 Starting target document update: {target_file_path}")
    
    # Read target document
    if not os.path.exists(target_file_path):
        thread_safe_print(f"❌ Target file does not exist: {target_file_path}")
        return False
    
    with open(target_file_path, 'r', encoding='utf-8') as f:
        target_lines = f.readlines()
    
    thread_safe_print(f"📄 Target document total lines: {len(target_lines)}")
    
    # Process modifications in order (bottom-modified -> regular -> bottom-added)
    for i, (key, section_data, target_line_num) in enumerate(all_sections, 1):
        operation = section_data.get('source_operation', '')
        insertion_type = section_data.get('insertion_type', '')
        target_hierarchy = section_data.get('target_hierarchy', '')
        target_new_content = section_data.get('target_new_content')
        
        thread_safe_print(f"\n📝 {i}/{len(all_sections)} Processing {key} (Line {target_line_num})")
        thread_safe_print(f"   Operation type: {operation}")
        thread_safe_print(f"   Target section: {target_hierarchy}")
        
        if operation == 'deleted':
            # Delete logic: remove the specified section
            if target_line_num == -1:
                thread_safe_print(f"   ❌ Invalid delete operation for bottom section")
                continue
                
            thread_safe_print(f"   🗑️  Delete mode: removing section starting at line {target_line_num}")
            
            # Find section end position
            start_line = target_line_num - 1  # Convert to 0-based index
            
            if start_line >= len(target_lines):
                thread_safe_print(f"   ❌ Line number out of range: {target_line_num} > {len(target_lines)}")
                continue
            
            # Find section end position
            end_line = find_section_end_for_update(target_lines, start_line, target_hierarchy)
            
            thread_safe_print(f"   📍 Delete range: line {start_line + 1} to {end_line}")
            thread_safe_print(f"   📄 Delete content: {target_lines[start_line].strip()[:50]}...")
            
            # Delete content
            deleted_lines = target_lines[start_line:end_line]
            target_lines[start_line:end_line] = []
            
            thread_safe_print(f"   ✅ Deleted {len(deleted_lines)} lines of content")
            
        elif target_new_content is None:
            thread_safe_print(f"   ⚠️  Skipping: target_new_content is null")
            continue
            
        elif not target_new_content:
            thread_safe_print(f"   ⚠️  Skipping: target_new_content is empty")
            continue
            
        else:
            # Handle content format
            thread_safe_print(f"   📄 Content preview: {repr(target_new_content[:80])}...")
            
            if target_hierarchy.startswith('bottom-'):
                # Bottom section special handling
                if target_hierarchy.startswith('bottom-modified-'):
                    # Bottom modified: find and replace existing content at document end
                    thread_safe_print(f"   🔄 Bottom modified section: replacing existing content at document end")
                    
                    # Get the old content to search for
                    source_operation_data = section_data.get('source_operation_data', {})
                    old_content = source_operation_data.get('old_content', '').strip()
                    
                    if old_content:
                        # Search backwards from end to find the matching section
                        found_line = None
                        for idx in range(len(target_lines) - 1, -1, -1):
                            line_content = target_lines[idx].strip()
                            if line_content == old_content:
                                found_line = idx
                                thread_safe_print(f"   📍 Found target section at line {found_line + 1}: {line_content[:50]}...")
                                break
                        
                        if found_line is not None:
                            # Find section end
                            end_line = find_section_end_for_update(target_lines, found_line, target_hierarchy)
                            
                            # Ensure content format is correct
                            if not target_new_content.endswith('\n'):
                                target_new_content += '\n'
                            
                            # Split content by lines
                            new_lines = target_new_content.splitlines(keepends=True)
                            
                            # Replace content
                            target_lines[found_line:end_line] = new_lines
                            
                            thread_safe_print(f"   ✅ Replaced {end_line - found_line} lines with {len(new_lines)} lines")
                        else:
                            thread_safe_print(f"   ⚠️  Could not find target section, appending to end instead")
                            # Fallback: append to end
                            if not target_new_content.endswith('\n'):
                                target_new_content += '\n'
                            if target_lines and target_lines[-1].strip():
                                target_new_content = '\n' + target_new_content
                            new_lines = target_new_content.splitlines(keepends=True)
                            target_lines.extend(new_lines)
                            thread_safe_print(f"   ✅ Appended {len(new_lines)} lines to end of document")
                    else:
                        thread_safe_print(f"   ⚠️  No old_content found, appending to end instead")
                        # Fallback: append to end
                        if not target_new_content.endswith('\n'):
                            target_new_content += '\n'
                        if target_lines and target_lines[-1].strip():
                            target_new_content = '\n' + target_new_content
                        new_lines = target_new_content.splitlines(keepends=True)
                        target_lines.extend(new_lines)
                        thread_safe_print(f"   ✅ Appended {len(new_lines)} lines to end of document")
                        
                elif target_hierarchy.startswith('bottom-added-'):
                    # Bottom added: append new content to end of document
                    thread_safe_print(f"   🔚 Bottom added section: appending new content to end")
                    
                    # Ensure content format is correct
                    if not target_new_content.endswith('\n'):
                        target_new_content += '\n'
                    
                    # Add spacing before new section if needed
                    if target_lines and target_lines[-1].strip():
                        target_new_content = '\n' + target_new_content
                    
                    # Split content by lines
                    new_lines = target_new_content.splitlines(keepends=True)
                    
                    # Append to end of document
                    target_lines.extend(new_lines)
                    
                    thread_safe_print(f"   ✅ Appended {len(new_lines)} lines to end of document")
                else:
                    # Other bottom sections: append to end
                    thread_safe_print(f"   🔚 Other bottom section: appending to end of document")
                    
                    # Ensure content format is correct
                    if not target_new_content.endswith('\n'):
                        target_new_content += '\n'
                    
                    # Add spacing before new section if needed
                    if target_lines and target_lines[-1].strip():
                        target_new_content = '\n' + target_new_content
                    
                    # Split content by lines
                    new_lines = target_new_content.splitlines(keepends=True)
                    
                    # Append to end of document
                    target_lines.extend(new_lines)
                    
                    thread_safe_print(f"   ✅ Appended {len(new_lines)} lines to end of document")
                
            elif target_hierarchy == "frontmatter":
                # Frontmatter special handling: directly replace front lines
                thread_safe_print(f"   📄 Frontmatter mode: directly replacing document beginning")
                
                # Find the first top-level heading position
                first_header_line = 0
                for i, line in enumerate(target_lines):
                    if line.strip().startswith('# '):
                        first_header_line = i
                        break
                
                thread_safe_print(f"   📍 Frontmatter range: line 1 to {first_header_line}")
                
                # Split new content by lines, preserving original structure including trailing empty lines
                new_lines = target_new_content.splitlines(keepends=True)
                
                # If the original content ends with \n, it means there should be an empty line after the last content line
                # splitlines() doesn't create this empty line, so we need to add it manually
                if target_new_content.endswith('\n'):
                    new_lines.append('\n')
                elif target_new_content:
                    # If content doesn't end with newline, ensure the last line has one
                    if not new_lines[-1].endswith('\n'):
                        new_lines[-1] += '\n'
                
                # Replace frontmatter
                target_lines[0:first_header_line] = new_lines
                
                thread_safe_print(f"   ✅ Replaced {first_header_line} lines of frontmatter with {len(new_lines)} lines")
                
            elif insertion_type == "before_reference":
                # Insert logic: insert before specified line
                if target_line_num == -1:
                    thread_safe_print(f"   ❌ Invalid insert operation for bottom section")
                    continue
                    
                thread_safe_print(f"   📍 Insert mode: inserting before line {target_line_num}")
                
                # Ensure content format is correct
                if not target_new_content.endswith('\n'):
                    target_new_content += '\n'
                
                # Ensure spacing between sections
                if not target_new_content.endswith('\n\n'):
                    target_new_content += '\n'
                
                # Split content by lines
                new_lines = target_new_content.splitlines(keepends=True)
                
                # Insert at specified position
                insert_position = target_line_num - 1  # Convert to 0-based index
                if insert_position < 0:
                    insert_position = 0
                elif insert_position > len(target_lines):
                    insert_position = len(target_lines)
                
                # Execute insertion
                for j, line in enumerate(new_lines):
                    target_lines.insert(insert_position + j, line)
                
                thread_safe_print(f"   ✅ Inserted {len(new_lines)} lines of content")
                
            else:
                # Replace logic: find target section and replace
                if target_line_num == -1:
                    thread_safe_print(f"   ❌ Invalid replace operation for bottom section")
                    continue
                    
                thread_safe_print(f"   🔄 Replace mode: replacing section starting at line {target_line_num}")
                
                # Ensure content format is correct
                if not target_new_content.endswith('\n'):
                    target_new_content += '\n'
                
                # Ensure spacing between sections
                if not target_new_content.endswith('\n\n'):
                    target_new_content += '\n'
                
                # Find section end position
                start_line = target_line_num - 1  # Convert to 0-based index
                
                if start_line >= len(target_lines):
                    thread_safe_print(f"   ❌ Line number out of range: {target_line_num} > {len(target_lines)}")
                    continue
                
                # Find section end position
                end_line = find_section_end_for_update(target_lines, start_line, target_hierarchy)
                
                thread_safe_print(f"   📍 Replace range: line {start_line + 1} to {end_line}")
                
                # Split new content by lines
                new_lines = target_new_content.splitlines(keepends=True)
                
                # Replace content
                target_lines[start_line:end_line] = new_lines
                
                thread_safe_print(f"   ✅ Replaced {end_line - start_line} lines with {len(new_lines)} lines")
    
    
    with open(target_file_path, 'w', encoding='utf-8') as f:
        f.writelines(target_lines)
    
    thread_safe_print(f"\n✅ Target document update completed!")
    thread_safe_print(f"📄 Updated file: {target_file_path}")
    
    return True

def find_section_end_for_update(lines, start_line, target_hierarchy):
    """Find section end position - based on test_target_update.py logic"""
    current_line = lines[start_line].strip()
    
    if target_hierarchy == "frontmatter":
        # Frontmatter special handling: from --- to second ---, then to first top-level heading
        if start_line == 0 and current_line.startswith('---'):
            # Find second ---
            for i in range(start_line + 1, len(lines)):
                if lines[i].strip() == '---':
                    # Found frontmatter end, but need to include up to next content start
                    # Look for first non-empty line or first heading
                    for j in range(i + 1, len(lines)):
                        line = lines[j].strip()
                        if line and line.startswith('# '):
                            thread_safe_print(f"     📍 Frontmatter ends at line {j} (before first top-level heading)")
                            return j
                        elif line and not line.startswith('#'):
                            # If there's other content, end there
                            thread_safe_print(f"     📍 Frontmatter ends at line {j} (before other content)")
                            return j
                    # If no other content found, end after second ---
                    thread_safe_print(f"     📍 Frontmatter ends at line {i+1} (after second ---)")
                    return i + 1
        # If not standard frontmatter format, find first top-level heading
        for i in range(start_line + 1, len(lines)):
            if lines[i].strip().startswith('# '):
                thread_safe_print(f"     📍 Frontmatter ends at line {i} (before first top-level heading)")
                return i
        # If no top-level heading found, process entire file
        return len(lines)
    
    if current_line.startswith('#'):
        # Use file_updater.py method to calculate heading level
        current_level = len(current_line.split()[0]) if current_line.split() else 0
        thread_safe_print(f"     🔍 Current heading level: {current_level} (heading: {current_line[:50]}...)")
        
        # Special handling for top-level headings: only process until first second-level heading
        if current_level == 1:
            for i in range(start_line + 1, len(lines)):
                line = lines[i].strip()
                if line.startswith('##'):  # Find first second-level heading
                    thread_safe_print(f"     📍 Top-level heading ends at line {i} (before first second-level heading)")
                    return i
            # If no second-level heading found, look for next top-level heading
            for i in range(start_line + 1, len(lines)):
                line = lines[i].strip()
                if line.startswith('#') and not line.startswith('##'):
                    thread_safe_print(f"     📍 Top-level heading ends at line {i} (before next top-level heading)")
                    return i
        else:
            # For other level headings, stop at ANY header to get only direct content
            # This prevents including sub-sections in the update range
            for i in range(start_line + 1, len(lines)):
                line = lines[i].strip()
                if line.startswith('#'):
                    # Stop at ANY header to get only direct content
                    thread_safe_print(f"     📍 Found header at line {i}: {line[:30]}... (stopping for direct content only)")
                    return i
        
        # If not found, return file end
        thread_safe_print(f"     📍 No end position found, using file end")
        return len(lines)
    
    # Non-heading line, only replace current line
    return start_line + 1
