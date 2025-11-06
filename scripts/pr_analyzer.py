#!/usr/bin/env python3
"""
PR Analyzer Module
Handles PR analysis, diff parsing, content getting, hierarchy building, and section getting
"""

import json
import os
import re
import threading
from github import Github

# Thread-safe printing
print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    """Thread-safe print function"""
    with print_lock:
        print(*args, **kwargs)


def parse_pr_url(pr_url):
    """Parse PR URL to get repo info"""
    parts = pr_url.split('/')
    return parts[-4], parts[-3], int(parts[-1])  # owner, repo, pr_number

def get_repo_config(pr_url, repo_configs):
    """Get repository configuration based on source repo"""
    owner, repo, pr_number = parse_pr_url(pr_url)
    source_repo = f"{owner}/{repo}"
    
    if source_repo not in repo_configs:
        raise ValueError(f"Unsupported source repository: {source_repo}. Supported: {list(repo_configs.keys())}")
    
    config = repo_configs[source_repo].copy()
    config['source_repo'] = source_repo
    config['pr_number'] = pr_number
    
    return config

def get_pr_diff(pr_url, github_client):
    """Get the diff content from a GitHub PR"""
    try:
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
        print(f"   ❌ Error getting PR diff: {e}")
        return None

def get_changed_line_ranges(file):
    """Get the ranges of lines that were changed in the PR"""
    changed_ranges = []
    patch = file.patch
    if not patch:
        return changed_ranges
    
    lines = patch.split('\n')
    current_line = 0
    
    for line in lines:
        if line.startswith('@@'):
            # Parse the hunk header to get line numbers
            match = re.search(r'\+(\d+),?(\d+)?', line)
            if match:
                current_line = int(match.group(1))
        elif line.startswith('+') and not line.startswith('+++'):
            # This is an added line
            changed_ranges.append(current_line)
            current_line += 1
        elif line.startswith('-') and not line.startswith('---'):
            # This is a deleted line, also consider as changed
            changed_ranges.append(current_line)
            # Don't increment current_line for deleted lines
            continue
        elif line.startswith(' '):
            # Context line
            current_line += 1
    
    return changed_ranges

def analyze_diff_operations(file):
    """Analyze diff to categorize operations as added, modified, or deleted (improved GitHub-like approach)"""
    operations = {
        'added_lines': [],      # Lines that were added
        'deleted_lines': [],    # Lines that were deleted  
        'modified_lines': []    # Lines that were modified (both added and deleted content)
    }
    
    patch = file.patch
    if not patch:
        return operations
    
    lines = patch.split('\n')
    current_line = 0
    deleted_line = 0
    
    # Parse diff and keep track of sequence order for better modification detection
    diff_sequence = []  # Track the order of operations in diff
    
    for i, line in enumerate(lines):
        if line.startswith('@@'):
            # Parse the hunk header to get line numbers
            # Format: @@ -old_start,old_count +new_start,new_count @@
            match = re.search(r'-(\d+),?(\d+)?\s+\+(\d+),?(\d+)?', line)
            if match:
                deleted_line = int(match.group(1))
                current_line = int(match.group(3))
        elif line.startswith('+') and not line.startswith('+++'):
            # This is an added line
            added_entry = {
                'line_number': current_line,
                'content': line[1:],  # Remove the '+' prefix
                'is_header': line[1:].strip().startswith('#'),
                'diff_index': i  # Track position in diff
            }
            operations['added_lines'].append(added_entry)
            diff_sequence.append(('added', added_entry))
            current_line += 1
        elif line.startswith('-') and not line.startswith('---'):
            # This is a deleted line
            deleted_entry = {
                'line_number': deleted_line,
                'content': line[1:],  # Remove the '-' prefix
                'is_header': line[1:].strip().startswith('#'),
                'diff_index': i  # Track position in diff
            }
            operations['deleted_lines'].append(deleted_entry)
            diff_sequence.append(('deleted', deleted_entry))
            deleted_line += 1
        elif line.startswith(' '):
            # Context line (unchanged)
            current_line += 1
            deleted_line += 1
    
    # GitHub-like modification detection: based on diff sequence proximity
    modified_pairs = []
    deleted_headers = [d for d in operations['deleted_lines'] if d['is_header']]
    added_headers = [a for a in operations['added_lines'] if a['is_header']]
    
    used_added_indices = set()
    used_deleted_indices = set()
    
    # Helper function for semantic similarity
    def are_headers_similar(old, new):
        # Remove markdown markers
        old_clean = old.replace('#', '').replace('`', '').strip()
        new_clean = new.replace('#', '').replace('`', '').strip()
        
        # Check if one is a substring/extension of the other
        if old_clean in new_clean or new_clean in old_clean:
            return True
        
        # Check for similar patterns (like appending -pu, -new, etc.)
        old_base = old_clean.split('-')[0]
        new_base = new_clean.split('-')[0]
        if old_base and new_base and old_base == new_base:
            return True
            
        return False
    
    # GitHub-like approach: Look for adjacent or close operations in diff sequence
    for i, deleted_header in enumerate(deleted_headers):
        if i in used_deleted_indices:
            continue
            
        for j, added_header in enumerate(added_headers):
            if j in used_added_indices:
                continue
                
            deleted_content = deleted_header['content'].strip()
            added_content = added_header['content'].strip()
            
            # Check if they are close in the diff sequence (GitHub's approach)
            diff_distance = abs(added_header['diff_index'] - deleted_header['diff_index'])
            is_close_in_diff = diff_distance <= 5  # Allow small gap for context lines
            
            # Check semantic similarity
            is_similar = are_headers_similar(deleted_content, added_content)
            
            # GitHub-like logic: prioritize diff proximity + semantic similarity
            if is_close_in_diff and is_similar:
                modified_pairs.append({
                    'deleted': deleted_header,
                    'added': added_header,
                    'original_content': deleted_header['content']
                })
                used_added_indices.add(j)
                used_deleted_indices.add(i)
                break
            # Fallback: strong semantic similarity even if not adjacent
            elif is_similar and abs(added_header['line_number'] - deleted_header['line_number']) <= 20:
                modified_pairs.append({
                    'deleted': deleted_header,
                    'added': added_header,
                    'original_content': deleted_header['content']
                })
                used_added_indices.add(j)
                used_deleted_indices.add(i)
                break
    
    # Remove identified modifications from pure additions/deletions
    for pair in modified_pairs:
        if pair['deleted'] in operations['deleted_lines']:
            operations['deleted_lines'].remove(pair['deleted'])
        if pair['added'] in operations['added_lines']:
            operations['added_lines'].remove(pair['added'])
        # Store both new and original content for modified headers
        modified_entry = pair['added'].copy()
        modified_entry['original_content'] = pair['original_content']
        operations['modified_lines'].append(modified_entry)
    
    return operations

def build_hierarchy_dict(file_content):
    """Build hierarchy dictionary from file content, excluding content inside code blocks"""
    lines = file_content.split('\n')
    level_stack = []
    all_hierarchy_dict = {}
    
    # Track code block state
    in_code_block = False
    code_block_delimiter = None  # Track the type of code block (``` or ```)
    
    # Build complete hierarchy for all headers
    for line_num, line in enumerate(lines, 1):
        original_line = line
        line = line.strip()
        
        # Check for code block delimiters
        if line.startswith('```') or line.startswith('~~~'):
            if not in_code_block:
                # Entering a code block
                in_code_block = True
                code_block_delimiter = line[:3]  # Store the delimiter type
                continue
            elif line.startswith(code_block_delimiter):
                # Exiting a code block
                in_code_block = False
                code_block_delimiter = None
                continue
        
        # Skip processing if we're inside a code block
        if in_code_block:
            continue
        
        # Process headers only if not in code block
        if line.startswith('#'):
            match = re.match(r'^(#{1,10})\s+(.+)', line)
            if match:
                level = len(match.group(1))
                title = match.group(2).strip()
                
                # Remove items from stack that are at same or deeper level
                while level_stack and level_stack[-1][0] >= level:
                    level_stack.pop()
                
                # Build hierarchy with special handling for top-level titles
                if level == 1:
                    # Top-level titles are included directly without hierarchy path
                    hierarchy_line = line
                elif level_stack:
                    # For other levels, build path but skip the top-level title (level 1)
                    path_parts = [item[1] for item in level_stack if item[0] > 1]  # Skip level 1 items
                    path_parts.append(line)
                    hierarchy_line = " > ".join(path_parts)
                else:
                    # Fallback for other cases
                    hierarchy_line = line
                
                if hierarchy_line:  # Only add non-empty hierarchies
                    all_hierarchy_dict[line_num] = hierarchy_line
                
                level_stack.append((level, line))
    
    return all_hierarchy_dict

def build_hierarchy_path(lines, line_num, all_headers):
    """Build the full hierarchy path for a header at given line"""
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

def build_hierarchy_for_modified_section(file_content, target_line_num, original_line, base_hierarchy_dict):
    """Build hierarchy path for a modified section using original content"""
    lines = file_content.split('\n')
    
    # Get the level of the original header
    original_match = re.match(r'^(#{1,10})\s+(.+)', original_line)
    if not original_match:
        return None
    
    original_level = len(original_match.group(1))
    original_title = original_match.group(2).strip()
    
    # Find parent sections by looking backwards from target line
    level_stack = []
    
    for line_num in range(1, target_line_num):
        if line_num in base_hierarchy_dict:
            # This is a header line
            line_content = lines[line_num - 1].strip()
            if line_content.startswith('#'):
                match = re.match(r'^(#{1,10})\s+(.+)', line_content)
                if match:
                    level = len(match.group(1))
                    title = match.group(2).strip()
                    
                    # Remove items from stack that are at same or deeper level
                    while level_stack and level_stack[-1][0] >= level:
                        level_stack.pop()
                    
                    # Add this header to stack if it's a potential parent
                    if level < original_level:
                        level_stack.append((level, line_content))
    
    # Build hierarchy path using original content
    if level_stack:
        path_parts = [item[1] for item in level_stack[1:]]  # Skip first level
        path_parts.append(original_line)
        hierarchy_line = " > ".join(path_parts)
    else:
        hierarchy_line = original_line if original_level > 1 else ""
    
    return hierarchy_line if hierarchy_line else None

def find_section_boundaries(lines, hierarchy_dict):
    """Find the start and end line for each section based on hierarchy"""
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

def extract_section_content(lines, start_line, hierarchy_dict):
    """Extract the content of a section starting from start_line (includes sub-sections)"""
    if not lines or start_line < 1 or start_line > len(lines):
        return ""
    
    start_index = start_line - 1  # Convert to 0-based index
    section_content = []
    
    # Find the header at start_line
    current_line = lines[start_index].strip()
    if not current_line.startswith('#'):
        return ""
    
    # Get the level of current header
    current_level = len(current_line.split()[0])  # Count # characters
    section_content.append(current_line)
    
    # Special handling for top-level titles (level 1)
    if current_level == 1:
        # For top-level titles, only extract content until the first next-level header (##)
        for i in range(start_index + 1, len(lines)):
            line = lines[i].strip()
            
            if line.startswith('#'):
                # Check if this is a header of next level (##, ###, etc.)
                line_level = len(line.split()[0]) if line.split() else 0
                if line_level > current_level:
                    # Found first subsection, stop here for top-level titles
                    break
                elif line_level <= current_level:
                    # Found same or higher level header, also stop
                    break
            
            section_content.append(lines[i].rstrip())  # Keep original line without trailing whitespace
    else:
        # For non-top-level titles, use the original logic
        # Extract content until we hit the next header of same or higher level
        for i in range(start_index + 1, len(lines)):
            line = lines[i].strip()
            
            if line.startswith('#'):
                # Check if this is a header of same or higher level
                line_level = len(line.split()[0]) if line.split() else 0
                if line_level <= current_level:
                    # Found a header of same or higher level, stop here regardless
                    # Each section should be extracted individually
                    break
            
            section_content.append(lines[i].rstrip())  # Keep original line without trailing whitespace
    
    return '\n'.join(section_content)

def extract_section_direct_content(lines, start_line):
    """Extract ONLY the direct content of a section (excluding sub-sections) - for source diff dict"""
    if not lines or start_line < 1 or start_line > len(lines):
        return ""
    
    start_index = start_line - 1  # Convert to 0-based index
    section_content = []
    
    # Find the header at start_line
    current_line = lines[start_index].strip()
    if not current_line.startswith('#'):
        return ""
    
    # Add the header line
    section_content.append(current_line)
    
    # Only extract until the first header (any level)
    # This means we stop at ANY header - whether it's a sub-section OR same/higher level
    for i in range(start_index + 1, len(lines)):
        line = lines[i].strip()
        if line.startswith('#'):
            # Stop at ANY header to get only direct content
            break
        section_content.append(lines[i].rstrip())
    
    return '\n'.join(section_content)

def extract_frontmatter_content(file_lines):
    """Extract content from the beginning of file to the first top-level header"""
    if not file_lines:
        return ""
    
    frontmatter_lines = []
    for i, line in enumerate(file_lines):
        line_stripped = line.strip()
        # Stop when we hit the first top-level header
        if line_stripped.startswith('# '):
            break
        frontmatter_lines.append(line.rstrip())
    
    return '\n'.join(frontmatter_lines)


def extract_affected_sections(hierarchy_dict, file_lines):
    """Extract all affected sections based on hierarchy dict"""
    affected_sections = {}
    
    for line_num, hierarchy in hierarchy_dict.items():
        if line_num == "0" and hierarchy == "frontmatter":
            # Special handling for frontmatter
            frontmatter_content = extract_frontmatter_content(file_lines)
            if frontmatter_content:
                affected_sections[line_num] = frontmatter_content
        else:
            line_number = int(line_num)
            section_content = extract_section_content(file_lines, line_number, hierarchy_dict)
            
            if section_content:
                affected_sections[line_num] = section_content
    
    return affected_sections

def find_containing_section(line_num, all_headers):
    """Find which section a line belongs to"""
    current_section = None
    for header_line_num in sorted(all_headers.keys()):
        if header_line_num <= line_num:
            current_section = header_line_num
        else:
            break
    return current_section

def find_affected_sections(lines, changed_lines, all_headers):
    """Find which sections are affected by the changes"""
    affected_sections = set()
    
    for changed_line in changed_lines:
        # Find the section this changed line belongs to
        current_section = None
        
        # Find the most recent header before or at the changed line
        for line_num in sorted(all_headers.keys()):
            if line_num <= changed_line:
                current_section = line_num
            else:
                break
        
        if current_section:
            # Only add the directly affected section (the one that directly contains the change)
            affected_sections.add(current_section)
    
    return affected_sections

def find_sections_by_operation_type(lines, operations, all_headers, base_hierarchy_dict=None):
    """Find sections affected by different types of operations"""
    sections = {
        'added': set(),
        'modified': set(), 
        'deleted': set()
    }
    
    # Process added lines
    for added_line in operations['added_lines']:
        line_num = added_line['line_number']
        if added_line['is_header']:
            # This is a new header - only mark the section as added if the header itself is new
            sections['added'].add(line_num)
        # Note: We don't mark sections as "added" just because they contain new non-header content
        # That would be a "modified" section, not an "added" section
    
    # Process modified lines  
    for modified_line in operations['modified_lines']:
        line_num = modified_line['line_number']
        if modified_line['is_header']:
            sections['modified'].add(line_num)
        else:
            section = find_containing_section(line_num, all_headers)
            if section:
                sections['modified'].add(section)
    
    # Process deleted lines - use base hierarchy to find deleted sections
    for deleted_line in operations['deleted_lines']:
        if deleted_line['is_header']:
            # Find this header in the base file hierarchy (before deletion)
            deleted_title = clean_title_for_matching(deleted_line['content'])
            # Use base hierarchy if available, otherwise fall back to current headers
            search_hierarchy = base_hierarchy_dict if base_hierarchy_dict else all_headers
            
            found_deleted = False
            for line_num, hierarchy_line in search_hierarchy.items():
                # Extract title from hierarchy line
                if ' > ' in hierarchy_line:
                    original_title = clean_title_for_matching(hierarchy_line.split(' > ')[-1])
                else:
                    original_title = clean_title_for_matching(hierarchy_line)
                
                if deleted_title == original_title:
                    sections['deleted'].add(line_num)
                    print(f"   🗑️  Detected deleted section: {deleted_line['content']} (line {line_num})")
                    found_deleted = True
                    break
            
            if not found_deleted:
                # If not found by exact match, try partial matching for renamed sections
                print(f"   ⚠️  Could not find deleted section: {deleted_line['content']}")
    
    return sections


def get_target_hierarchy_and_content(file_path, github_client, target_repo):
    """Get target hierarchy and content"""
    try:
        repository = github_client.get_repo(target_repo)
        file_content = repository.get_contents(file_path, ref="master").decoded_content.decode('utf-8')
        lines = file_content.split('\n')
        
        # Build hierarchy using same method
        hierarchy = build_hierarchy_dict(file_content)
        
        return hierarchy, lines
    except Exception as e:
        print(f"   ❌ Error getting target file: {e}")
        return {}, []

def get_source_sections_content(pr_url, file_path, source_affected, github_client):
    """Get the content of source sections for better context"""
    try:
        owner, repo, pr_number = parse_pr_url(pr_url)
        repository = github_client.get_repo(f"{owner}/{repo}")
        pr = repository.get_pull(pr_number)
        
        # Get the source file content
        file_content = repository.get_contents(file_path, ref=pr.head.sha).decoded_content.decode('utf-8')
        lines = file_content.split('\n')
        
        # Extract source sections
        source_sections = {}
        
        for line_num, hierarchy in source_affected.items():
            if line_num == "0" and hierarchy == "frontmatter":
                # Special handling for frontmatter
                frontmatter_content = extract_frontmatter_content(lines)
                if frontmatter_content:
                    source_sections[line_num] = frontmatter_content
            else:
                line_number = int(line_num)
                section_content = extract_section_content(lines, line_number, source_affected)
                if section_content:
                    source_sections[line_num] = section_content
        
        return source_sections
    except Exception as e:
        thread_safe_print(f"   ⚠️  Could not get source sections: {e}")
        return {}

def get_source_file_hierarchy(file_path, pr_url, github_client, get_base_version=False):
    """Get source file hierarchy from PR head or base"""
    try:
        owner, repo, pr_number = parse_pr_url(pr_url)
        repository = github_client.get_repo(f"{owner}/{repo}")
        pr = repository.get_pull(pr_number)
        
        if get_base_version:
            # Get the source file content before PR changes (base version)
            source_file_content = repository.get_contents(file_path, ref=pr.base.sha).decoded_content.decode('utf-8')
        else:
            # Get the source file content after PR changes (head version)
            source_file_content = repository.get_contents(file_path, ref=pr.head.sha).decoded_content.decode('utf-8')
            
        source_hierarchy = build_hierarchy_dict(source_file_content)
        
        return source_hierarchy
        
    except Exception as e:
        thread_safe_print(f"   ❌ Error getting source file hierarchy: {e}")
        return {}

# Helper function needed for find_sections_by_operation_type
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

def find_previous_section_for_added(added_sections, hierarchy_dict):
    """Find the previous section hierarchy for each added section group"""
    insertion_points = {}
    
    if not added_sections:
        return insertion_points
    
    # Group consecutive added sections
    added_list = sorted(list(added_sections))
    groups = []
    current_group = [added_list[0]]
    
    for i in range(1, len(added_list)):
        if added_list[i] - added_list[i-1] <= 10:  # Consider sections within 10 lines as consecutive
            current_group.append(added_list[i])
        else:
            groups.append(current_group)
            current_group = [added_list[i]]
    groups.append(current_group)
    
    # For each group, find the previous section hierarchy
    for group in groups:
        first_new_section = min(group)
        
        # Find the section that comes before this group
        previous_section_line = None
        previous_section_hierarchy = None
        
        for line_num_str in sorted(hierarchy_dict.keys(), key=int):
            line_num = int(line_num_str)
            if line_num < first_new_section:
                previous_section_line = line_num
                previous_section_hierarchy = hierarchy_dict[line_num_str]
            else:
                break
        
        if previous_section_hierarchy:
            insertion_points[f"group_{groups.index(group)}"] = {
                'previous_section_hierarchy': previous_section_hierarchy,
                'previous_section_line': previous_section_line,
                'new_sections': group,
                'insertion_type': 'multiple' if len(group) > 1 else 'single'
            }
            print(f"   📍 Added section group: {len(group)} sections after '{previous_section_hierarchy}'")
        else:
            print(f"   ⚠️  Could not find previous section for added sections starting at line {first_new_section}")
    
    return insertion_points

def build_source_diff_dict(modified_sections, added_sections, deleted_sections, all_hierarchy_dict, base_hierarchy_dict, operations, file_content, base_file_content):
    """Build source diff dictionary with correct structure for matching"""
    from section_matcher import clean_title_for_matching
    source_diff_dict = {}
    
    # Helper function to extract section content (only direct content, no sub-sections)
    def extract_section_content_for_diff(line_num, hierarchy_dict):
        if str(line_num) == "0":
            # Handle frontmatter
            return extract_frontmatter_content(file_content.split('\n'))
        else:
            return extract_section_direct_content(file_content.split('\n'), line_num)
    
    # Helper function to extract old content from base file (only direct content, no sub-sections)
    def extract_old_content_for_diff(line_num, base_hierarchy_dict, base_file_content):
        if str(line_num) == "0":
            # Handle frontmatter from base file
            return extract_frontmatter_content(base_file_content.split('\n'))
        else:
            return extract_section_direct_content(base_file_content.split('\n'), line_num)
    
    # Helper function to extract old content by hierarchy (for modified sections that may have moved)
    def extract_old_content_by_hierarchy(original_hierarchy, base_hierarchy_dict, base_file_content):
        """Extract old content by finding the section with matching hierarchy in base file (only direct content)"""
        if original_hierarchy == "frontmatter":
            return extract_frontmatter_content(base_file_content.split('\n'))
        
        # Find the line number in base file that matches the original hierarchy
        for base_line_num_str, base_hierarchy in base_hierarchy_dict.items():
            if base_hierarchy == original_hierarchy:
                base_line_num = int(base_line_num_str) if base_line_num_str != "0" else 0
                if base_line_num == 0:
                    return extract_frontmatter_content(base_file_content.split('\n'))
                else:
                    return extract_section_direct_content(base_file_content.split('\n'), base_line_num)
        
        # If exact match not found, return empty string
        print(f"   ⚠️  Could not find matching hierarchy in base file: {original_hierarchy}")
        return ""
    
    # Helper function to build complete hierarchy for a section using base file info
    def build_complete_original_hierarchy(line_num, current_hierarchy, base_hierarchy_dict, operations):
        """Build complete hierarchy path for original section"""
        line_num_str = str(line_num)
        
        # Special cases: frontmatter and top-level titles
        if line_num_str == "0":
            return "frontmatter"
        
        # Check if this line was modified and has original content
        for modified_line in operations.get('modified_lines', []):
            if (modified_line.get('is_header') and 
                modified_line.get('line_number') == line_num and 
                'original_content' in modified_line):
                original_line = modified_line['original_content'].strip()
                
                # For top-level titles, return the original content directly
                if ' > ' not in current_hierarchy:
                    return original_line
                
                # For nested sections, build the complete hierarchy using original content
                # Find the hierarchy path using base hierarchy dict and replace the leaf with original
                if line_num_str in base_hierarchy_dict:
                    base_hierarchy = base_hierarchy_dict[line_num_str]
                    if ' > ' in base_hierarchy:
                        # Replace the leaf (last part) with original content
                        hierarchy_parts = base_hierarchy.split(' > ')
                        hierarchy_parts[-1] = original_line
                        return ' > '.join(hierarchy_parts)
                    else:
                        # Single level, return original content
                        return original_line
                
                # Fallback: return original content
                return original_line
        
        # If not modified, use base hierarchy if available
        if line_num_str in base_hierarchy_dict:
            return base_hierarchy_dict[line_num_str]
        
        # If not found in base (new section), use current hierarchy
        return current_hierarchy
    
    # Process modified sections
    for line_num_str, hierarchy in modified_sections.items():
        line_num = int(line_num_str) if line_num_str != "0" else 0
        
        # Build complete original hierarchy
        original_hierarchy = build_complete_original_hierarchy(line_num, hierarchy, base_hierarchy_dict, operations)
        
        # Extract both old and new content
        new_content = extract_section_content_for_diff(line_num, all_hierarchy_dict)
        # Use hierarchy-based lookup for old content instead of line number
        old_content = extract_old_content_by_hierarchy(original_hierarchy, base_hierarchy_dict, base_file_content)
        
        # Only include if content actually changed
        if new_content != old_content:
            # Check if this is a bottom modified section (no next section in base file)
            is_bottom_modified = False
            if line_num_str in base_hierarchy_dict:
                # Get all sections in base file sorted by line number
                base_sections = sorted([(int(ln), hier) for ln, hier in base_hierarchy_dict.items() if ln != "0"])
                
                # Check if there's any section after this line in base file
                has_next_section = any(base_line > line_num for base_line, _ in base_sections)
                
                if not has_next_section:
                    is_bottom_modified = True
                    print(f"   ✅ Bottom modified section detected at line {line_num_str}: no next section in base file")
            
            # Use special marker for bottom modified sections
            if is_bottom_modified:
                final_original_hierarchy = f"bottom-modified-{line_num}"
            else:
                final_original_hierarchy = original_hierarchy
            
            source_diff_dict[f"modified_{line_num_str}"] = {
                "new_line_number": line_num,
                "original_hierarchy": final_original_hierarchy,
                "operation": "modified",
                "new_content": new_content,
                "old_content": old_content
            }
            print(f"   ✅ Real modification detected at line {line_num_str}: content changed")
        else:
            print(f"   🚫 Filtered out false positive at line {line_num_str}: content unchanged (likely line shift artifact)")
    
    # Process added sections - find next section from current document hierarchy
    for line_num_str, hierarchy in added_sections.items():
        line_num = int(line_num_str)
        
        print(f"   🔍 Finding next section for added section at line {line_num}: {hierarchy}")
        
        # Strategy: Find the next section directly from the current document (post-PR)
        # Get all current sections sorted by line number
        current_sections = sorted([(int(ln), curr_hierarchy) for ln, curr_hierarchy in all_hierarchy_dict.items()])
        print(f"   📋 Current sections around line {line_num}: {[(ln, h.split(' > ')[-1] if ' > ' in h else h) for ln, h in current_sections if abs(ln - line_num) <= 15]}")
        
        next_section_original_hierarchy = None
        
        # Find the next section that comes after the added section in the current document
        for curr_line_num, curr_hierarchy in current_sections:
            if curr_line_num > line_num:
                # Found the next section in current document
                # Now find its original hierarchy in base document
                curr_line_str = str(curr_line_num)
                
                # Get the original hierarchy for this next section
                # Use the same logic as build_complete_original_hierarchy to get original content
                if curr_line_str in base_hierarchy_dict:
                    # Check if this section was modified
                    was_modified = False
                    for modified_line in operations.get('modified_lines', []):
                        if (modified_line.get('is_header') and 
                            modified_line.get('line_number') == curr_line_num and 
                            'original_content' in modified_line):
                            # This section was modified, use original content
                            original_line = modified_line['original_content'].strip()
                            base_hierarchy = base_hierarchy_dict[curr_line_str]
                            
                            if ' > ' in base_hierarchy:
                                # Replace the leaf with original content
                                hierarchy_parts = base_hierarchy.split(' > ')
                                hierarchy_parts[-1] = original_line
                                next_section_original_hierarchy = ' > '.join(hierarchy_parts)
                            else:
                                next_section_original_hierarchy = original_line
                            
                            print(f"   ✅ Found next section (modified): line {curr_line_num} -> {next_section_original_hierarchy.split(' > ')[-1] if ' > ' in next_section_original_hierarchy else next_section_original_hierarchy}")
                            was_modified = True
                            break
                    
                    if not was_modified:
                        # Section was not modified, use base hierarchy directly
                        next_section_original_hierarchy = base_hierarchy_dict[curr_line_str]
                        print(f"   ✅ Found next section (unchanged): line {curr_line_num} -> {next_section_original_hierarchy.split(' > ')[-1] if ' > ' in next_section_original_hierarchy else next_section_original_hierarchy}")
                    
                    break
                else:
                    # This next section might also be new or modified
                    # Try to find it by content matching in base hierarchy
                    found_match = False
                    for base_line_str, base_hierarchy in base_hierarchy_dict.items():
                        # Compare the leaf titles (last part of hierarchy)
                        curr_leaf = curr_hierarchy.split(' > ')[-1] if ' > ' in curr_hierarchy else curr_hierarchy
                        base_leaf = base_hierarchy.split(' > ')[-1] if ' > ' in base_hierarchy else base_hierarchy
                        
                        # Clean titles for comparison
                        curr_clean = clean_title_for_matching(curr_leaf)
                        base_clean = clean_title_for_matching(base_leaf)
                        
                        if curr_clean == base_clean:
                            next_section_original_hierarchy = base_hierarchy
                            print(f"   ✅ Found next section (by content): {base_hierarchy.split(' > ')[-1] if ' > ' in base_hierarchy else base_hierarchy}")
                            found_match = True
                            break
                    
                    if found_match:
                        break
                    else:
                        print(f"   ⚠️  Next section at line {curr_line_num} not found in base, continuing search...")
        
        # If no next section found, this is being added at the end
        if not next_section_original_hierarchy:
            print(f"   ✅ Bottom section detected: this section is added at the end of document")
            # Use special marker for bottom added sections - no matching needed
            next_section_original_hierarchy = f"bottom-added-{line_num}"
        
        source_diff_dict[f"added_{line_num_str}"] = {
            "new_line_number": line_num,
            "original_hierarchy": next_section_original_hierarchy,
            "operation": "added",
            "new_content": extract_section_content_for_diff(line_num, all_hierarchy_dict),
            "old_content": None  # Added sections have no old content
        }
    
    # Process deleted sections - use original hierarchy from base file
    for line_num_str, hierarchy in deleted_sections.items():
        line_num = int(line_num_str)
        # Use complete hierarchy from base file
        original_hierarchy = base_hierarchy_dict.get(line_num_str, hierarchy)
        
        # Extract old content for deleted sections
        old_content = extract_old_content_for_diff(line_num, base_hierarchy_dict, base_file_content)
        
        source_diff_dict[f"deleted_{line_num_str}"] = {
            "new_line_number": line_num,
            "original_hierarchy": original_hierarchy,
            "operation": "deleted",
            "new_content": None,  # No new content for deleted sections
            "old_content": old_content  # Show what was deleted
        }
    
    # Sort the dictionary by new_line_number for better readability
    sorted_items = sorted(source_diff_dict.items(), key=lambda x: x[1]['new_line_number'])
    source_diff_dict = dict(sorted_items)
    
    return source_diff_dict

def analyze_source_changes(pr_url, github_client, special_files=None, ignore_files=None, repo_configs=None, max_non_system_sections=120, pr_diff=None):
    """Analyze source language changes and categorize them as added, modified, or deleted"""
    # Import modules needed in this function
    import os
    import json
    from toc_processor import process_toc_operations
    from image_processor import is_image_file
    
    owner, repo, pr_number = parse_pr_url(pr_url)
    repository = github_client.get_repo(f"{owner}/{repo}")
    pr = repository.get_pull(pr_number)
    
    # Get repository configuration for target repo info
    repo_config = get_repo_config(pr_url, repo_configs)
    
    print(f"📋 Processing PR #{pr_number}: {pr.title}")
    
    # Get all files
    files = pr.get_files()
    
    # Separate markdown files and image files
    markdown_files = [f for f in files if f.filename.endswith('.md')]
    image_files = [f for f in files if is_image_file(f.filename)]
    
    print(f"📄 Found {len(markdown_files)} markdown files")
    print(f"🖼️  Found {len(image_files)} image files")
    
    # Return dictionaries for different operation types
    added_sections = {}      # New sections that were added
    modified_sections = {}   # Existing sections that were modified  
    deleted_sections = {}    # Sections that were deleted
    added_files = {}         # Completely new files that were added
    deleted_files = []       # Completely deleted files
    ignored_files = []       # Files that were ignored
    toc_files = {}           # Special TOC files requiring special processing
    
    # Image-related returns
    added_images = []        # New image files that were added
    modified_images = []     # Image files that were modified
    deleted_images = []      # Image files that were deleted
    
    for file in markdown_files:
        print(f"\n🔍 Analyzing {file.filename}")
        
        # Check if this file should be ignored
        if file.filename in ignore_files:
            print(f"   ⏭️  Skipping ignored file: {file.filename}")
            ignored_files.append(file.filename)
            continue
        
        # Check if this is a completely new file or deleted file
        if file.status == 'added':
            print(f"   ➕ Detected new file: {file.filename}")
            try:
                file_content = repository.get_contents(file.filename, ref=pr.head.sha).decoded_content.decode('utf-8')
                added_files[file.filename] = file_content
                print(f"   ✅ Added complete file for translation")
                continue
            except Exception as e:
                print(f"   ❌ Error getting new file content: {e}")
                continue
        
        elif file.status == 'removed':
            print(f"   🗑️  Detected deleted file: {file.filename}")
            deleted_files.append(file.filename)
            print(f"   ✅ Marked file for deletion")
            continue
        
        # For modified files, check if it's a special file like TOC.md
        try:
            file_content = repository.get_contents(file.filename, ref=pr.head.sha).decoded_content.decode('utf-8')
        except Exception as e:
            print(f"   ❌ Error getting content: {e}")
            continue
        
        # Check if this is a TOC.md file requiring special processing
        if os.path.basename(file.filename) in special_files:
            print(f"   📋 Detected special file: {file.filename}")
            
            # Get target file content for comparison
            try:
                target_repository = github_client.get_repo(repo_config['target_repo'])
                target_file_content = target_repository.get_contents(file.filename, ref="master").decoded_content.decode('utf-8')
                target_lines = target_file_content.split('\n')
            except Exception as e:
                print(f"   ⚠️  Could not get target file content: {e}")
                continue
            
            # Analyze diff operations for TOC.md
            operations = analyze_diff_operations(file)
            source_lines = file_content.split('\n')
            
            # Process with special TOC logic
            toc_results = process_toc_operations(file.filename, operations, source_lines, target_lines, "")  # Local path will be determined later
            
            # Store TOC operations for later processing
            if any([toc_results['added'], toc_results['modified'], toc_results['deleted']]):
                # Combine all operations for processing
                all_toc_operations = []
                all_toc_operations.extend(toc_results['added'])
                all_toc_operations.extend(toc_results['modified']) 
                all_toc_operations.extend(toc_results['deleted'])
                
                # Add to special TOC processing queue (separate from regular sections)
                toc_files[file.filename] = {
                    'type': 'toc',
                    'operations': all_toc_operations
                }
                
                print(f"   📋 TOC operations queued for processing:")
                if toc_results['added']:
                    print(f"      ➕ Added: {len(toc_results['added'])} entries")
                if toc_results['modified']:
                    print(f"      ✏️  Modified: {len(toc_results['modified'])} entries") 
                if toc_results['deleted']:
                    print(f"      ❌ Deleted: {len(toc_results['deleted'])} entries")
            else:
                print(f"   ℹ️  No TOC operations found")
            
            continue  # Skip regular processing for TOC files
        
        # Analyze diff operations
        operations = analyze_diff_operations(file)
        print(f"   📝 Diff analysis: {len(operations['added_lines'])} added, {len(operations['modified_lines'])} modified, {len(operations['deleted_lines'])} deleted lines")
        
        lines = file_content.split('\n')
        all_headers = {}
        
        # Track code block state
        in_code_block = False
        code_block_delimiter = None
        
        # First pass: collect all headers (excluding those in code blocks)
        for line_num, line in enumerate(lines, 1):
            original_line = line
            line = line.strip()
            
            # Check for code block delimiters
            if line.startswith('```') or line.startswith('~~~'):
                if not in_code_block:
                    # Entering a code block
                    in_code_block = True
                    code_block_delimiter = line[:3]
                    continue
                elif line.startswith(code_block_delimiter):
                    # Exiting a code block
                    in_code_block = False
                    code_block_delimiter = None
                    continue
            
            # Skip processing if we're inside a code block
            if in_code_block:
                continue
            
            # Process headers only if not in code block
            if line.startswith('#'):
                match = re.match(r'^(#{1,10})\s+(.+)', line)
                if match:
                    level = len(match.group(1))
                    title = match.group(2).strip()
                    all_headers[line_num] = {
                        'level': level,
                        'title': title,
                        'line': line
                    }
        
        # Build complete hierarchy from HEAD (after changes)
        all_hierarchy_dict = build_hierarchy_dict(file_content)
        
        # For deletion detection, we also need the base file hierarchy
        try:
            base_file_content = repository.get_contents(file.filename, ref=f"{repository.default_branch}").decoded_content.decode('utf-8')
            base_hierarchy_dict = build_hierarchy_dict(base_file_content)
        except Exception as e:
            print(f"   ⚠️  Could not get base file content: {e}")
            base_hierarchy_dict = all_hierarchy_dict
            base_file_content = file_content  # Fallback to current content
        
        # Find sections by operation type with corrected logic
        sections_by_type = find_sections_by_operation_type(lines, operations, all_headers, base_hierarchy_dict)
        
        # Prioritize modified headers over added ones (fix for header changes like --host -> --hosts)
        modified_header_lines = set()
        for modified_line in operations['modified_lines']:
            if modified_line['is_header']:
                modified_header_lines.add(modified_line['line_number'])
        
        # Remove modified header lines from added set
        sections_by_type['added'] = sections_by_type['added'] - modified_header_lines
        
        # Enhanced logic: check for actual content changes within sections
        # This helps detect changes in section content (not just headers)
        print(f"   🔍 Enhanced detection: checking for actual section content changes...")
        
        # Get only lines that have actual content changes (exclude headers)
        real_content_changes = set()
        
        # Added lines (new content, excluding headers)
        for added_line in operations['added_lines']:
            if not added_line['is_header']:
                real_content_changes.add(added_line['line_number'])
        
        # Deleted lines (removed content, excluding headers)
        for deleted_line in operations['deleted_lines']:
            if not deleted_line['is_header']:
                real_content_changes.add(deleted_line['line_number'])
        
        # Modified lines (changed content, excluding headers)
        for modified_line in operations['modified_lines']:
            if not modified_line['is_header']:
                real_content_changes.add(modified_line['line_number'])
        
        print(f"   📝 Real content changes (non-header): {sorted(real_content_changes)}")
        
        # Find sections that contain actual content changes
        content_affected_sections = set()
        for changed_line in real_content_changes:
            # Find which section this changed line belongs to
            containing_section = None
            for line_num in sorted(all_headers.keys()):
                if line_num <= changed_line:
                    containing_section = line_num
                else:
                    break
            
            if containing_section and containing_section not in sections_by_type['added']:
                # Additional check: make sure this is not just a line number shift
                # Only add if the change is within reasonable distance from the section header
                # AND if the changed line is not part of a completely deleted section header
                is_deleted_header = False
                for deleted_line in operations['deleted_lines']:
                    if (deleted_line['is_header'] and 
                        abs(changed_line - deleted_line['line_number']) <= 2):
                        is_deleted_header = True
                        print(f"   ⚠️  Skipping change at line {changed_line} (deleted header near line {deleted_line['line_number']})")
                        break
                
                # More precise filtering: check if this change is actually meaningful
                # Skip changes that are part of deleted content or line shifts due to deletions
                should_include = True
                
                # Skip exact deleted headers
                for deleted_line in operations['deleted_lines']:
                    if (deleted_line['is_header'] and 
                        changed_line == deleted_line['line_number']):
                        should_include = False
                        print(f"   ⚠️  Skipping change at line {changed_line} (exact deleted header)")
                        break
                
                # Skip changes that are very close to deleted content AND far from their containing section
                # This helps filter out line shift artifacts while keeping real content changes
                if should_include:
                    for deleted_line in operations['deleted_lines']:
                        # Only skip if both conditions are met:
                        # 1. Very close to deleted content (within 5 lines)
                        # 2. The change is far from its containing section (likely a shift artifact)
                        distance_to_deletion = abs(changed_line - deleted_line['line_number'])
                        distance_to_section = changed_line - containing_section
                        
                        if (distance_to_deletion <= 5 and distance_to_section > 100):
                            should_include = False
                            print(f"   ⚠️  Skipping change at line {changed_line} (likely line shift: {distance_to_deletion} lines from deletion, {distance_to_section} from section)")
                            break
                
                if should_include and changed_line - containing_section <= 30:
                    content_affected_sections.add(containing_section)
                    print(f"   📝 Content change at line {changed_line} affects section at line {containing_section}")
                elif should_include:
                    print(f"   ⚠️  Skipping distant change at line {changed_line} from section {containing_section}")
        
        # Add content-modified sections to the modified set, but exclude sections that are already marked as added or deleted
        for line_num in content_affected_sections:
            if (line_num not in sections_by_type['modified'] and 
                line_num not in sections_by_type['added'] and
                line_num not in sections_by_type['deleted']):  # ✅ Critical fix: exclude deleted sections      
                sections_by_type['modified'].add(line_num)
                print(f"   📝 Added content-modified section at line {line_num}")
            elif line_num in sections_by_type['deleted']:
                print(f"   🚫 Skipping content-modified section at line {line_num}: already marked as deleted")
        
        # Prepare sections data for source_diff_dict
        file_modified = {}
        file_added = {}
        file_deleted = {}
        
        # Build modified sections
        for line_num in sections_by_type['modified']:
            if line_num in all_hierarchy_dict:
                file_modified[str(line_num)] = all_hierarchy_dict[line_num]
        
        # Build added sections  
        for line_num in sections_by_type['added']:
            if line_num in all_hierarchy_dict:
                file_added[str(line_num)] = all_hierarchy_dict[line_num]
        
        # Build deleted sections
        for line_num in sections_by_type['deleted']:
            if line_num in base_hierarchy_dict:
                file_deleted[str(line_num)] = base_hierarchy_dict[line_num]
        
        # Check for frontmatter changes (content before first top-level header)
        print(f"   🔍 Checking for frontmatter changes...")
        frontmatter_changed = False
        
        # Check if any changes occur before the first top-level header
        first_header_line = None
        for line_num in sorted(all_headers.keys()):
            header_info = all_headers[line_num]
            if header_info['level'] == 1:  # First top-level header
                first_header_line = line_num
                break
        
        print(f"   📊 First header line: {first_header_line}")
        print(f"   📊 Real content changes: {sorted(real_content_changes)}")
        
        if first_header_line:
            # Check if any real content changes are before the first header
            for line_num in real_content_changes:
                #print(f"   🔍 Checking line {line_num} vs first header {first_header_line}")
                if line_num < first_header_line:
                    frontmatter_changed = True
                    print(f"   📄 Frontmatter change detected: line {line_num} < {first_header_line}")
                    break
        
        print(f"   📊 Frontmatter changed: {frontmatter_changed}")
        
        if frontmatter_changed:
            print(f"   📄 Frontmatter changes detected (before line {first_header_line})")
            # Add frontmatter as a special section with line number 0
            file_modified["0"] = "frontmatter"
            print(f"   ✅ Added frontmatter section to modified sections")
        
        # Build source diff dictionary
        source_diff_dict = build_source_diff_dict(
            file_modified, file_added, file_deleted, 
            all_hierarchy_dict, base_hierarchy_dict, 
            operations, file_content, base_file_content
        )
        
        # Breakpoint: Output source_diff_dict to file for review with file prefix
        
        # Ensure temp_output directory exists
        script_dir = os.path.dirname(os.path.abspath(__file__))
        temp_dir = os.path.join(script_dir, "temp_output")
        os.makedirs(temp_dir, exist_ok=True)
        
        file_prefix = file.filename.replace('/', '-').replace('.md', '')
        output_file = os.path.join(temp_dir, f"{file_prefix}-source-diff-dict.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(source_diff_dict, f, ensure_ascii=False, indent=2)
        
        print(f"   💾 Saved source diff dictionary to: {output_file}")
        print(f"   📊 Source diff dictionary contains {len(source_diff_dict)} sections:")
        for key, diff_info in source_diff_dict.items():
            print(f"      {diff_info['operation']}: {key} -> original_hierarchy: {diff_info['original_hierarchy']}")
        
        # source-diff-dict.json generation is complete, continue to next step in main.py
        
        # For modified headers, we need to build a mapping using original titles for matching
        original_hierarchy_dict = all_hierarchy_dict.copy()
        
        # Update hierarchy dict to use original content for modified headers when needed for matching
        for line_num in sections_by_type['modified']:
            if line_num in all_headers:
                header_info = all_headers[line_num]
                # Check if this header was modified and has original content
                for op in operations['modified_lines']:
                    if (op['is_header'] and 
                        op['line_number'] == line_num and 
                        'original_content' in op):
                        # Create hierarchy path using original content for matching
                        original_line = op['original_content'].strip()
                        if original_line.startswith('#'):
                            # Build original hierarchy for matching
                            original_hierarchy = build_hierarchy_for_modified_section(
                                file_content, line_num, original_line, all_hierarchy_dict)
                            if original_hierarchy:
                                original_hierarchy_dict[line_num] = original_hierarchy
                        break
        
        # Process added sections
        if sections_by_type['added']:
            file_added = {}
            # Find insertion points using the simplified logic: 
            # Record the previous section hierarchy for each added section
            insertion_points = find_previous_section_for_added(sections_by_type['added'], all_hierarchy_dict)
            
            # Get actual content for added sections
            for line_num in sections_by_type['added']:
                if line_num in all_hierarchy_dict:
                    file_added[str(line_num)] = all_hierarchy_dict[line_num]
            
            # Get source sections content (actual content, not just hierarchy)
            if file_added:
                source_sections_content = get_source_sections_content(pr_url, file.filename, file_added, github_client)
                file_added = source_sections_content  # Replace hierarchy with actual content
            
            if file_added:
                added_sections[file.filename] = {
                    'sections': file_added,
                    'insertion_points': insertion_points
                }
                print(f"   ➕ Found {len(file_added)} added sections with {len(insertion_points)} insertion points")
        
        # Process modified sections
        if sections_by_type['modified']:
            file_modified = {}
            for line_num in sections_by_type['modified']:
                if line_num in original_hierarchy_dict:
                    file_modified[str(line_num)] = original_hierarchy_dict[line_num]
            
            if file_modified:
                modified_sections[file.filename] = {
                    'sections': file_modified,
                    'original_hierarchy': original_hierarchy_dict,
                    'current_hierarchy': all_hierarchy_dict
                }
                print(f"   ✏️  Found {len(file_modified)} modified sections")
        
        # Process deleted sections  
        if sections_by_type['deleted']:
            file_deleted = {}
            for line_num in sections_by_type['deleted']:
                # Use base hierarchy to get the deleted section info
                if line_num in base_hierarchy_dict:
                    file_deleted[str(line_num)] = base_hierarchy_dict[line_num]
            
            if file_deleted:
                deleted_sections[file.filename] = file_deleted
                print(f"   ❌ Found {len(file_deleted)} deleted sections")
        
        # Enhanced logic: also check content-level changes using legacy detection
        # This helps detect changes in section content (not just headers)
        print(f"   🔍 Enhanced detection: checking content-level changes...")
        changed_lines = get_changed_line_ranges(file)
        affected_sections = find_affected_sections(lines, changed_lines, all_headers)
        
        legacy_modified = {}
        for line_num in affected_sections:
            if line_num in all_hierarchy_dict:
                section_hierarchy = all_hierarchy_dict[line_num]
                # Only add if not already detected by operation-type analysis
                already_detected = False
                if file.filename in modified_sections:
                    for existing_line, existing_hierarchy in modified_sections[file.filename].get('sections', {}).items():
                        if existing_hierarchy == section_hierarchy:
                            already_detected = True
                            break
                
                if not already_detected:
                    legacy_modified[str(line_num)] = section_hierarchy
        
        if legacy_modified:
            print(f"   ✅ Enhanced detection found {len(legacy_modified)} additional content-modified sections")
            # Merge with existing modified sections
            if file.filename in modified_sections:
                # Merge the sections
                existing_sections = modified_sections[file.filename].get('sections', {})
                existing_sections.update(legacy_modified)
                modified_sections[file.filename]['sections'] = existing_sections
            else:
                # Create new entry
                modified_sections[file.filename] = {
                    'sections': legacy_modified,
                    'original_hierarchy': all_hierarchy_dict,
                    'current_hierarchy': all_hierarchy_dict
                }
    
    # Process image files
    print(f"\n🖼️  Analyzing image files...")
    for file in image_files:
        print(f"\n🔍 Analyzing image: {file.filename}")
        
        # Check if this file should be ignored
        if file.filename in ignore_files:
            print(f"   ⏭️  Skipping ignored image: {file.filename}")
            ignored_files.append(file.filename)
            continue
        
        # Categorize image operations based on file status
        if file.status == 'added':
            print(f"   ➕ Detected new image: {file.filename}")
            added_images.append(file.filename)
        elif file.status == 'removed':
            print(f"   🗑️  Detected deleted image: {file.filename}")
            deleted_images.append(file.filename)
        elif file.status == 'modified':
            print(f"   🔄 Detected modified image: {file.filename}")
            modified_images.append(file.filename)
        elif file.status == 'renamed':
            # Renamed images are treated as delete old + add new
            print(f"   🔄 Detected renamed image: {file.previous_filename} -> {file.filename}")
            if hasattr(file, 'previous_filename') and file.previous_filename:
                deleted_images.append(file.previous_filename)
            added_images.append(file.filename)
    
    print(f"\n📊 Summary:")
    #print(f"   ✏️  Modified files: {} files") 
    print(f"   📄 Added files: {len(added_files)} files")
    print(f"   🗑️  Deleted files: {len(deleted_files)} files")
    print(f"   📋 TOC files: {len(toc_files)} files")
    print(f"   🖼️  Added images: {len(added_images)} images")
    print(f"   🖼️  Modified images: {len(modified_images)} images")
    print(f"   🖼️  Deleted images: {len(deleted_images)} images")
    if ignored_files:
        print(f"   ⏭️  Ignored files: {len(ignored_files)} files")
        for ignored_file in ignored_files:
            print(f"      - {ignored_file}")
    
    return added_sections, modified_sections, deleted_sections, added_files, deleted_files, toc_files, added_images, modified_images, deleted_images
