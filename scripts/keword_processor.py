"""
Keyword Processor Module
Handles special processing logic for keywords.md files.

keywords.md uses letter-based blocks under <TabsPanel>. This module provides:
1) TabsPanel region parsing
2) Letter-block diffing between source base/head
3) AI-driven target block updates for changed letters only
"""

import os
import re
import json
import difflib
import threading

print_lock = threading.Lock()


def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)


LETTER_ANCHOR_RE = re.compile(
    r'<a\s+id="([A-Z])"\s+class="letter"\s+href="#[A-Z]">[A-Z]</a>'
)

KEYWORD_LINE_RE = re.compile(r'^- \S')


def find_tabs_region(lines):
    """Find the TabsPanel region in keywords.md.

    Returns a dict:
      {
        'start_idx': int,   # 0-based inclusive
        'end_idx': int,     # 0-based exclusive
      }
    or None if not found.
    """
    start_idx = None
    for i, line in enumerate(lines):
        if "<TabsPanel" in line:
            start_idx = i
            break

    if start_idx is None:
        return None

    end_idx = len(lines)
    for i in range(start_idx + 1, len(lines)):
        stripped = lines[i].strip()
        # End at next top-level section boundary for this file.
        if stripped.startswith("## ") or stripped.startswith("# "):
            end_idx = i
            break

    return {
        "start_idx": start_idx,
        "end_idx": end_idx,
    }


def parse_letter_blocks(lines, region):
    """Parse letter blocks under TabsPanel.

    Returns:
      {
        'A': {'start_idx': int, 'end_idx': int, 'content': str},
        ...
      }
    """
    if not region:
        return {}

    start_idx = region["start_idx"]
    end_idx = region["end_idx"]

    blocks = {}
    current_letter = None
    current_start = None

    for i in range(start_idx, end_idx):
        line = lines[i]
        m = LETTER_ANCHOR_RE.search(line)
        if not m:
            continue

        letter = m.group(1)
        if current_letter is not None:
            blocks[current_letter] = {
                "start_idx": current_start,
                "end_idx": i,
                "content": "\n".join(lines[current_start:i]),
            }

        current_letter = letter
        current_start = i

    if current_letter is not None and current_start is not None:
        blocks[current_letter] = {
            "start_idx": current_start,
            "end_idx": end_idx,
            "content": "\n".join(lines[current_start:end_idx]),
        }

    return blocks


def _build_letter_diff(letter, old_block, new_block):
    old_lines = (old_block or "").splitlines()
    new_lines = (new_block or "").splitlines()
    return "\n".join(
        difflib.unified_diff(
            old_lines,
            new_lines,
            fromfile=f"{letter}_old",
            tofile=f"{letter}_new",
            lineterm="",
        )
    )


def diff_changed_letters(base_blocks, head_blocks):
    """Find changed letters between source base and source head blocks."""
    changed = {}
    all_letters = sorted(set(base_blocks.keys()) | set(head_blocks.keys()))

    for letter in all_letters:
        old_block = base_blocks.get(letter, {}).get("content")
        new_block = head_blocks.get(letter, {}).get("content")

        old_norm = (old_block or "").strip()
        new_norm = (new_block or "").strip()

        if old_norm == new_norm:
            continue

        changed[letter] = {
            "source_old_block": old_block,
            "source_new_block": new_block,
            "source_diff": _build_letter_diff(letter, old_block, new_block),
        }

    return changed


def extract_keyword_name(line):
    """Extract the keyword name from '- ADD (R)' -> 'ADD'."""
    stripped = line.strip()
    if stripped.startswith('- '):
        rest = stripped[2:].strip()
        return rest.split()[0] if rest else ''
    return ''


def find_insertion_index(existing_keywords, new_keyword_name):
    """Find insertion index for sorted keyword list."""
    for i, kw_line in enumerate(existing_keywords):
        existing_name = extract_keyword_name(kw_line)
        if existing_name.upper() > new_keyword_name.upper():
            return i
    return len(existing_keywords)


def classify_diff_lines(operations):
    """Backward-compatible helper for keyword diff classification."""
    added_by_letter = {}
    deleted_by_letter = {}

    for entry in operations.get('added_lines', []):
        content = entry['content']
        if KEYWORD_LINE_RE.match(content.strip()):
            name = extract_keyword_name(content)
            if name:
                letter = name[0].upper()
                added_by_letter.setdefault(letter, []).append(content.rstrip())

    for entry in operations.get('deleted_lines', []):
        content = entry['content']
        if KEYWORD_LINE_RE.match(content.strip()):
            name = extract_keyword_name(content)
            if name:
                letter = name[0].upper()
                deleted_by_letter.setdefault(letter, []).append(content.rstrip())

    modified_by_letter = {}
    added_names = {}
    for letter, kw_lines in added_by_letter.items():
        for line in kw_lines:
            added_names.setdefault(letter, {})[extract_keyword_name(line)] = line

    deleted_names = {}
    for letter, kw_lines in deleted_by_letter.items():
        for line in kw_lines:
            deleted_names.setdefault(letter, {})[extract_keyword_name(line)] = line

    for letter in set(list(added_names.keys()) + list(deleted_names.keys())):
        a_names = added_names.get(letter, {})
        d_names = deleted_names.get(letter, {})
        common = set(a_names.keys()) & set(d_names.keys())
        for name in common:
            modified_by_letter.setdefault(letter, []).append(
                (d_names[name], a_names[name])
            )

    pure_added = {}
    for letter, kw_lines in added_by_letter.items():
        d_names_set = set(deleted_names.get(letter, {}).keys())
        pure = [l for l in kw_lines if extract_keyword_name(l) not in d_names_set]
        if pure:
            pure_added[letter] = pure

    pure_deleted = {}
    for letter, kw_lines in deleted_by_letter.items():
        a_names_set = set(added_names.get(letter, {}).keys())
        pure = [l for l in kw_lines if extract_keyword_name(l) not in a_names_set]
        if pure:
            pure_deleted[letter] = pure

    return {
        'added': pure_added,
        'deleted': pure_deleted,
        'modified': modified_by_letter,
    }


def process_keyword_operations(file_path, operations, source_lines, target_lines, target_local_path):
    """Backward-compatible wrapper used by older analyzer paths."""
    thread_safe_print("\n   Processing keywords.md with keyword-specific logic...")
    changes = classify_diff_lines(operations)
    thread_safe_print(
        "   Keyword changes: "
        f"{sum(len(v) for v in changes['added'].values())} added, "
        f"{sum(len(v) for v in changes['deleted'].values())} deleted, "
        f"{sum(len(v) for v in changes['modified'].values())} modified"
    )
    return changes


def _extract_json_object(text):
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r'^```(?:json)?\\s*', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'\\s*```$', '', cleaned)

    start = cleaned.find('{')
    end = cleaned.rfind('}')
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON object found in AI response")

    return json.loads(cleaned[start:end + 1])


def _extract_keyword_map(block):
    """Extract keyword-name -> full line mapping from a letter block."""
    if not block:
        return {}

    keyword_map = {}
    for raw_line in block.splitlines():
        stripped = raw_line.strip()
        if not stripped.startswith('- '):
            continue
        name = extract_keyword_name(stripped)
        if not name:
            continue
        keyword_map[name] = stripped
    return keyword_map


def _compute_expected_target_delta(source_old_block, source_new_block, target_old_block):
    """Compute expected effective keyword delta on target block."""
    source_old_map = _extract_keyword_map(source_old_block)
    source_new_map = _extract_keyword_map(source_new_block)
    target_old_map = _extract_keyword_map(target_old_block)

    source_added = set(source_new_map.keys()) - set(source_old_map.keys())
    source_deleted = set(source_old_map.keys()) - set(source_new_map.keys())
    source_modified = {
        name for name in (set(source_old_map.keys()) & set(source_new_map.keys()))
        if source_old_map[name] != source_new_map[name]
    }

    expected_added = {name for name in source_added if name not in target_old_map}
    expected_deleted = {name for name in source_deleted if name in target_old_map}
    allowed_modified = {name for name in source_modified if name in target_old_map}

    return expected_added, expected_deleted, allowed_modified


def _validate_ai_letter_update(letter, payload, updated_block):
    """Validate AI output only applies expected source-derived keyword changes."""
    target_old_block = payload.get("target_old_block") or ""
    source_old_block = payload.get("source_old_block") or ""
    source_new_block = payload.get("source_new_block") or ""

    target_old_map = _extract_keyword_map(target_old_block)
    target_new_map = _extract_keyword_map(updated_block)

    actual_added = set(target_new_map.keys()) - set(target_old_map.keys())
    actual_deleted = set(target_old_map.keys()) - set(target_new_map.keys())
    actual_modified = {
        name for name in (set(target_old_map.keys()) & set(target_new_map.keys()))
        if target_old_map[name] != target_new_map[name]
    }

    expected_added, expected_deleted, allowed_modified = _compute_expected_target_delta(
        source_old_block,
        source_new_block,
        target_old_block,
    )

    if actual_added != expected_added:
        raise ValueError(
            f"Letter {letter} unexpected added keywords. "
            f"expected={sorted(expected_added)}, actual={sorted(actual_added)}"
        )

    if actual_deleted != expected_deleted:
        raise ValueError(
            f"Letter {letter} unexpected deleted keywords. "
            f"expected={sorted(expected_deleted)}, actual={sorted(actual_deleted)}"
        )

    unexpected_modified = actual_modified - allowed_modified
    if unexpected_modified:
        raise ValueError(
            f"Letter {letter} modified unchanged keywords: {sorted(unexpected_modified)}"
        )


def update_letter_blocks_with_ai(changed_letters_payload, ai_client, repo_config, file_path="keywords.md"):
    """Use AI to update target letter blocks based on source block diff."""
    if not changed_letters_payload:
        return {}

    letters = sorted(changed_letters_payload.keys())
    request_payload = {letter: changed_letters_payload[letter] for letter in letters}

    for letter in letters:
        target_old_block = request_payload[letter].get("target_old_block")
        if not isinstance(target_old_block, str) or not target_old_block.strip():
            raise ValueError(f"Missing target_old_block for letter {letter}")

    source_lang = repo_config.get("source_language", "English")
    target_lang = repo_config.get("target_language", "Chinese")

    prompt = f"""You are updating `{file_path}` blocks under a `<TabsPanel letters=.../>` section.
Source language is {source_lang}. Target language is {target_lang}.

Each key is a LETTER block unit in source/target docs. Input JSON fields per letter:
- source_old_block: source-language block before PR
- source_new_block: source-language block after PR
- source_diff: unified diff from old->new in source
- target_old_block: current target-language block to update

Task:
For each letter, apply the source change pattern to `target_old_block` and return the updated target-language block.

Rules:
1. Return STRICT JSON object only: {{"A": "...", "S": "..."}} with exactly the same keys as input.
2. Keep each letter anchor line exactly valid and matching its key, e.g. `<a id="A" class="letter" href="#A">A</a>`.
3. Preserve markdown list formatting and spacing.
4. Keep SQL keywords, symbols, `(R)` markers, links, and code-like tokens unchanged unless source diff changed them.
5. Do not include any explanation or markdown code fence.
6. Apply only the keyword additions/deletions/modifications implied by source_diff for that letter.
7. Do not add or remove unrelated keywords.

Input JSON:
{json.dumps(request_payload, ensure_ascii=False, indent=2)}
"""

    try:
        response = ai_client.chat_completion(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
    except Exception as e:
        raise ValueError(f"Keyword block AI call failed: {e}")

    parsed = _extract_json_object(response)
    if not isinstance(parsed, dict):
        raise ValueError("AI response JSON is not an object")

    extra = [k for k in parsed.keys() if k not in letters]
    if extra:
        raise ValueError(f"AI response contains unexpected letters: {sorted(extra)}")

    missing = [k for k in letters if k not in parsed]
    if missing:
        raise ValueError(f"AI response missing letters: {missing}")

    updates = {}
    for letter in letters:
        value = parsed.get(letter)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"AI response for letter {letter} is empty or not a string")

        expected_anchor = f'<a id="{letter}" class="letter" href="#{letter}">{letter}</a>'
        if expected_anchor not in value:
            raise ValueError(f"AI response for letter {letter} missing expected anchor")

        _validate_ai_letter_update(letter, request_payload[letter], value)
        updates[letter] = value

    return updates


def _find_insert_index_for_letter(letter, blocks, tabs_region, lines_len):
    """Find insertion index for a new letter block in target lines."""
    if blocks:
        ordered = sorted(blocks.items(), key=lambda x: x[1]["start_idx"])
        for existing_letter, meta in ordered:
            if existing_letter > letter:
                return meta["start_idx"]

        last_letter, last_meta = ordered[-1]
        return last_meta["end_idx"]

    if tabs_region:
        return tabs_region["end_idx"]

    return lines_len


def apply_letter_block_updates(target_lines, target_blocks, ai_updates, tabs_region=None):
    """Apply AI-updated letter blocks to target lines."""
    result_lines = list(target_lines)

    # Replace existing blocks (from bottom to top to avoid index shifts).
    replacements = []
    for letter, new_block in ai_updates.items():
        if letter in target_blocks:
            meta = target_blocks[letter]
            replacements.append((meta["start_idx"], meta["end_idx"], letter, new_block))

    replacements.sort(key=lambda x: x[0], reverse=True)
    for start_idx, end_idx, letter, new_block in replacements:
        new_lines = new_block.split('\n')
        result_lines[start_idx:end_idx] = new_lines

    # Insert blocks that do not exist in target.
    missing_letters = sorted([letter for letter in ai_updates if letter not in target_blocks])
    if missing_letters:
        for letter in missing_letters:
            current_region = find_tabs_region(result_lines)
            current_blocks = parse_letter_blocks(result_lines, current_region)
            insert_idx = _find_insert_index_for_letter(letter, current_blocks, current_region, len(result_lines))

            new_lines = ai_updates[letter].split('\n')
            if insert_idx > 0 and result_lines[insert_idx - 1].strip() != "":
                new_lines = [""] + new_lines
            if insert_idx < len(result_lines) and result_lines[insert_idx].strip() != "":
                new_lines = new_lines + [""]

            result_lines[insert_idx:insert_idx] = new_lines

    return result_lines


def process_keyword_file(file_path, keyword_data, pr_url, github_client, ai_client, repo_config):
    """Apply AI-updated TabsPanel letter changes to a keyword file.

    Returns:
      bool: True if successful, False otherwise.
    """
    thread_safe_print(f"\n   Processing keyword file: {file_path}")

    try:
        target_local_path = repo_config['target_local_path']
        target_file_path = os.path.join(target_local_path, file_path)

        if not os.path.exists(target_file_path):
            thread_safe_print(f"   Target file not found: {target_file_path}")
            return False

        with open(target_file_path, 'r', encoding='utf-8') as f:
            target_content = f.read()

        target_lines = target_content.split('\n')
        tabs_region = find_tabs_region(target_lines)
        if not tabs_region:
            thread_safe_print("   TabsPanel region not found in target file")
            return False

        target_blocks = parse_letter_blocks(target_lines, tabs_region)
        tabs_changes = keyword_data.get('tabs_changes', {})

        if not tabs_changes:
            thread_safe_print("   No TabsPanel letter changes to process")
            return True

        thread_safe_print(f"   TabsPanel changed letters: {sorted(tabs_changes.keys())}")

        ai_updates = update_letter_blocks_with_ai(
            tabs_changes,
            ai_client,
            repo_config,
            file_path=file_path,
        )

        updated_lines = apply_letter_block_updates(
            target_lines,
            target_blocks,
            ai_updates,
            tabs_region=tabs_region,
        )

        with open(target_file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(updated_lines))

        thread_safe_print(f"   Keyword file updated with AI letter blocks: {file_path}")
        return True

    except Exception as e:
        thread_safe_print(f"   Error processing keyword file {file_path}: {e}")
        return False


def process_keyword_files(keyword_files, pr_url, github_client, ai_client, repo_config):
    """Process all keyword files.

    Returns:
      bool: True if all keyword files are processed successfully, else False.
    """
    if not keyword_files:
        return True

    thread_safe_print(f"\n   Processing {len(keyword_files)} keyword files...")

    for file_path, kw_data in keyword_files.items():
        if kw_data.get('type') != 'keyword':
            thread_safe_print(f"   Unknown keyword data type: {kw_data.get('type')} for {file_path}")
            return False

        success = process_keyword_file(
            file_path,
            kw_data,
            pr_url,
            github_client,
            ai_client,
            repo_config,
        )
        if not success:
            thread_safe_print(f"   Keyword file processing failed: {file_path}")
            return False

    thread_safe_print("   All keyword files processed")
    return True
