"""
Glossary Module
Loads terms from a markdown glossary file and filters relevant terms
for a given markdown content using text matching.

Reference: docs-toolkit/markdown-translator/src/glossary.js
"""

import os
import re
from log_sanitizer import sanitize_exception_message

# Pattern placeholder used in terms.md for version-like patterns
_VERSION_PLACEHOLDER = 'X.X.X'
_VERSION_REGEX = r'\d+\.\d+(?:\.\d+)*'


def _build_term_matcher(text):
    """Build a matcher for a single term string.

    If the term contains 'X.X.X' (version placeholder), return a compiled
    regex that matches actual version numbers.  Otherwise return None,
    indicating plain substring matching should be used.
    """
    if _VERSION_PLACEHOLDER not in text:
        return None
    parts = text.split(_VERSION_PLACEHOLDER)
    escaped = [re.escape(p) for p in parts]
    pattern = _VERSION_REGEX.join(escaped)
    return re.compile(pattern, re.IGNORECASE)


def load_glossary(terms_path):
    """Load glossary terms from a markdown table file.

    Expected format (terms.md):
        <!-- markdownlint-disable MD041 -->
        | en | zh | comments |
        |:---|:---|:---|
        | term1 | 术语1 | comment1 |
        ...

    Returns:
        list of dicts: [{"en": "...", "zh": "...", "comment": "..."}, ...]
    """
    if not terms_path or not os.path.exists(terms_path):
        print(f"   ⚠️  Glossary file not found: {terms_path}")
        return []

    try:
        with open(terms_path, 'r', encoding='utf-8') as f:
            lines = f.read().strip().split('\n')

        # Find the separator line (|:---|:---|...) to skip header rows
        data_start = 0
        for i, line in enumerate(lines):
            if line.strip().startswith('|:---') or line.strip().startswith('| :---'):
                data_start = i + 1
                break

        glossary = []
        for line in lines[data_start:]:
            line = line.strip()
            if not line or not line.startswith('|'):
                continue
            columns = [col.strip() for col in line.split('|')]
            # After splitting "| a | b | c |", we get ['', ' a ', ' b ', ' c ', '']
            columns = [c for c in columns if c != '']
            if len(columns) >= 2:
                entry = {
                    'en': columns[0],
                    'zh': columns[1],
                    'comment': columns[2] if len(columns) >= 3 else ''
                }
                if entry['en'] and entry['zh']:
                    glossary.append(entry)

        print(f"   📚 Loaded {len(glossary)} glossary terms from {terms_path}")
        return glossary

    except Exception as e:
        print(f"   ❌ Error loading glossary: {sanitize_exception_message(e)}")
        return []


def create_glossary_matcher(glossary):
    """Create a matcher function from the glossary.

    Returns:
        callable or None:
            matcher(text, source_language=None) -> list of matched term dicts

        source_language controls which glossary column is searched:
          - "Chinese"  -> match the zh column against the text
          - "English"  -> match the en column against the text
          - None       -> match both columns (fallback)
    """
    if not glossary:
        return None

    MIN_EN_LEN = 2
    MIN_ZH_LEN = 2

    valid_terms = []
    for entry in glossary:
        en = entry['en']
        zh = entry['zh']
        if len(en) < MIN_EN_LEN and len(zh) < MIN_ZH_LEN:
            continue
        valid_terms.append({
            'en': en,
            'en_lower': en.lower(),
            'en_regex': _build_term_matcher(en),
            'zh': zh,
            'zh_regex': _build_term_matcher(zh),
            'comment': entry.get('comment', ''),
        })

    # Longer terms first so more specific matches take priority
    valid_terms.sort(key=lambda x: max(len(x['en']), len(x['zh'])), reverse=True)

    def matcher(text, source_language=None):
        if not text:
            return []

        text_lower = text.lower()
        matched = []
        seen_en = set()

        for term in valid_terms:
            if term['en_lower'] in seen_en:
                continue

            hit = False

            # Determine which columns to check based on source_language
            check_en = source_language in (None, "English")
            check_zh = source_language in (None, "Chinese")

            # --- English column ---
            if not hit and check_en and len(term['en']) >= MIN_EN_LEN:
                if term['en_regex']:
                    if term['en_regex'].search(text):
                        hit = True
                elif term['en_lower'] in text_lower:
                    hit = True

            # --- Chinese column ---
            if not hit and check_zh and len(term['zh']) >= MIN_ZH_LEN:
                if term['zh_regex']:
                    if term['zh_regex'].search(text):
                        hit = True
                elif term['zh'] in text:
                    hit = True

            if hit:
                seen_en.add(term['en_lower'])
                matched.append({
                    'en': term['en'],
                    'zh': term['zh'],
                    'comment': term['comment']
                })

        return matched

    print(f"   📚 Created glossary matcher with {len(valid_terms)} terms")
    return matcher


def filter_terms_for_content(glossary_matcher, *content_parts, source_language=None):
    """Match glossary terms against one or more content strings.

    Args:
        glossary_matcher: matcher function from create_glossary_matcher
        *content_parts: one or more strings to match against
        source_language: "Chinese" | "English" | None
            Controls which glossary column is searched.
            - "Chinese"  -> match zh column (for ZH->EN translation)
            - "English"  -> match en column (for EN->ZH translation)
            - None       -> match both columns

    Returns:
        list of matched term dicts (deduplicated), or empty list
    """
    if glossary_matcher is None:
        return []

    combined = '\n'.join(str(part) for part in content_parts if part)
    if not combined.strip():
        return []

    return glossary_matcher(combined, source_language=source_language)


def format_terms_for_prompt(matched_terms):
    """Format matched terms as a markdown table for inclusion in AI prompt.

    Returns:
        str: formatted glossary section, or empty string if no terms
    """
    if not matched_terms:
        return ""

    lines = [
        "Glossary - Use the following term translations for consistency:",
        "| English | Chinese | Comment |",
        "|:---|:---|:---|",
    ]
    for term in matched_terms:
        comment = term.get('comment', '')
        lines.append(f"| {term['en']} | {term['zh']} | {comment} |")

    return '\n'.join(lines)
