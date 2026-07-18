"""
Helpers for redacting secrets from exception strings before logging them.
"""

import os
import re


_SECRET_ENV_VARS = (
    "GITHUB_TOKEN",
    "DEEPSEEK_API_TOKEN",
    "GEMINI_API_TOKEN",
    "OPENAI_API_TOKEN",
    "AZURE_OPENAI_KEY",
)

_REDACTION_PATTERNS = (
    (
        re.compile(r'(?i)(authorization\s*:\s*(?:bearer|token)\s+)[^\s,"\']+'),
        r"\1[REDACTED]",
    ),
    (
        re.compile(r"(?i)(x-access-token:)[^@/\s]+"),
        r"\1[REDACTED]",
    ),
    (
        re.compile(r"(https?://[^/\s:@]+:)[^@/\s]+(@)"),
        r"\1[REDACTED]\2",
    ),
    (
        re.compile(
            r"(?i)([?&](?:api[_-]?key|access[_-]?token|refresh[_-]?token|token|password)=)[^&\s]+"
        ),
        r"\1[REDACTED]",
    ),
    (
        re.compile(
            r'(?i)(["\']?(?:api[_-]?key|access[_-]?token|refresh[_-]?token|password|authorization)["\']?\s*[:=]\s*["\']?)[^"\',\s}]+'
        ),
        r"\1[REDACTED]",
    ),
)


def sanitize_log_text(value):
    """Redact known secret values and common credential shapes from text."""
    text = "" if value is None else str(value)
    if not text:
        return text

    for env_var in _SECRET_ENV_VARS:
        secret = os.getenv(env_var)
        if secret:
            text = text.replace(secret, f"[REDACTED:{env_var}]")

    for pattern, replacement in _REDACTION_PATTERNS:
        text = pattern.sub(replacement, text)

    return text


def sanitize_exception_message(exc):
    """Return a log-safe exception string."""
    sanitized = sanitize_log_text(exc)
    if sanitized:
        return sanitized
    if exc is None:
        return ""
    return exc.__class__.__name__


def safe_target_path(base_dir, relative_path):
    """Join base_dir and relative_path, raising ValueError on path traversal.

    Uses os.path.realpath to resolve symlinks, avoiding false positives on
    systems where e.g. /var is a symlink to /private/var (macOS).
    """
    relative_path = str(relative_path or "")
    normalized_parts = relative_path.replace("\\", "/").split("/")
    if os.path.isabs(relative_path) or ".git" in normalized_parts:
        raise ValueError(f"Unsafe target path: {relative_path!r}")

    lexical_base = os.path.abspath(base_dir)
    lexical_joined = os.path.abspath(os.path.join(lexical_base, relative_path))
    try:
        within_lexical_base = os.path.commonpath([lexical_base, lexical_joined]) == lexical_base
    except ValueError:
        within_lexical_base = False
    if not within_lexical_base:
        raise ValueError(f"Path traversal detected: {relative_path!r}")

    current = lexical_base
    for part in os.path.relpath(lexical_joined, lexical_base).split(os.sep):
        if part in {"", "."}:
            continue
        current = os.path.join(current, part)
        if os.path.islink(current):
            raise ValueError(f"Symlink target paths are not allowed: {relative_path!r}")

    real_base = os.path.realpath(base_dir)
    joined = os.path.realpath(lexical_joined)
    if not joined.startswith(real_base + os.sep) and joined != real_base:
        raise ValueError(f"Path traversal detected: {relative_path!r}")
    return joined
