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
