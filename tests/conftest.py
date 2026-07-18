"""Keep local developer credentials out of the test process.

Tests that exercise credential selection set explicit placeholders themselves.
This runs before test-module collection, so clients cannot capture real shell
credentials in module-level constants by accident.
"""

import os


for variable in (
    "GITHUB_TOKEN",
    "DEEPSEEK_API_TOKEN",
    "GEMINI_API_TOKEN",
    "OPENAI_API_TOKEN",
    "AZURE_OPENAI_KEY",
    "AZURE_OPENAI_BASE_URL",
    "OPENAI_BASE_URL",
    "TRANS_KEY",
    "TRANS_URL",
):
    os.environ.pop(variable, None)
