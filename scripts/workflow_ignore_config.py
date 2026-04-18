"""Load workflow ignore settings from JSON."""

import json
import os


WORKFLOW_IGNORE_CONFIG_PATH_ENV = "WORKFLOW_IGNORE_CONFIG_PATH"
DEFAULT_WORKFLOW_IGNORE_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "workflow_ignore_config.json",
)
IGNORE_CONFIG_KEYS = (
    "PR_MODE_IGNORE_FILES",
    "PR_MODE_IGNORE_FOLDERS",
    "COMMIT_BASED_MODE_IGNORE_FILES",
    "COMMIT_BASED_MODE_IGNORE_FOLDERS",
)


def _normalize_string_list(value, key):
    if not isinstance(value, list):
        raise ValueError(f"{key} must be a JSON array")

    normalized = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError(f"{key} entries must be strings")
        stripped = item.strip().strip("/").strip()
        if stripped:
            normalized.append(stripped)
    return normalized


def load_workflow_ignore_config(path=None):
    """Load and validate workflow ignore config."""
    config_path = path or os.getenv(
        WORKFLOW_IGNORE_CONFIG_PATH_ENV,
        DEFAULT_WORKFLOW_IGNORE_CONFIG_PATH,
    )
    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = json.load(f)

    missing_keys = [key for key in IGNORE_CONFIG_KEYS if key not in raw_config]
    if missing_keys:
        raise ValueError(
            f"Missing workflow ignore config key(s): {', '.join(missing_keys)}"
        )

    return {
        key: _normalize_string_list(raw_config[key], key)
        for key in IGNORE_CONFIG_KEYS
    }
