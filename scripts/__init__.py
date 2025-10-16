#!/usr/bin/env python3
"""
Auto-Sync PR Changes - Refactored Modular Version

This package contains the refactored version of the auto-sync-pr-changes script,
split into logical modules for better maintainability and testing.

Modules:
- pr_analyzer: PR analysis, diff parsing, content getting, hierarchy building
- section_matcher: Section matching (direct matching + AI matching)  
- file_adder: New file processing and translation
- file_deleter: Deleted file processing
- file_updater: Updated file processing and translation
- toc_processor: TOC file special processing
- main: Main orchestration function
"""

# Import main functionality for easy access
from main import main

# Make main function available at package level
__all__ = ["main"]
