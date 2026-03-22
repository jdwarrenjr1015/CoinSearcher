"""
api/index.py
------------
Vercel serverless entry point — imports and exposes the Flask app.
"""
import sys
import os

# Add project root and tools/ to path so imports resolve correctly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools"))

from tools.web_app import app  # noqa: F401 — Vercel looks for 'app'
