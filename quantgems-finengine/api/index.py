"""Vercel Serverless 入口，沿用財報引擎 Flask API。"""

import sys
from pathlib import Path


SERVER_DIR = Path(__file__).resolve().parents[1] / "server"
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

from app import app  # noqa: E402

