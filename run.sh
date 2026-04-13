#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

echo "=== TabClaw Startup ==="

# Install dependencies
echo "Installing Python dependencies..."
pip3 install -q fastapi "uvicorn[standard]" python-multipart aiofiles

echo "Starting TabClaw server..."
echo "Open http://localhost:8000 in your browser"
echo "(Press Ctrl+C to stop)"
echo ""

python3 -m uvicorn app:app --host 0.0.0.0 --port 8000 --reload
