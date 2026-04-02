#!/bin/bash
# Start both backend API and frontend dev server concurrently.
# Logs are prefixed with [API] and [WEB] for easy debugging.
# Usage: ./start.sh

trap 'kill 0' EXIT  # Kill all child processes on exit (Ctrl+C)

echo "🚀 Starting QuantPrime..."
echo "   API  → http://localhost:8000"
echo "   WEB  → http://localhost:3000"
echo "   Ctrl+C to stop both"
echo ""

# Backend API (prefixed)
(
  source .venv/bin/activate
  python3 -m uvicorn src.api.server:app --reload --port 8000 2>&1 | \
    tee api.log | sed 's/^/[API] /'
) &

# Frontend (prefixed)
(
  # Wipe next cache to avoid SST/Turbopack corruption bugs
  rm -rf frontend/.next
  cd frontend && pnpm dev 2>&1 | \
    sed 's/^/[WEB] /'
) &

wait
