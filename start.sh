#!/bin/bash
# Start both backend API and frontend dev server concurrently.
# Logs are prefixed with [API] and [WEB] for easy debugging.
# Usage: ./start.sh

trap 'kill 0' EXIT  # Kill all child processes on exit (Ctrl+C)

API_PORT=8000
WEB_PORT=3000

kill_port_if_busy() {
  local port="$1"
  local pids
  pids=$(lsof -ti tcp:"$port" 2>/dev/null || true)
  if [ -n "$pids" ]; then
    echo "⚠ Port $port is in use by PID(s): $pids"
    echo "  Stopping existing process(es) on port $port..."
    kill $pids 2>/dev/null || true
    sleep 1
  fi
}

echo "🚀 Starting QuantPrime..."
echo "   API  → http://localhost:${API_PORT}"
echo "   WEB  → http://localhost:${WEB_PORT}"
echo "   Ctrl+C to stop both"
echo ""

# Ensure a clean single-instance dev session.
kill_port_if_busy "$API_PORT"
kill_port_if_busy "$WEB_PORT"

# Backend API (prefixed)
(
  ./.venv/bin/python -m uvicorn src.api.server:app --reload --port "$API_PORT" 2>&1 | \
    tee api.log | sed 's/^/[API] /'
) &

# Frontend (prefixed)
(
  # Wipe next cache to avoid SST/Turbopack corruption bugs
  rm -rf frontend/.next
  cd frontend && NEXT_PUBLIC_API_URL="http://localhost:${API_PORT}" pnpm dev --port "$WEB_PORT" 2>&1 | \
    sed 's/^/[WEB] /'
) &

wait
