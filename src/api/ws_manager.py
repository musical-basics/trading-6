"""
ws_manager.py — Level 5 WebSocket Connection Manager

Manages active WebSocket client connections and provides
broadcast capabilities for real-time telemetry push.
"""

from __future__ import annotations

import json
import logging
from fastapi import WebSocket

logger = logging.getLogger(__name__)


class WebSocketManager:
    """Manages active WebSocket client connections."""

    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        """Accept a new WebSocket connection."""
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"🔌 WebSocket connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        """Remove a disconnected WebSocket."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"🔌 WebSocket disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, event_type: str, data: dict):
        """Push an event to all connected clients."""
        message = json.dumps({"type": event_type, "data": data})
        disconnected = []
        for conn in self.active_connections:
            try:
                await conn.send_text(message)
            except Exception:
                disconnected.append(conn)
        for conn in disconnected:
            self.active_connections.remove(conn)

    async def send_personal(self, websocket: WebSocket, event_type: str, data: dict):
        """Send an event to a single client."""
        message = json.dumps({"type": event_type, "data": data})
        try:
            await websocket.send_text(message)
        except Exception:
            pass


# Singleton instance used across the application
manager = WebSocketManager()
