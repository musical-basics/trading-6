/**
 * use-websocket.ts — Level 5 WebSocket Provider Hook
 *
 * Connects to the FastAPI WebSocket telemetry endpoint and
 * provides real-time events to React components.
 *
 * Usage:
 *   const { lastEvent, isConnected, executionFeed } = useWebSocket()
 */

"use client"

import { useState, useEffect, useRef, useCallback } from "react"

const WS_BASE = process.env.NEXT_PUBLIC_API_URL?.replace("http", "ws") ?? "ws://localhost:8000"
const WS_URL = `${WS_BASE}/api/ws/telemetry`

export interface TelemetryEvent {
  type: string
  data: Record<string, unknown>
  timestamp: string
}

export interface ExecutionEvent {
  ticker: string
  action: string
  quantity: number
  price: number
  timestamp: string
  trader_id?: number
  portfolio_id?: number
}

export function useWebSocket() {
  const [isConnected, setIsConnected] = useState(false)
  const [lastEvent, setLastEvent] = useState<TelemetryEvent | null>(null)
  const [executionFeed, setExecutionFeed] = useState<ExecutionEvent[]>([])
  const wsRef = useRef<WebSocket | null>(null)
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null)
  const pingIntervalRef = useRef<NodeJS.Timeout | null>(null)

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return

    try {
      const ws = new WebSocket(WS_URL)

      ws.onopen = () => {
        setIsConnected(true)
        // Start ping heartbeat
        pingIntervalRef.current = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) {
            ws.send("ping")
          }
        }, 30000)
      }

      ws.onmessage = (event) => {
        try {
          const parsed = JSON.parse(event.data) as TelemetryEvent

          if (parsed.type === "pong") return // Ignore heartbeat responses

          // Add timestamp if not present
          if (!parsed.timestamp) {
            parsed.timestamp = new Date().toISOString()
          }

          setLastEvent(parsed)

          // Accumulate execution events
          if (parsed.type === "execution") {
            setExecutionFeed((prev) => [
              parsed.data as unknown as ExecutionEvent,
              ...prev.slice(0, 99), // Keep last 100 events
            ])
          }
        } catch {
          // Ignore malformed messages
        }
      }

      ws.onclose = () => {
        setIsConnected(false)
        if (pingIntervalRef.current) {
          clearInterval(pingIntervalRef.current)
        }
        // Auto-reconnect after 3 seconds
        reconnectTimeoutRef.current = setTimeout(connect, 3000)
      }

      ws.onerror = () => {
        ws.close()
      }

      wsRef.current = ws
    } catch {
      // WebSocket not available — degrade gracefully
      setIsConnected(false)
    }
  }, [])

  useEffect(() => {
    connect()

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current)
      }
      if (pingIntervalRef.current) {
        clearInterval(pingIntervalRef.current)
      }
      wsRef.current?.close()
    }
  }, [connect])

  return {
    isConnected,
    lastEvent,
    executionFeed,
  }
}
