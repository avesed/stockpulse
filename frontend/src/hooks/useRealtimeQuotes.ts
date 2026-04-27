/**
 * Hook for subscribing to real-time quote updates via WebSocket.
 *
 * Returns the latest quote data from the realtime store.
 */
import { useEffect, useRef } from 'react'
import { getAdminWebSocket, StockPulseWebSocket } from '@/api/websocket'
import { getAccessToken } from '@/api/client'
import { useRealtimeStore } from '@/stores/realtimeStore'
import type { WsQuoteEvent, WsServerEvent } from '@/types/websocket'

export function useRealtimeQuotes(symbols: string[]) {
  const wsRef = useRef(getAdminWebSocket(getAccessToken))
  const connectionStatus = useRealtimeStore((s) => s.connectionStatus)
  const quotes = useRealtimeStore((s) => s.quotes)
  const updateQuote = useRealtimeStore((s) => s.updateQuote)
  const setConnectionStatus = useRealtimeStore((s) => s.setConnectionStatus)

  useEffect(() => {
    const ws = wsRef.current

    ws.onStatusChange = (status) => {
      setConnectionStatus(status)
    }

    const prevHandler = ws.onMessage
    ws.onMessage = (event: WsServerEvent) => {
      if (event.type === 'quote' || event.type === 'trade') {
        updateQuote(event)
      }
      // Call previous handler if set (e.g., from useCollectionProgress)
      prevHandler?.(event)
    }

    ws.connect()

    return () => {
      // Restore previous handler
      ws.onMessage = prevHandler ?? null
    }
  }, [updateQuote, setConnectionStatus])

  // Subscribe/unsubscribe to symbols when they change
  useEffect(() => {
    const ws = wsRef.current
    if (connectionStatus === 'connected' && symbols.length > 0) {
      ws.subscribe('quotes', symbols)
    }
    return () => {
      if (connectionStatus === 'connected' && symbols.length > 0) {
        ws.unsubscribe('quotes', symbols)
      }
    }
  }, [connectionStatus, symbols.join(',')])

  // Return quotes for requested symbols
  const result: Record<string, WsQuoteEvent | undefined> = {}
  for (const sym of symbols) {
    result[sym.toUpperCase()] = quotes.get(sym.toUpperCase())
  }

  return {
    quotes: result,
    isConnected: connectionStatus === 'connected',
  }
}
