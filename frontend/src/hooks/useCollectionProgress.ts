/**
 * Hook for real-time collection progress via WebSocket.
 *
 * Falls back to HTTP polling when WS is not connected.
 * When WS delivers progress events, it injects them into the React Query cache
 * and disables polling.
 */
import { useEffect, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { getAdminWebSocket } from '@/api/websocket'
import { getAccessToken } from '@/api/client'
import { useRealtimeStore } from '@/stores/realtimeStore'
import type { WsServerEvent } from '@/types/websocket'

export function useCollectionProgressWs(markets: readonly string[]) {
  const queryClient = useQueryClient()
  const wsRef = useRef(getAdminWebSocket(getAccessToken))
  const connectionStatus = useRealtimeStore((s) => s.connectionStatus)
  const updateCollectionProgress = useRealtimeStore((s) => s.updateCollectionProgress)
  const setConnectionStatus = useRealtimeStore((s) => s.setConnectionStatus)

  useEffect(() => {
    const ws = wsRef.current

    ws.onStatusChange = (status) => {
      setConnectionStatus(status)
    }

    ws.onMessage = (event: WsServerEvent) => {
      if (event.type === 'collection_progress') {
        updateCollectionProgress(event)

        // Also inject into React Query cache for backward compat
        queryClient.setQueryData(['collection-progress', event.market], {
          market: event.market,
          progress: {
            current: event.symbols_done,
            total: event.symbols_total,
            message: `${event.new_bars} new bars`,
          },
          taskRunning: event.percent < 100,
        })
      }

      if (event.type === 'collector_status') {
        useRealtimeStore.getState().updateCollectorStatus(event)
      }
    }

    ws.connect()

    return () => {
      // Don't disconnect on unmount — the singleton persists
    }
  }, [queryClient, updateCollectionProgress, setConnectionStatus])

  // Subscribe to collection progress for the given markets
  useEffect(() => {
    const ws = wsRef.current
    if (connectionStatus === 'connected' && markets.length > 0) {
      ws.subscribe('collection_progress', [...markets])
    }
  }, [connectionStatus, markets])

  // Return whether WS is active (so callers can decide on polling)
  return {
    isWsConnected: connectionStatus === 'connected',
  }
}
