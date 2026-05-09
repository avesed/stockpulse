import { describe, it, expect, beforeEach } from 'vitest'
import { useRealtimeStore } from '../realtimeStore'
import type { WsQuoteEvent, WsCollectionProgressEvent } from '@/types/websocket'

describe('realtimeStore', () => {
  beforeEach(() => {
    useRealtimeStore.getState().reset()
  })

  it('starts with disconnected status', () => {
    expect(useRealtimeStore.getState().connectionStatus).toBe('disconnected')
  })

  it('updateQuote adds/updates quote', () => {
    const event: WsQuoteEvent = {
      type: 'quote',
      symbol: 'AAPL',
      price: 150,
      volume: 1000,
      change: 2,
      change_pct: 1.3,
      source: 'yfinance',
      ts: Date.now() / 1000,
    }
    useRealtimeStore.getState().updateQuote(event)
    const quotes = useRealtimeStore.getState().quotes
    expect(quotes.get('AAPL')?.price).toBe(150)
  })

  it('updateCollectionProgress sets market progress', () => {
    const event = {
      type: 'collection_progress',
      market: 'us',
      current: 50,
      total: 100,
    } as WsCollectionProgressEvent
    useRealtimeStore.getState().updateCollectionProgress(event)
    expect(useRealtimeStore.getState().collectionProgress.get('us')).toBeDefined()
  })

  it('clearCollectionProgress removes market entry', () => {
    const event = {
      type: 'collection_progress',
      market: 'us',
      current: 50,
      total: 100,
    } as WsCollectionProgressEvent
    useRealtimeStore.getState().updateCollectionProgress(event)
    useRealtimeStore.getState().clearCollectionProgress('us')
    expect(useRealtimeStore.getState().collectionProgress.has('us')).toBe(false)
  })

  it('reset returns to initial state', () => {
    useRealtimeStore.getState().setConnectionStatus('connected')
    useRealtimeStore.getState().reset()
    expect(useRealtimeStore.getState().connectionStatus).toBe('disconnected')
    expect(useRealtimeStore.getState().quotes.size).toBe(0)
  })
})
