import { create } from 'zustand'
import type { ConnectionStatus } from '@/api/websocket'
import type {
  WsQuoteEvent,
  WsTradeEvent,
  WsCollectionProgressEvent,
  WsCollectorStatusEvent,
} from '@/types/websocket'

interface RealtimeState {
  // Connection
  connectionStatus: ConnectionStatus

  // Quotes
  quotes: Map<string, WsQuoteEvent>

  // Collection progress (market -> progress)
  collectionProgress: Map<string, WsCollectionProgressEvent>

  // Collector status (provider -> status)
  collectorStatus: Map<string, WsCollectorStatusEvent>
}

interface RealtimeActions {
  setConnectionStatus: (status: ConnectionStatus) => void
  updateQuote: (event: WsQuoteEvent | WsTradeEvent) => void
  updateCollectionProgress: (event: WsCollectionProgressEvent) => void
  updateCollectorStatus: (event: WsCollectorStatusEvent) => void
  clearCollectionProgress: (market: string) => void
  reset: () => void
}

const initialState: RealtimeState = {
  connectionStatus: 'disconnected',
  quotes: new Map(),
  collectionProgress: new Map(),
  collectorStatus: new Map(),
}

export const useRealtimeStore = create<RealtimeState & RealtimeActions>((set) => ({
  ...initialState,

  setConnectionStatus: (status) => set({ connectionStatus: status }),

  updateQuote: (event) =>
    set((state) => {
      const quotes = new Map(state.quotes)
      quotes.set(event.symbol, {
        type: 'quote',
        symbol: event.symbol,
        price: event.price,
        volume: event.volume,
        change: 'change' in event ? event.change : 0,
        change_pct: 'change_pct' in event ? event.change_pct : 0,
        source: event.source,
        ts: event.ts,
      })
      return { quotes }
    }),

  updateCollectionProgress: (event) =>
    set((state) => {
      const collectionProgress = new Map(state.collectionProgress)
      collectionProgress.set(event.market, event)
      return { collectionProgress }
    }),

  updateCollectorStatus: (event) =>
    set((state) => {
      const collectorStatus = new Map(state.collectorStatus)
      collectorStatus.set(event.provider, event)
      return { collectorStatus }
    }),

  clearCollectionProgress: (market) =>
    set((state) => {
      const collectionProgress = new Map(state.collectionProgress)
      collectionProgress.delete(market)
      return { collectionProgress }
    }),

  reset: () => set(initialState),
}))
