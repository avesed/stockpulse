// WebSocket message types — mirrors backend ws/protocol.py

// ---------------------------------------------------------------------------
// Client → Server
// ---------------------------------------------------------------------------

export interface WsSubscribeMessage {
  type: 'subscribe'
  channel: 'quotes' | 'collection_progress'
  symbols?: string[]
  markets?: string[]
}

export interface WsUnsubscribeMessage {
  type: 'unsubscribe'
  channel: 'quotes' | 'collection_progress'
  symbols?: string[]
  markets?: string[]
}

export interface WsCommandMessage {
  type: 'command'
  action: 'start_collector' | 'stop_collector' | 'get_collector_status'
  provider?: string
  symbols?: string[]
}

export interface WsPingMessage {
  type: 'ping'
}

export type WsClientMessage =
  | WsSubscribeMessage
  | WsUnsubscribeMessage
  | WsCommandMessage
  | WsPingMessage

// ---------------------------------------------------------------------------
// Server → Client
// ---------------------------------------------------------------------------

export interface WsConnectedEvent {
  type: 'connected'
  ts: string
  session_id: string
}

export interface WsQuoteEvent {
  type: 'quote'
  symbol: string
  price: number
  volume: number
  change: number
  change_pct: number
  source: string
  ts: string
}

export interface WsTradeEvent {
  type: 'trade'
  symbol: string
  price: number
  volume: number
  source: string
  conditions?: string[]
  ts: string
}

export interface WsCollectionProgressEvent {
  type: 'collection_progress'
  market: string
  symbols_done: number
  symbols_total: number
  new_bars: number
  percent: number
  updated_at: string
}

export interface WsCollectorStatusEvent {
  type: 'collector_status'
  provider: string
  status: 'connected' | 'disconnected' | 'stopped'
  symbols_count: number
  last_message_at?: string
  error?: string
}

export interface WsSubscribedEvent {
  type: 'subscribed'
  channel: string
  symbols?: string[]
  markets?: string[]
}

export interface WsUnsubscribedEvent {
  type: 'unsubscribed'
  channel: string
  symbols?: string[]
  markets?: string[]
}

export interface WsErrorEvent {
  type: 'error'
  code: string
  message: string
}

export interface WsPongEvent {
  type: 'pong'
  ts: string
}

export interface WsTokenExpiringEvent {
  type: 'token_expiring'
  expires_in_seconds: number
  ts: string
}

export interface WsCommandResultEvent {
  type: 'command_result'
  action: string
  success: boolean
  message?: string
  collectors?: WsCollectorStatusEvent[]
}

export type WsServerEvent =
  | WsConnectedEvent
  | WsQuoteEvent
  | WsTradeEvent
  | WsCollectionProgressEvent
  | WsCollectorStatusEvent
  | WsSubscribedEvent
  | WsUnsubscribedEvent
  | WsErrorEvent
  | WsPongEvent
  | WsTokenExpiringEvent
  | WsCommandResultEvent
