/**
 * StockPulse WebSocket client with auto-reconnect and auth handling.
 *
 * Usage:
 *   const ws = new StockPulseWebSocket('/ws/admin', () => getAccessToken() ?? '')
 *   ws.onMessage = (event) => { ... }
 *   ws.connect()
 */
import type { WsClientMessage, WsServerEvent } from '@/types/websocket'

export type WsEndpoint = '/ws/admin' | '/ws/data'

export type ConnectionStatus = 'connecting' | 'connected' | 'disconnected' | 'reconnecting'

const INITIAL_RECONNECT_DELAY = 1000
const MAX_RECONNECT_DELAY = 30000
const PING_INTERVAL = 25000 // slightly less than server's 30s heartbeat

export class StockPulseWebSocket {
  private ws: WebSocket | null = null
  private reconnectDelay = INITIAL_RECONNECT_DELAY
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private pingTimer: ReturnType<typeof setInterval> | null = null
  private pendingSubscriptions: WsClientMessage[] = []
  private _status: ConnectionStatus = 'disconnected'
  private _sessionId: string | null = null
  private intentionalClose = false

  onMessage: ((event: WsServerEvent) => void) | null = null
  onStatusChange: ((status: ConnectionStatus) => void) | null = null

  constructor(
    private endpoint: WsEndpoint,
    private getAuthParam: () => string,
  ) {}

  get status(): ConnectionStatus {
    return this._status
  }

  get sessionId(): string | null {
    return this._sessionId
  }

  connect(): void {
    if (this.ws?.readyState === WebSocket.OPEN || this.ws?.readyState === WebSocket.CONNECTING) {
      return
    }
    this.intentionalClose = false
    this._setStatus('connecting')

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const authParam = this.endpoint === '/ws/admin' ? 'token' : 'api_key'
    const authValue = this.getAuthParam()
    const url = `${protocol}//${window.location.host}/api/v1${this.endpoint}?${authParam}=${encodeURIComponent(authValue)}`

    this.ws = new WebSocket(url)

    this.ws.onopen = () => {
      this.reconnectDelay = INITIAL_RECONNECT_DELAY
      this._startPing()
    }

    this.ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data) as WsServerEvent
        this._handleMessage(data)
      } catch {
        // Ignore malformed messages
      }
    }

    this.ws.onclose = (event) => {
      this._stopPing()
      this._sessionId = null

      if (event.code === 4001) {
        // Auth failed — don't reconnect
        this._setStatus('disconnected')
        return
      }

      if (!this.intentionalClose) {
        this._setStatus('reconnecting')
        this._scheduleReconnect()
      } else {
        this._setStatus('disconnected')
      }
    }

    this.ws.onerror = () => {
      // onclose will fire after onerror
    }
  }

  disconnect(): void {
    this.intentionalClose = true
    this._stopPing()
    this._clearReconnect()
    if (this.ws) {
      this.ws.close(1000, 'Client disconnect')
      this.ws = null
    }
    this._sessionId = null
    this._setStatus('disconnected')
  }

  send(message: WsClientMessage): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message))
    } else {
      // Queue for re-send after reconnect
      this.pendingSubscriptions.push(message)
    }
  }

  subscribe(channel: 'quotes', symbols: string[]): void
  subscribe(channel: 'collection_progress', markets: string[]): void
  subscribe(channel: string, items: string[]): void {
    const msg: WsClientMessage = channel === 'quotes'
      ? { type: 'subscribe', channel: 'quotes', symbols: items }
      : { type: 'subscribe', channel: 'collection_progress', markets: items }
    this.send(msg)
    // Store for resubscription on reconnect
    this.pendingSubscriptions = this.pendingSubscriptions.filter(
      (m) => !(m.type === 'subscribe' && 'channel' in m && m.channel === channel),
    )
    this.pendingSubscriptions.push(msg)
  }

  unsubscribe(channel: 'quotes', symbols: string[]): void
  unsubscribe(channel: 'collection_progress', markets: string[]): void
  unsubscribe(channel: string, items: string[]): void {
    const msg: WsClientMessage = channel === 'quotes'
      ? { type: 'unsubscribe', channel: 'quotes', symbols: items }
      : { type: 'unsubscribe', channel: 'collection_progress', markets: items }
    this.send(msg)
  }

  private _handleMessage(data: WsServerEvent): void {
    if (data.type === 'connected') {
      this._sessionId = data.session_id
      this._setStatus('connected')
      // Re-send pending subscriptions
      for (const sub of this.pendingSubscriptions) {
        if (sub.type === 'subscribe' && this.ws?.readyState === WebSocket.OPEN) {
          this.ws.send(JSON.stringify(sub))
        }
      }
    }

    this.onMessage?.(data)
  }

  private _setStatus(status: ConnectionStatus): void {
    if (this._status !== status) {
      this._status = status
      this.onStatusChange?.(status)
    }
  }

  private _scheduleReconnect(): void {
    this._clearReconnect()
    this.reconnectTimer = setTimeout(() => {
      this.connect()
    }, this.reconnectDelay)
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, MAX_RECONNECT_DELAY)
  }

  private _clearReconnect(): void {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
  }

  private _startPing(): void {
    this._stopPing()
    this.pingTimer = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ type: 'ping' }))
      }
    }, PING_INTERVAL)
  }

  private _stopPing(): void {
    if (this.pingTimer !== null) {
      clearInterval(this.pingTimer)
      this.pingTimer = null
    }
  }
}

// ---------------------------------------------------------------------------
// Singleton admin WS instance
// ---------------------------------------------------------------------------

let _adminWs: StockPulseWebSocket | null = null

export function getAdminWebSocket(getToken: () => string | null): StockPulseWebSocket {
  if (_adminWs === null) {
    _adminWs = new StockPulseWebSocket('/ws/admin', () => getToken() ?? '')
  }
  return _adminWs
}
