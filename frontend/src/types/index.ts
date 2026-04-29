export interface User {
  id: number
  email: string
  displayName: string | null
  role: 'admin' | 'user'
  locale: string
}

export interface TokenResponse {
  accessToken: string
  refreshToken: string
  tokenType: string
}

export interface ApiError {
  detail: string
}

export interface ApiConsumer {
  id: string
  name: string
  apiKeyPrefix: string
  description: string | null
  isActive: boolean
  rateLimit: number
  allowedEndpoints: string[] | null
  lastUsedAt: string | null
  createdAt: string
  rawApiKey?: string  // only on creation
}

export interface ProviderConfig {
  id: number
  providerName: string
  displayName: string
  isEnabled: boolean
  hasApiKey: boolean
  apiKeysCount: number
  configJson: Record<string, unknown> | null
  lastHealthCheck: string | null
  healthStatus: 'healthy' | 'degraded' | 'error' | 'unknown'
  errorMessage: string | null
  createdAt: string
  updatedAt: string
}

export interface DashboardStats {
  totalBars: number
  totalSymbols: number
  activeConsumers: number
  totalProviders: number
  enabledProviders: number
  requestsToday: number
  errorsToday: number
}

export interface CollectionProgress {
  market: string
  progress: {
    status?: string
    current?: number
    total?: number
    message?: string
    elapsedSeconds?: number
    errorsCount?: number
    estimatedRemaining?: number | null
    startedAt?: string
    lastRun?: LastRunSummary | null
  } | null
  taskRunning: boolean
  lastRun?: LastRunSummary | null
}

export interface LastRunSummary {
  status: string
  durationSeconds: number | null
  errorCount: number
  newBars: number
  finishedAt: string | null
}

export interface CollectionRun {
  id: number
  market: string
  runType: 'collect' | 'rebuild' | 'scheduled'
  status: 'running' | 'completed' | 'failed'
  symbolsTotal: number
  symbolsDone: number
  newBars: number
  errorCount: number
  startedAt: string
  finishedAt: string | null
  durationSeconds: number | null
  triggeredBy: string
}

export interface CollectionRunDetail extends CollectionRun {
  errorsJson: Array<{ symbol: string; error: string; category: string }> | null
}

export interface CollectionRunsResponse {
  runs: CollectionRun[]
  total: number
}

export interface SchedulerJob {
  id: string
  name: string
  trigger: string | null
  nextRunTime: string | null
}

export interface SchedulerStatus {
  isLeader: boolean
  running: boolean
  jobs: SchedulerJob[]
}

export interface ConsumerRequestStats {
  consumerId: string
  consumerName: string | null
  date: string
  requests: number
  errors: number
}

export interface SystemSetting {
  key: string
  value: string
  updatedAt: string | null
}

export interface StockListItem {
  symbol: string
  name: string
  market: string
}
