import apiClient from '@/api/client'
import type {
  DashboardStats,
  ApiConsumer,
  ProviderConfig,
  CollectionProgress,
  CollectionRunsResponse,
  CollectionRunDetail,
  SchedulerStatus,
  ConsumerRequestStats,
  SystemSetting,
} from '@/types'

// Dashboard
export async function getDashboardStats(): Promise<DashboardStats> {
  const response = await apiClient.get<DashboardStats>('/admin/stats/dashboard')
  return response.data
}

// Consumers
export async function getConsumers(): Promise<ApiConsumer[]> {
  const response = await apiClient.get<ApiConsumer[]>('/admin/consumers')
  return response.data
}

export async function createConsumer(data: {
  name: string
  description?: string
  rateLimit?: number
  allowedEndpoints?: string[]
}): Promise<ApiConsumer> {
  const response = await apiClient.post<ApiConsumer>('/admin/consumers', data)
  return response.data
}

export async function deactivateConsumer(id: string): Promise<void> {
  await apiClient.delete(`/admin/consumers/${id}`)
}

// Providers
export async function getProviders(): Promise<ProviderConfig[]> {
  const response = await apiClient.get<ProviderConfig[]>('/admin/providers')
  return response.data
}

export async function updateProvider(
  id: number,
  data: { displayName?: string; isEnabled?: boolean; apiKey?: string; configJson?: Record<string, unknown> }
): Promise<ProviderConfig> {
  const response = await apiClient.patch<ProviderConfig>(`/admin/providers/${id}`, data)
  return response.data
}

export async function toggleProvider(id: number, enabled: boolean): Promise<ProviderConfig> {
  const response = await apiClient.patch<ProviderConfig>(`/admin/providers/${id}`, { isEnabled: enabled })
  return response.data
}

export async function testProvider(id: number): Promise<{ success: boolean; message: string; elapsedMs: number | null }> {
  const response = await apiClient.post<{ success: boolean; message: string; elapsedMs: number | null }>(
    `/admin/providers/${id}/test`
  )
  return response.data
}

// Collection — daily bars
export async function getCollectionProgress(market: string): Promise<CollectionProgress> {
  const response = await apiClient.get<CollectionProgress>(
    `/admin/collection/daily-bars/${market}/progress`
  )
  return response.data
}

export async function startCollection(market: string): Promise<{ status: string; market: string; message: string }> {
  const response = await apiClient.post<{ status: string; market: string; message: string }>(
    `/admin/collection/daily-bars/${market}/collect`
  )
  return response.data
}

export async function rebuildCollection(market: string): Promise<{ status: string; market: string; message: string }> {
  const response = await apiClient.post<{ status: string; market: string; message: string }>(
    `/admin/collection/daily-bars/${market}/rebuild`
  )
  return response.data
}

export async function unlockCollection(market: string): Promise<void> {
  await apiClient.post(`/admin/collection/daily-bars/${market}/unlock`)
}

// Collection — runs (audit history)
export async function getCollectionRuns(params: {
  market?: string | undefined; limit?: number; offset?: number
}): Promise<CollectionRunsResponse> {
  const response = await apiClient.get<CollectionRunsResponse>('/admin/collection/runs', { params })
  return response.data
}

export async function getCollectionRunDetail(runId: number): Promise<CollectionRunDetail> {
  const response = await apiClient.get<CollectionRunDetail>(`/admin/collection/runs/${runId}`)
  return response.data
}

// Collection — stock list
export async function buildStockList(): Promise<{ status: string; message: string }> {
  const response = await apiClient.post<{ status: string; message: string }>(
    '/admin/collection/stock-list/build'
  )
  return response.data
}

export async function getStockListProgress(): Promise<{ progress: unknown; taskRunning: boolean }> {
  const response = await apiClient.get<{ progress: unknown; taskRunning: boolean }>(
    '/admin/collection/stock-list/progress'
  )
  return response.data
}

// Collection — profiles
export async function startProfileCollection(market: string): Promise<{ status: string; market: string; message: string }> {
  const response = await apiClient.post<{ status: string; market: string; message: string }>(
    `/admin/collection/stock-profiles/${market}/collect`
  )
  return response.data
}

// Scheduler
export async function getSchedulerStatus(): Promise<SchedulerStatus> {
  const response = await apiClient.get<SchedulerStatus>('/admin/scheduler/status')
  return response.data
}

// Request Stats
export async function getRequestStats(days?: number): Promise<ConsumerRequestStats[]> {
  const params = days ? { days } : {}
  const response = await apiClient.get<ConsumerRequestStats[]>('/admin/stats/requests', { params })
  return response.data
}

// Settings
export async function getSettings(): Promise<SystemSetting[]> {
  const response = await apiClient.get<SystemSetting[]>('/admin/settings')
  return response.data
}

export async function upsertSetting(key: string, value: string): Promise<SystemSetting> {
  const response = await apiClient.put<SystemSetting>(`/admin/settings/${key}`, { value })
  return response.data
}

export async function deleteSetting(key: string): Promise<void> {
  await apiClient.delete(`/admin/settings/${key}`)
}
