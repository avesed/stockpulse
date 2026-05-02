import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Play, RefreshCw, Unlock, Loader2, List, Wifi, WifiOff,
  Clock, AlertTriangle, ChevronDown, ChevronRight, Timer,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Progress } from '@/components/ui/progress'
import { Skeleton } from '@/components/ui/skeleton'
import { Separator } from '@/components/ui/separator'
import {
  getCollectionProgress,
  startCollection,
  rebuildCollection,
  unlockCollection,
  startProfileCollection,
  getFundamentalsProgress,
  startFundamentals,
  getSchedulerStatus,
  getCollectionRuns,
  getCollectionRunDetail,
} from '@/api/admin'
import type { FundProgress } from '@/api/admin'
import { getErrorMessage } from '@/api/client'
import { showToast, showErrorToast } from '@/stores/toastStore'
import { useCollectionProgressWs } from '@/hooks/useCollectionProgress'
import type { CollectionRun, CollectionRunDetail as RunDetail, LastRunSummary } from '@/types'

const MARKETS = ['cn', 'hk', 'us', 'metal'] as const

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null || seconds <= 0) return '-'
  const m = Math.floor(seconds / 60)
  const s = Math.round(seconds % 60)
  if (m === 0) return `${s}s`
  return `${m}m ${s}s`
}

function formatRelativeTime(iso: string | null | undefined): string {
  if (!iso) return '-'
  const diff = (Date.now() - new Date(iso).getTime()) / 1000
  if (diff < 60) return `${Math.round(diff)}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return `${Math.floor(diff / 86400)}d ago`
}

function formatNumber(n: number): string {
  return n.toLocaleString()
}

// ---------------------------------------------------------------------------
// MarketCard
// ---------------------------------------------------------------------------

function MarketCard({ market, isWsConnected }: { market: string; isWsConnected: boolean }) {
  const { t } = useTranslation()
  const queryClient = useQueryClient()

  const { data: progress, isLoading } = useQuery({
    queryKey: ['collection-progress', market],
    queryFn: () => getCollectionProgress(market),
    refetchInterval: (query) => {
      if (isWsConnected) return false
      const data = query.state.data
      return data?.taskRunning ? 2000 : false
    },
  })

  const collectMutation = useMutation({
    mutationFn: () => startCollection(market),
    onSuccess: () => {
      showToast(t('common.success'), `${market.toUpperCase()} collection started`)
      queryClient.invalidateQueries({ queryKey: ['collection-progress', market] })
    },
    onError: (err) => showErrorToast(t('common.error'), getErrorMessage(err)),
  })

  const rebuildMutation = useMutation({
    mutationFn: () => rebuildCollection(market),
    onSuccess: () => {
      showToast(t('common.success'), `${market.toUpperCase()} rebuild started`)
      queryClient.invalidateQueries({ queryKey: ['collection-progress', market] })
    },
    onError: (err) => showErrorToast(t('common.error'), getErrorMessage(err)),
  })

  const unlockMutation = useMutation({
    mutationFn: () => unlockCollection(market),
    onSuccess: () => {
      showToast(t('common.success'), `${market.toUpperCase()} unlocked`)
      queryClient.invalidateQueries({ queryKey: ['collection-progress', market] })
    },
    onError: (err) => showErrorToast(t('common.error'), getErrorMessage(err)),
  })

  const isRunning = progress?.taskRunning === true
  const progressData = progress?.progress
  const pct = progressData && (progressData.total ?? 0) > 0
    ? Math.round(((progressData.current ?? 0) / (progressData.total ?? 1)) * 100)
    : 0

  // Last run: from progress response or top-level
  const lastRun: LastRunSummary | null | undefined =
    progressData?.lastRun ?? progress?.lastRun

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-base font-semibold">{market.toUpperCase()}</CardTitle>
        {isLoading ? (
          <Skeleton className="h-5 w-16" />
        ) : isRunning ? (
          <Badge className="bg-blue-500/10 text-blue-600 border-blue-200">{t('collection.running')}</Badge>
        ) : (
          <Badge variant="secondary">{t('collection.idle')}</Badge>
        )}
      </CardHeader>
      <CardContent className="space-y-4">
        {isRunning && progressData && (
          <div className="space-y-2">
            <Progress value={pct} className="h-2" />
            <div className="flex justify-between text-xs text-muted-foreground">
              <span>{progressData.message ?? ''}</span>
              <span>{progressData.current ?? 0}/{progressData.total ?? 0} ({pct}%)</span>
            </div>
            {/* Enhanced: elapsed, errors, ETA */}
            <div className="flex items-center gap-3 text-xs text-muted-foreground">
              {progressData.elapsedSeconds != null && (
                <span className="flex items-center gap-1">
                  <Timer className="h-3 w-3" />
                  {formatDuration(progressData.elapsedSeconds)}
                </span>
              )}
              {(progressData.errorsCount ?? 0) > 0 && (
                <span className="flex items-center gap-1 text-red-500">
                  <AlertTriangle className="h-3 w-3" />
                  {progressData.errorsCount} {t('collection.errors')}
                </span>
              )}
              {progressData.estimatedRemaining != null && progressData.estimatedRemaining > 0 && (
                <span className="flex items-center gap-1">
                  <Clock className="h-3 w-3" />
                  {t('collection.remaining', { time: formatDuration(progressData.estimatedRemaining) })}
                </span>
              )}
            </div>
          </div>
        )}

        <div className="flex gap-2">
          <Button
            size="sm"
            onClick={() => collectMutation.mutate()}
            disabled={isRunning || collectMutation.isPending}
          >
            {collectMutation.isPending ? (
              <Loader2 className="mr-1 h-4 w-4 animate-spin" />
            ) : (
              <Play className="mr-1 h-4 w-4" />
            )}
            {t('collection.collect')}
          </Button>
          <Button
            size="sm"
            variant="outline"
            onClick={() => rebuildMutation.mutate()}
            disabled={isRunning || rebuildMutation.isPending}
          >
            <RefreshCw className="mr-1 h-4 w-4" />
            {t('collection.rebuild')}
          </Button>
          <Button
            size="sm"
            variant="ghost"
            onClick={() => unlockMutation.mutate()}
            disabled={unlockMutation.isPending}
          >
            <Unlock className="mr-1 h-4 w-4" />
            {t('collection.unlock')}
          </Button>
        </div>

        {/* Last run summary */}
        {!isRunning && lastRun && lastRun.finishedAt && (
          <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground pt-1 border-t">
            <span>{t('collection.lastRun')}:</span>
            <span>{formatRelativeTime(lastRun.finishedAt)}</span>
            <span className="text-foreground/50">|</span>
            <span>{formatNumber(lastRun.newBars)} {t('collection.barsInserted')}</span>
            {lastRun.errorCount > 0 && (
              <>
                <span className="text-foreground/50">|</span>
                <span className="text-red-500">{lastRun.errorCount} {t('collection.errors')}</span>
              </>
            )}
            {lastRun.durationSeconds != null && (
              <>
                <span className="text-foreground/50">|</span>
                <span>{formatDuration(lastRun.durationSeconds)}</span>
              </>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// StockListPanel
// ---------------------------------------------------------------------------

function StockListPanel() {
  const { t } = useTranslation()
  const [selectedMarket, setSelectedMarket] = useState<string>('cn')
  const queryClient = useQueryClient()

  const profileMutation = useMutation({
    mutationFn: () => startProfileCollection(selectedMarket),
    onSuccess: () => {
      showToast(t('common.success'), `Profile collection started for ${selectedMarket.toUpperCase()}`)
      queryClient.invalidateQueries({ queryKey: ['collection-progress', selectedMarket] })
    },
    onError: (err: unknown) => showErrorToast(t('common.error'), getErrorMessage(err)),
  })

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg">{t('collection.stockList')}</CardTitle>
          <div className="flex gap-2">
            {MARKETS.map((m) => (
              <Button
                key={m}
                size="sm"
                variant={selectedMarket === m ? 'default' : 'outline'}
                onClick={() => setSelectedMarket(m)}
              >
                {m.toUpperCase()}
              </Button>
            ))}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <div className="mb-4">
          <Button
            size="sm"
            variant="outline"
            onClick={() => profileMutation.mutate()}
            disabled={profileMutation.isPending}
          >
            {profileMutation.isPending ? (
              <Loader2 className="mr-1 h-4 w-4 animate-spin" />
            ) : (
              <List className="mr-1 h-4 w-4" />
            )}
            {t('collection.startProfiles')}
          </Button>
        </div>
        <p className="text-sm text-muted-foreground">{t('common.noData')}</p>
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// CollectionHistoryPanel
// ---------------------------------------------------------------------------

function RunStatusBadge({ status }: { status: string }) {
  const { t } = useTranslation()
  switch (status) {
    case 'completed':
      return <Badge className="bg-green-500/10 text-green-600 border-green-200 text-xs">{t('collection.completed')}</Badge>
    case 'failed':
      return <Badge className="bg-red-500/10 text-red-600 border-red-200 text-xs">{t('collection.failed')}</Badge>
    case 'running':
      return <Badge className="bg-blue-500/10 text-blue-600 border-blue-200 text-xs">{t('collection.running')}</Badge>
    default:
      return <Badge variant="secondary" className="text-xs">{status}</Badge>
  }
}

function ErrorDetailRow({ run }: { run: CollectionRun }) {
  const { t } = useTranslation()
  const { data: detail, isLoading } = useQuery({
    queryKey: ['collection-run-detail', run.id],
    queryFn: () => getCollectionRunDetail(run.id),
  })

  if (isLoading) {
    return (
      <tr>
        <td colSpan={8} className="py-3 px-4">
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <Loader2 className="h-3 w-3 animate-spin" />
            {t('common.loading')}
          </div>
        </td>
      </tr>
    )
  }

  const errors = detail?.errorsJson
  if (!errors || errors.length === 0) {
    return (
      <tr>
        <td colSpan={8} className="py-3 px-4">
          <p className="text-xs text-muted-foreground">{t('collection.noErrors')}</p>
        </td>
      </tr>
    )
  }

  return (
    <tr>
      <td colSpan={8} className="py-2 px-4">
        <div className="max-h-48 overflow-y-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b text-left text-muted-foreground">
                <th className="pb-1 pr-3 font-medium">{t('collection.symbol')}</th>
                <th className="pb-1 pr-3 font-medium">{t('collection.category')}</th>
                <th className="pb-1 font-medium">{t('collection.errorMessage')}</th>
              </tr>
            </thead>
            <tbody>
              {errors.map((err, i) => (
                <tr key={i} className="border-b last:border-0">
                  <td className="py-1 pr-3 font-mono">{err.symbol || '-'}</td>
                  <td className="py-1 pr-3">
                    <Badge variant="outline" className="text-[10px] px-1">
                      {err.category}
                    </Badge>
                  </td>
                  <td className="py-1 text-red-400 break-all">{err.error}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </td>
    </tr>
  )
}

function CollectionHistoryPanel() {
  const { t } = useTranslation()
  const [marketFilter, setMarketFilter] = useState<string | undefined>(undefined)
  const [expandedId, setExpandedId] = useState<number | null>(null)
  const [limit, setLimit] = useState(20)

  const { data, isLoading } = useQuery({
    queryKey: ['collection-runs', { market: marketFilter, limit }],
    queryFn: () => getCollectionRuns({ market: marketFilter, limit, offset: 0 }),
    refetchInterval: 30000,
  })

  const runs = data?.runs ?? []
  const total = data?.total ?? 0

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg">{t('collection.history')}</CardTitle>
          <div className="flex gap-1">
            <Button
              size="sm"
              variant={marketFilter == null ? 'default' : 'outline'}
              onClick={() => setMarketFilter(undefined)}
            >
              {t('collection.allMarkets')}
            </Button>
            {MARKETS.map((m) => (
              <Button
                key={m}
                size="sm"
                variant={marketFilter === m ? 'default' : 'outline'}
                onClick={() => setMarketFilter(m)}
              >
                {m.toUpperCase()}
              </Button>
            ))}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="space-y-2">
            {[1, 2, 3].map((i) => <Skeleton key={i} className="h-10 w-full" />)}
          </div>
        ) : runs.length === 0 ? (
          <p className="text-sm text-muted-foreground">{t('common.noData')}</p>
        ) : (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-left">
                    <th className="pb-2 pr-2 w-6"></th>
                    <th className="pb-2 pr-4 font-medium">Market</th>
                    <th className="pb-2 pr-4 font-medium">{t('collection.type')}</th>
                    <th className="pb-2 pr-4 font-medium">{t('common.status')}</th>
                    <th className="pb-2 pr-4 font-medium text-right">{t('collection.symbols')}</th>
                    <th className="pb-2 pr-4 font-medium text-right">{t('collection.newBars')}</th>
                    <th className="pb-2 pr-4 font-medium text-right">{t('collection.errors')}</th>
                    <th className="pb-2 pr-4 font-medium">{t('collection.duration')}</th>
                    <th className="pb-2 font-medium">{t('collection.time')}</th>
                  </tr>
                </thead>
                <tbody>
                  {runs.map((run) => (
                    <>
                      <tr
                        key={run.id}
                        className="border-b last:border-0 cursor-pointer hover:bg-muted/50 transition-colors"
                        onClick={() => setExpandedId(expandedId === run.id ? null : run.id)}
                      >
                        <td className="py-2 pr-2">
                          {run.errorCount > 0 ? (
                            expandedId === run.id
                              ? <ChevronDown className="h-4 w-4 text-muted-foreground" />
                              : <ChevronRight className="h-4 w-4 text-muted-foreground" />
                          ) : (
                            <span className="inline-block w-4" />
                          )}
                        </td>
                        <td className="py-2 pr-4 font-medium">{run.market.toUpperCase()}</td>
                        <td className="py-2 pr-4">
                          <Badge variant="outline" className="text-xs">
                            {run.triggeredBy === 'scheduler' ? t('collection.scheduled') : run.runType}
                          </Badge>
                        </td>
                        <td className="py-2 pr-4"><RunStatusBadge status={run.status} /></td>
                        <td className="py-2 pr-4 text-right tabular-nums">{formatNumber(run.symbolsTotal)}</td>
                        <td className="py-2 pr-4 text-right tabular-nums">{formatNumber(run.newBars)}</td>
                        <td className={`py-2 pr-4 text-right tabular-nums ${run.errorCount > 0 ? 'text-red-500' : ''}`}>
                          {run.errorCount}
                        </td>
                        <td className="py-2 pr-4 tabular-nums">{formatDuration(run.durationSeconds)}</td>
                        <td className="py-2 text-muted-foreground">{formatRelativeTime(run.finishedAt ?? run.startedAt)}</td>
                      </tr>
                      {expandedId === run.id && run.errorCount > 0 && (
                        <ErrorDetailRow key={`detail-${run.id}`} run={run} />
                      )}
                    </>
                  ))}
                </tbody>
              </table>
            </div>
            {runs.length < total && (
              <div className="mt-4 text-center">
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => setLimit((l) => l + 20)}
                >
                  {t('collection.loadMore')} ({runs.length}/{total})
                </Button>
              </div>
            )}
          </>
        )}
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// FundamentalsPanel
// ---------------------------------------------------------------------------

const FUND_JOB_LABELS: Record<string, string> = {
  financials: 'Financials',
  analyst_ratings: 'Analyst Ratings',
  northbound: 'Northbound',
  institutional_holders: 'Institutional Holders',
  fund_holdings: 'Fund Holdings',
}

function FundamentalsPanel() {
  const { t } = useTranslation()
  const queryClient = useQueryClient()
  const [selectedMarket, setSelectedMarket] = useState<string>('us')

  const { data: jobs, isLoading } = useQuery({
    queryKey: ['fund-progress', selectedMarket],
    queryFn: () => getFundamentalsProgress(selectedMarket),
    refetchInterval: (query) => {
      const items = query.state.data
      if (items?.some((j) => j.taskRunning)) return 3000
      return false
    },
  })

  const triggerMutation = useMutation({
    mutationFn: (jobType: string) => startFundamentals(jobType, selectedMarket),
    onSuccess: (_d, jobType) => {
      showToast(t('common.success'), `${FUND_JOB_LABELS[jobType] ?? jobType} started for ${selectedMarket.toUpperCase()}`)
      queryClient.invalidateQueries({ queryKey: ['fund-progress', selectedMarket] })
    },
    onError: (err) => showErrorToast(t('common.error'), getErrorMessage(err)),
  })

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg">{t('collection.fundamentals', 'Fundamentals')}</CardTitle>
          <div className="flex gap-1">
            {(['us', 'cn', 'hk'] as const).map((m) => (
              <Button
                key={m}
                size="sm"
                variant={selectedMarket === m ? 'default' : 'outline'}
                onClick={() => setSelectedMarket(m)}
              >
                {m.toUpperCase()}
              </Button>
            ))}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="space-y-2">
            {[1, 2, 3].map((i) => <Skeleton key={i} className="h-10 w-full" />)}
          </div>
        ) : (
          <div className="space-y-3">
            {(jobs ?? []).map((job) => {
              const p = job.progress
              const pct = p && (p.total ?? 0) > 0
                ? Math.round(((p.current ?? 0) / (p.total ?? 1)) * 100)
                : 0

              return (
                <div key={job.jobType} className="flex items-center gap-3">
                  <div className="w-40 shrink-0">
                    <span className="text-sm font-medium">{FUND_JOB_LABELS[job.jobType] ?? job.jobType}</span>
                  </div>

                  {job.taskRunning && p ? (
                    <div className="flex-1 space-y-1">
                      <Progress value={pct} className="h-1.5" />
                      <div className="flex items-center gap-3 text-xs text-muted-foreground">
                        <span>{p.current}/{p.total} ({pct}%)</span>
                        <span>{p.message}</span>
                        {p.elapsedSeconds != null && (
                          <span className="flex items-center gap-1">
                            <Timer className="h-3 w-3" />
                            {formatDuration(p.elapsedSeconds)}
                          </span>
                        )}
                        {(p.errorsCount ?? 0) > 0 && (
                          <span className="text-red-500">{p.errorsCount} errors</span>
                        )}
                        {p.estimatedRemaining != null && p.estimatedRemaining > 0 && (
                          <span className="flex items-center gap-1">
                            <Clock className="h-3 w-3" />
                            ETA {formatDuration(p.estimatedRemaining)}
                          </span>
                        )}
                      </div>
                    </div>
                  ) : (
                    <div className="flex-1">
                      <Badge variant="secondary" className="text-xs">{t('collection.idle')}</Badge>
                    </div>
                  )}

                  <Button
                    size="sm"
                    variant="ghost"
                    className="shrink-0"
                    onClick={() => triggerMutation.mutate(job.jobType)}
                    disabled={job.taskRunning || triggerMutation.isPending}
                  >
                    {job.taskRunning ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Play className="h-4 w-4" />
                    )}
                  </Button>
                </div>
              )
            })}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// SchedulePanel
// ---------------------------------------------------------------------------

function SchedulePanel() {
  const { t } = useTranslation()

  const { data: scheduler, isLoading } = useQuery({
    queryKey: ['scheduler-status'],
    queryFn: getSchedulerStatus,
    refetchInterval: 30000,
  })

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">{t('collection.schedule')}</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <div className="space-y-2">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        ) : scheduler ? (
          <div className="space-y-3">
            <div className="flex items-center gap-2">
              <span className="text-sm font-medium">Scheduler:</span>
              <Badge variant={scheduler.running ? 'default' : 'secondary'}>
                {scheduler.running ? t('common.active') : t('common.inactive')}
              </Badge>
              {scheduler.isLeader && (
                <Badge className="ml-2 bg-green-500/10 text-green-600 border-green-200">Leader</Badge>
              )}
            </div>
            <Separator />
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-left">
                    <th className="pb-2 pr-4 font-medium">{t('common.name')}</th>
                    <th className="pb-2 pr-4 font-medium">Trigger</th>
                    <th className="pb-2 pr-4 font-medium">Next Run</th>
                    <th className="pb-2 font-medium">{t('common.status')}</th>
                  </tr>
                </thead>
                <tbody>
                  {scheduler.jobs
                    .filter((job) => job.id !== 'leader_heartbeat')
                    .map((job) => (
                    <tr key={job.id} className="border-b last:border-0">
                      <td className="py-2 pr-4">{job.name}</td>
                      <td className="py-2 pr-4 font-mono text-xs">{job.trigger ?? '-'}</td>
                      <td className="py-2 pr-4 text-muted-foreground">
                        {job.nextRunTime ? job.nextRunTime.slice(0, 19) : '-'}
                      </td>
                      <td className="py-2">
                        <Badge variant={job.nextRunTime ? 'default' : 'secondary'} className="text-xs">
                          {job.nextRunTime ? t('common.active') : t('common.inactive')}
                        </Badge>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ) : (
          <p className="text-sm text-muted-foreground">{t('common.noData')}</p>
        )}
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// Main Page
// ---------------------------------------------------------------------------

export default function CollectionPage() {
  const { t } = useTranslation()
  const { isWsConnected } = useCollectionProgressWs(MARKETS)

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-3xl font-bold tracking-tight">{t('collection.title')}</h2>
        <Badge
          variant="outline"
          className={isWsConnected
            ? 'text-green-600 border-green-200'
            : 'text-muted-foreground'
          }
        >
          {isWsConnected ? (
            <><Wifi className="mr-1 h-3 w-3" /> Live</>
          ) : (
            <><WifiOff className="mr-1 h-3 w-3" /> Polling</>
          )}
        </Badge>
      </div>

      {/* Market cards */}
      <div className="grid gap-4 md:grid-cols-2">
        {MARKETS.map((market) => (
          <MarketCard key={market} market={market} isWsConnected={isWsConnected} />
        ))}
      </div>

      {/* Fundamentals */}
      <FundamentalsPanel />

      {/* Stock list / profiles */}
      <StockListPanel />

      {/* Collection history */}
      <CollectionHistoryPanel />

      {/* Schedule */}
      <SchedulePanel />
    </div>
  )
}
