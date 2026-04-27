import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Play, RefreshCw, Unlock, Loader2, List, Wifi, WifiOff } from 'lucide-react'
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
  getSchedulerStatus,
} from '@/api/admin'
import { getErrorMessage } from '@/api/client'
import { showToast, showErrorToast } from '@/stores/toastStore'
import { useCollectionProgressWs } from '@/hooks/useCollectionProgress'

const MARKETS = ['cn', 'hk', 'us', 'metal'] as const

function MarketCard({ market, isWsConnected }: { market: string; isWsConnected: boolean }) {
  const { t } = useTranslation()
  const queryClient = useQueryClient()

  const { data: progress, isLoading } = useQuery({
    queryKey: ['collection-progress', market],
    queryFn: () => getCollectionProgress(market),
    refetchInterval: (query) => {
      // Disable polling when WebSocket is delivering updates
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
      </CardContent>
    </Card>
  )
}

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

      {/* Stock list */}
      <StockListPanel />

      {/* Schedule */}
      <SchedulePanel />
    </div>
  )
}
