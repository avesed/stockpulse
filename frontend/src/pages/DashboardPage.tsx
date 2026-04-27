import { useTranslation } from 'react-i18next'
import { useQuery } from '@tanstack/react-query'
import {
  BarChart3,
  Database,
  Users,
  Server,
  Activity,
  AlertTriangle,
  CheckCircle2,
} from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { getDashboardStats, getProviders } from '@/api/admin'
import { formatNumber } from '@/lib/utils'

export default function DashboardPage() {
  const { t } = useTranslation()

  const { data: stats, isLoading: statsLoading } = useQuery({
    queryKey: ['dashboard-stats'],
    queryFn: getDashboardStats,
    refetchInterval: 30000,
  })

  const { data: providers, isLoading: providersLoading } = useQuery({
    queryKey: ['providers'],
    queryFn: getProviders,
  })

  const statCards = [
    {
      title: t('dashboard.totalBars'),
      value: stats?.totalBars,
      icon: Database,
    },
    {
      title: t('dashboard.totalSymbols'),
      value: stats?.totalSymbols,
      icon: BarChart3,
    },
    {
      title: t('dashboard.totalConsumers'),
      value: stats?.activeConsumers,
      icon: Users,
    },
    {
      title: t('dashboard.totalProviders'),
      value: stats?.enabledProviders != null && stats?.totalProviders != null
        ? `${stats.enabledProviders}/${stats.totalProviders}`
        : undefined,
      icon: Server,
    },
  ]

  const secondaryCards = [
    {
      title: t('dashboard.requestsToday'),
      value: stats?.requestsToday,
      icon: Activity,
    },
    {
      title: t('dashboard.errorsToday'),
      value: stats?.errorsToday,
      icon: AlertTriangle,
    },
  ]

  const healthBadge = (status: string) => {
    switch (status) {
      case 'healthy':
        return <Badge className="bg-green-500/10 text-green-600 border-green-200">{t('providers.healthy')}</Badge>
      case 'degraded':
        return <Badge className="bg-yellow-500/10 text-yellow-600 border-yellow-200">{t('providers.degraded')}</Badge>
      case 'error':
        return <Badge variant="destructive">{t('providers.unhealthy')}</Badge>
      default:
        return <Badge variant="secondary">{t('providers.unknown')}</Badge>
    }
  }

  return (
    <div className="space-y-6">
      <h2 className="text-3xl font-bold tracking-tight">{t('dashboard.title')}</h2>

      {/* Primary stats */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {statCards.map((card) => (
          <Card key={card.title}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">{card.title}</CardTitle>
              <card.icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              {statsLoading ? (
                <Skeleton className="h-8 w-24" />
              ) : (
                <div className="text-2xl font-bold">
                  {card.value != null ? (typeof card.value === 'number' ? formatNumber(card.value) : card.value) : '0'}
                </div>
              )}
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Secondary stats */}
      <div className="grid gap-4 md:grid-cols-2">
        {secondaryCards.map((card) => (
          <Card key={card.title}>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">{card.title}</CardTitle>
              <card.icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              {statsLoading ? (
                <Skeleton className="h-8 w-16" />
              ) : (
                <div className="text-2xl font-bold">{card.value ?? 0}</div>
              )}
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Provider health */}
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">{t('dashboard.providerHealth')}</CardTitle>
        </CardHeader>
        <CardContent>
          {providersLoading ? (
            <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
              {[1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-12 w-full" />
              ))}
            </div>
          ) : providers && providers.length > 0 ? (
            <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
              {providers.map((provider) => (
                <div
                  key={provider.id}
                  className="flex items-center justify-between rounded-lg border p-3"
                >
                  <div className="flex items-center gap-3">
                    {provider.isEnabled ? (
                      <CheckCircle2 className="h-4 w-4 text-green-500" />
                    ) : (
                      <AlertTriangle className="h-4 w-4 text-muted-foreground" />
                    )}
                    <div>
                      <p className="text-sm font-medium">{provider.displayName}</p>
                      <p className="text-xs text-muted-foreground">{provider.providerName}</p>
                    </div>
                  </div>
                  {healthBadge(provider.healthStatus)}
                </div>
              ))}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">{t('common.noData')}</p>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
