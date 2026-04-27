import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useQuery } from '@tanstack/react-query'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { getRequestStats } from '@/api/admin'
import { formatNumber } from '@/lib/utils'

export default function RequestStatsPage() {
  const { t } = useTranslation()
  const [days, setDays] = useState(7)

  const { data: stats, isLoading } = useQuery({
    queryKey: ['request-stats', days],
    queryFn: () => getRequestStats(days),
  })

  // Aggregate totals from per-consumer stats
  const totals = stats?.reduce(
    (acc, row) => ({
      requests: acc.requests + (row.requests ?? 0),
      errors: acc.errors + (row.errors ?? 0),
    }),
    { requests: 0, errors: 0 }
  )

  // Group by date for display
  const byDate = new Map<string, { requests: number; errors: number }>()
  if (stats) {
    for (const row of stats) {
      const existing = byDate.get(row.date) ?? { requests: 0, errors: 0 }
      existing.requests += row.requests ?? 0
      existing.errors += row.errors ?? 0
      byDate.set(row.date, existing)
    }
  }
  const dateRows = [...byDate.entries()].sort((a, b) => b[0].localeCompare(a[0]))

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-3xl font-bold tracking-tight">{t('stats.title')}</h2>
        <div className="flex gap-2">
          <Button
            size="sm"
            variant={days === 7 ? 'default' : 'outline'}
            onClick={() => setDays(7)}
          >
            {t('stats.last7Days')}
          </Button>
          <Button
            size="sm"
            variant={days === 30 ? 'default' : 'outline'}
            onClick={() => setDays(30)}
          >
            {t('stats.last30Days')}
          </Button>
        </div>
      </div>

      {/* Summary cards */}
      {totals && (
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">{t('stats.totalRequests')}</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{formatNumber(totals.requests)}</div>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium">{t('stats.errorCount')}</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-destructive">
                {formatNumber(totals.errors)}
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      {/* Stats table by date */}
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">{t('stats.dailyBreakdown')}</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="space-y-3">
              {[1, 2, 3, 4, 5].map((i) => (
                <Skeleton key={i} className="h-10 w-full" />
              ))}
            </div>
          ) : dateRows.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-left">
                    <th className="pb-3 pr-4 font-medium">{t('stats.date')}</th>
                    <th className="pb-3 pr-4 font-medium text-right">{t('stats.totalRequests')}</th>
                    <th className="pb-3 pr-4 font-medium text-right">{t('stats.errorCount')}</th>
                    <th className="pb-3 font-medium text-right">{t('stats.errorRate')}</th>
                  </tr>
                </thead>
                <tbody>
                  {dateRows.map(([date, row]) => {
                    const errorRate = row.requests > 0
                      ? ((row.errors / row.requests) * 100).toFixed(1)
                      : '0.0'
                    return (
                      <tr key={date} className="border-b last:border-0">
                        <td className="py-3 pr-4">{date}</td>
                        <td className="py-3 pr-4 text-right font-medium">
                          {formatNumber(row.requests)}
                        </td>
                        <td className="py-3 pr-4 text-right">
                          <span className={row.errors > 0 ? 'text-destructive' : ''}>
                            {formatNumber(row.errors)}
                          </span>
                        </td>
                        <td className="py-3 text-right">
                          {row.errors > 0 ? (
                            <Badge variant="destructive" className="text-xs">
                              {errorRate}%
                            </Badge>
                          ) : (
                            <span className="text-muted-foreground">0%</span>
                          )}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">{t('common.noData')}</p>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
