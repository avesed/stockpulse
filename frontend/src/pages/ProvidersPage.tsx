import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Loader2, CheckCircle2, XCircle, AlertTriangle, Plug } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { getProviders, toggleProvider, testProvider, updateProvider } from '@/api/admin'
import { getErrorMessage } from '@/api/client'
import { showToast, showErrorToast } from '@/stores/toastStore'

function HealthIcon({ status }: { status: string }) {
  switch (status) {
    case 'healthy':
      return <CheckCircle2 className="h-5 w-5 text-green-500" />
    case 'degraded':
      return <AlertTriangle className="h-5 w-5 text-yellow-500" />
    case 'error':
      return <XCircle className="h-5 w-5 text-red-500" />
    default:
      return <Plug className="h-5 w-5 text-muted-foreground" />
  }
}

export default function ProvidersPage() {
  const { t } = useTranslation()
  const queryClient = useQueryClient()

  const [editingProvider, setEditingProvider] = useState<number | null>(null)
  const [editApiKey, setEditApiKey] = useState('')
  const [testResults, setTestResults] = useState<Record<number, { success: boolean; message: string }>>({})
  const [testingIds, setTestingIds] = useState<Set<number>>(new Set())

  const { data: providers, isLoading } = useQuery({
    queryKey: ['providers'],
    queryFn: getProviders,
  })

  const toggleMutation = useMutation({
    mutationFn: ({ id, enabled }: { id: number; enabled: boolean }) =>
      toggleProvider(id, enabled),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['providers'] })
      showToast(t('common.success'))
    },
    onError: (err) => showErrorToast(t('common.error'), getErrorMessage(err)),
  })

  const updateMutation = useMutation({
    mutationFn: ({ id, apiKey }: { id: number; apiKey: string }) =>
      updateProvider(id, { apiKey }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['providers'] })
      showToast(t('common.success'))
      setEditingProvider(null)
      setEditApiKey('')
    },
    onError: (err) => showErrorToast(t('common.error'), getErrorMessage(err)),
  })

  const handleTest = async (id: number) => {
    setTestingIds((prev) => new Set(prev).add(id))
    try {
      const result = await testProvider(id)
      setTestResults((prev) => ({ ...prev, [id]: result }))
    } catch (err) {
      setTestResults((prev) => ({
        ...prev,
        [id]: { success: false, message: getErrorMessage(err) },
      }))
    } finally {
      setTestingIds((prev) => {
        const next = new Set(prev)
        next.delete(id)
        return next
      })
    }
  }

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
      <h2 className="text-3xl font-bold tracking-tight">{t('providers.title')}</h2>

      {isLoading ? (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {[1, 2, 3, 4].map((i) => (
            <Skeleton key={i} className="h-48" />
          ))}
        </div>
      ) : providers && providers.length > 0 ? (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {providers.map((provider) => (
            <Card key={provider.id}>
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <HealthIcon status={provider.healthStatus} />
                    <CardTitle className="text-base">{provider.displayName}</CardTitle>
                  </div>
                  {healthBadge(provider.healthStatus)}
                </div>
                <CardDescription>{provider.providerName}</CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">API Key</span>
                  <Badge variant={provider.hasApiKey ? 'default' : 'secondary'} className="text-xs">
                    {provider.hasApiKey ? t('common.configured') : t('common.notConfigured')}
                  </Badge>
                </div>
                <div className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">{t('common.status')}</span>
                  <Badge variant={provider.isEnabled ? 'default' : 'secondary'} className="text-xs">
                    {provider.isEnabled ? t('common.enabled') : t('common.disabled')}
                  </Badge>
                </div>

                {/* Test result inline */}
                {testResults[provider.id] != null && (
                  <div
                    className={`rounded-md p-2 text-xs ${
                      testResults[provider.id]?.success
                        ? 'bg-green-500/10 text-green-600'
                        : 'bg-destructive/10 text-destructive'
                    }`}
                  >
                    {testResults[provider.id]?.message}
                  </div>
                )}

                <div className="flex gap-2 pt-1">
                  <Button
                    size="sm"
                    variant="outline"
                    className="flex-1"
                    onClick={() => handleTest(provider.id)}
                    disabled={testingIds.has(provider.id)}
                  >
                    {testingIds.has(provider.id) ? (
                      <>
                        <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                        {t('providers.testing')}
                      </>
                    ) : (
                      t('providers.testConnection')
                    )}
                  </Button>
                  <Button
                    size="sm"
                    variant={provider.isEnabled ? 'secondary' : 'default'}
                    onClick={() =>
                      toggleMutation.mutate({
                        id: provider.id,
                        enabled: !provider.isEnabled,
                      })
                    }
                    disabled={toggleMutation.isPending}
                  >
                    {provider.isEnabled ? t('common.disable') : t('common.enable')}
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => {
                      setEditingProvider(provider.id)
                      setEditApiKey('')
                    }}
                  >
                    {t('common.edit')}
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      ) : (
        <p className="text-sm text-muted-foreground">{t('common.noData')}</p>
      )}

      {/* Edit API Key Dialog */}
      <Dialog
        open={editingProvider !== null}
        onOpenChange={(open) => {
          if (!open) {
            setEditingProvider(null)
            setEditApiKey('')
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t('providers.editApiKey')}</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label>API Key</Label>
              <Input
                type="password"
                value={editApiKey}
                onChange={(e) => setEditApiKey(e.target.value)}
                placeholder="Enter new API key"
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setEditingProvider(null)}>
              {t('common.cancel')}
            </Button>
            <Button
              onClick={() => {
                if (editingProvider) {
                  updateMutation.mutate({ id: editingProvider, apiKey: editApiKey })
                }
              }}
              disabled={!editApiKey || updateMutation.isPending}
            >
              {updateMutation.isPending && (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              )}
              {t('common.save')}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
