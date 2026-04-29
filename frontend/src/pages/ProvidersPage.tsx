import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Loader2, CheckCircle2, XCircle, AlertTriangle, Plug, KeyRound, Plus, Trash2 } from 'lucide-react'
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
import type { ProviderConfig } from '@/types'

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

// Providers that support multi-key configuration
const MULTI_KEY_PROVIDERS = new Set(['finnhub'])

export default function ProvidersPage() {
  const { t } = useTranslation()
  const queryClient = useQueryClient()

  const [editingProvider, setEditingProvider] = useState<ProviderConfig | null>(null)
  // For multi-key providers: array of key strings; index 0 = primary
  const [editKeys, setEditKeys] = useState<string[]>([''])
  // For single-key providers
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
    mutationFn: ({ id, data }: { id: number; data: { apiKey?: string; configJson?: Record<string, unknown> } }) =>
      updateProvider(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['providers'] })
      showToast(t('common.success'))
      closeDialog()
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

  const openEditDialog = (provider: ProviderConfig) => {
    setEditingProvider(provider)
    if (MULTI_KEY_PROVIDERS.has(provider.providerName)) {
      // Build key list: first slot is primary (empty = unchanged), rest from config_json
      const extras = (provider.configJson as { extra_api_keys?: string[] })?.extra_api_keys
      const keys = ['']  // primary key placeholder (masked)
      if (Array.isArray(extras)) {
        keys.push(...extras)
      }
      setEditKeys(keys)
      setEditApiKey('')
    } else {
      setEditApiKey('')
      setEditKeys([''])
    }
  }

  const closeDialog = () => {
    setEditingProvider(null)
    setEditApiKey('')
    setEditKeys([''])
  }

  // Multi-key list operations
  const updateKey = (index: number, value: string) => {
    setEditKeys((prev) => prev.map((k, i) => (i === index ? value : k)))
  }

  const addKey = () => {
    setEditKeys((prev) => [...prev, ''])
  }

  const removeKey = (index: number) => {
    if (index === 0) return // cannot remove primary
    setEditKeys((prev) => prev.filter((_, i) => i !== index))
  }

  const handleSave = () => {
    if (!editingProvider) return

    const isMultiKey = MULTI_KEY_PROVIDERS.has(editingProvider.providerName)

    if (isMultiKey) {
      const payload: { apiKey?: string; configJson?: Record<string, unknown> } = {}

      // Primary key: only send if user typed something (index 0)
      const primaryKey = editKeys[0]?.trim()
      if (primaryKey) {
        payload.apiKey = primaryKey
      }

      // Extra keys: indices 1+
      const extraKeys = editKeys.slice(1).map((k) => k.trim()).filter(Boolean)
      const existingConfig = (editingProvider.configJson as Record<string, unknown>) || {}

      if (extraKeys.length > 0) {
        payload.configJson = { ...existingConfig, extra_api_keys: extraKeys }
      } else if (existingConfig.extra_api_keys) {
        const { extra_api_keys: _, ...rest } = existingConfig
        payload.configJson = Object.keys(rest).length > 0 ? rest : {}
      }

      if (!payload.apiKey && !payload.configJson) return
      updateMutation.mutate({ id: editingProvider.id, data: payload })
    } else {
      if (!editApiKey) return
      updateMutation.mutate({ id: editingProvider.id, data: { apiKey: editApiKey } })
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

  // Compute hasChanges for save button
  const isMultiKey = editingProvider ? MULTI_KEY_PROVIDERS.has(editingProvider.providerName) : false
  const hasChanges = isMultiKey
    ? (() => {
        // Primary key changed?
        if (editKeys[0]?.trim()) return true
        // Extra keys changed?
        const oldExtras = (editingProvider?.configJson as { extra_api_keys?: string[] })?.extra_api_keys || []
        const newExtras = editKeys.slice(1).map((k) => k.trim()).filter(Boolean)
        if (oldExtras.length !== newExtras.length) return true
        return oldExtras.some((k, i) => k !== newExtras[i])
      })()
    : !!editApiKey

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
                  <div className="flex items-center gap-1.5">
                    {provider.apiKeysCount > 1 && (
                      <Badge variant="outline" className="text-xs gap-1">
                        <KeyRound className="h-3 w-3" />
                        {t('providers.keysCount', { count: provider.apiKeysCount })}
                      </Badge>
                    )}
                    <Badge variant={provider.hasApiKey ? 'default' : 'secondary'} className="text-xs">
                      {provider.hasApiKey ? t('common.configured') : t('common.notConfigured')}
                    </Badge>
                  </div>
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
                    onClick={() => openEditDialog(provider)}
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
          if (!open) closeDialog()
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {isMultiKey ? t('providers.editApiKeys') : t('providers.editApiKey')}
              {editingProvider && (
                <span className="ml-2 text-sm font-normal text-muted-foreground">
                  — {editingProvider.displayName}
                </span>
              )}
            </DialogTitle>
          </DialogHeader>

          {isMultiKey ? (
            /* Multi-key: list of inputs, first = primary */
            <div className="space-y-3">
              {editKeys.map((key, index) => (
                <div key={index} className="flex items-center gap-2">
                  <div className="flex-1 space-y-1">
                    {index === 0 && (
                      <Label className="text-xs text-muted-foreground">{t('providers.primaryKey')}</Label>
                    )}
                    <Input
                      type="password"
                      value={key}
                      onChange={(e) => updateKey(index, e.target.value)}
                      placeholder={index === 0 && editingProvider?.hasApiKey ? '••••••••' : `API Key ${index + 1}`}
                    />
                  </div>
                  {index > 0 && (
                    <Button
                      size="icon"
                      variant="ghost"
                      className="h-9 w-9 shrink-0 text-muted-foreground hover:text-destructive"
                      onClick={() => removeKey(index)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  )}
                </div>
              ))}
              <Button
                variant="outline"
                size="sm"
                className="w-full"
                onClick={addKey}
              >
                <Plus className="mr-1 h-3.5 w-3.5" />
                {t('providers.addKey')}
              </Button>
              <p className="text-xs text-muted-foreground">
                {t('providers.extraKeysHint')}
              </p>
            </div>
          ) : (
            /* Single-key */
            <div className="space-y-4">
              <div className="space-y-2">
                <Label>API Key</Label>
                <Input
                  type="password"
                  value={editApiKey}
                  onChange={(e) => setEditApiKey(e.target.value)}
                  placeholder={editingProvider?.hasApiKey ? '••••••••' : 'Enter API key'}
                />
              </div>
            </div>
          )}

          <DialogFooter>
            <Button variant="outline" onClick={closeDialog}>
              {t('common.cancel')}
            </Button>
            <Button
              onClick={handleSave}
              disabled={!hasChanges || updateMutation.isPending}
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
