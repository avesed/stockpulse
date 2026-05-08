import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, Copy, Check, Loader2 } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { getConsumers, createConsumer, deactivateConsumer, deleteConsumer } from '@/api/admin'
import { getErrorMessage } from '@/api/client'
import { showToast, showErrorToast } from '@/stores/toastStore'
import { formatDate } from '@/lib/utils'

export default function ConsumersPage() {
  const { t } = useTranslation()
  const queryClient = useQueryClient()

  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDescription, setNewDescription] = useState('')
  const [newRateLimit, setNewRateLimit] = useState('60')
  const [createdKey, setCreatedKey] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)

  const { data: consumers, isLoading } = useQuery({
    queryKey: ['consumers'],
    queryFn: getConsumers,
  })

  const createMutation = useMutation({
    mutationFn: createConsumer,
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: ['consumers'] })
      setCreatedKey(data.rawApiKey ?? null)
      setNewName('')
      setNewDescription('')
      setNewRateLimit('60')
    },
    onError: (err) => showErrorToast(t('common.error'), getErrorMessage(err)),
  })

  const [deleteTarget, setDeleteTarget] = useState<string | null>(null)

  const deactivateMutation = useMutation({
    mutationFn: (id: string) => deactivateConsumer(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['consumers'] })
      showToast(t('common.success'))
    },
    onError: (err) => showErrorToast(t('common.error'), getErrorMessage(err)),
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => deleteConsumer(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['consumers'] })
      showToast(t('common.success'))
      setDeleteTarget(null)
    },
    onError: (err) => showErrorToast(t('common.error'), getErrorMessage(err)),
  })

  const handleCreate = () => {
    const payload: { name: string; description?: string; rateLimit?: number } = {
      name: newName,
      rateLimit: parseInt(newRateLimit, 10) || 100,
    }
    if (newDescription) {
      payload.description = newDescription
    }
    createMutation.mutate(payload)
  }

  const handleCopy = async (text: string) => {
    await navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const handleDialogClose = (open: boolean) => {
    if (!open) {
      setCreatedKey(null)
      setCopied(false)
    }
    setCreateOpen(open)
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-3xl font-bold tracking-tight">{t('consumers.title')}</h2>
        <Dialog open={createOpen} onOpenChange={handleDialogClose}>
          <DialogTrigger asChild>
            <Button>
              <Plus className="mr-2 h-4 w-4" />
              {t('consumers.createConsumer')}
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>{t('consumers.createConsumer')}</DialogTitle>
              {!createdKey && (
                <DialogDescription>
                  Create a new API consumer to access StockPulse data.
                </DialogDescription>
              )}
            </DialogHeader>

            {createdKey ? (
              <div className="space-y-4">
                <Alert>
                  <AlertDescription>{t('consumers.apiKeyWarning')}</AlertDescription>
                </Alert>
                <div className="flex items-center gap-2">
                  <Input value={createdKey} readOnly className="font-mono text-xs" />
                  <Button
                    size="icon"
                    variant="outline"
                    onClick={() => handleCopy(createdKey)}
                  >
                    {copied ? (
                      <Check className="h-4 w-4" />
                    ) : (
                      <Copy className="h-4 w-4" />
                    )}
                  </Button>
                </div>
                <DialogFooter>
                  <Button onClick={() => handleDialogClose(false)}>
                    {t('common.confirm')}
                  </Button>
                </DialogFooter>
              </div>
            ) : (
              <div className="space-y-4">
                <div className="space-y-2">
                  <Label>{t('common.name')}</Label>
                  <Input
                    value={newName}
                    onChange={(e) => setNewName(e.target.value)}
                    placeholder="Consumer name"
                  />
                </div>
                <div className="space-y-2">
                  <Label>{t('common.description')}</Label>
                  <Input
                    value={newDescription}
                    onChange={(e) => setNewDescription(e.target.value)}
                    placeholder="Optional description"
                  />
                </div>
                <div className="space-y-2">
                  <Label>{t('consumers.rateLimit')} ({t('consumers.perMinute')})</Label>
                  <Input
                    type="number"
                    value={newRateLimit}
                    onChange={(e) => setNewRateLimit(e.target.value)}
                  />
                </div>
                <DialogFooter>
                  <Button variant="outline" onClick={() => handleDialogClose(false)}>
                    {t('common.cancel')}
                  </Button>
                  <Button
                    onClick={handleCreate}
                    disabled={!newName || createMutation.isPending}
                  >
                    {createMutation.isPending && (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    )}
                    {t('common.create')}
                  </Button>
                </DialogFooter>
              </div>
            )}
          </DialogContent>
        </Dialog>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-lg">{t('consumers.title')}</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="space-y-3">
              {[1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-16 w-full" />
              ))}
            </div>
          ) : consumers && consumers.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-left">
                    <th className="pb-3 pr-4 font-medium">{t('common.name')}</th>
                    <th className="pb-3 pr-4 font-medium">{t('common.description')}</th>
                    <th className="pb-3 pr-4 font-medium">Key Prefix</th>
                    <th className="pb-3 pr-4 font-medium">{t('consumers.rateLimit')}</th>
                    <th className="pb-3 pr-4 font-medium">{t('consumers.lastUsed')}</th>
                    <th className="pb-3 pr-4 font-medium">{t('common.status')}</th>
                    <th className="pb-3 font-medium">{t('common.actions')}</th>
                  </tr>
                </thead>
                <tbody>
                  {consumers.map((consumer) => (
                    <tr key={consumer.id} className="border-b last:border-0">
                      <td className="py-3 pr-4 font-medium">{consumer.name}</td>
                      <td className="py-3 pr-4 text-muted-foreground">
                        {consumer.description ?? '-'}
                      </td>
                      <td className="py-3 pr-4 font-mono text-xs">{consumer.apiKeyPrefix}</td>
                      <td className="py-3 pr-4">
                        {consumer.rateLimit}{t('consumers.perMinute')}
                      </td>
                      <td className="py-3 pr-4 text-muted-foreground">
                        {consumer.lastUsedAt ? formatDate(consumer.lastUsedAt) : '-'}
                      </td>
                      <td className="py-3 pr-4">
                        <Badge variant={consumer.isActive ? 'default' : 'secondary'}>
                          {consumer.isActive ? t('common.active') : t('common.inactive')}
                        </Badge>
                      </td>
                      <td className="py-3">
                        <div className="flex gap-1">
                          <Button
                            size="sm"
                            variant="destructive"
                            onClick={() => deactivateMutation.mutate(consumer.id)}
                            disabled={!consumer.isActive || deactivateMutation.isPending}
                          >
                            {t('consumers.deactivate')}
                          </Button>
                          <Button
                            size="sm"
                            variant="ghost"
                            className="text-destructive hover:text-destructive"
                            onClick={() => setDeleteTarget(consumer.id)}
                            disabled={deleteMutation.isPending}
                          >
                            {t('consumers.deleteConsumer')}
                          </Button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">{t('common.noData')}</p>
          )}
        </CardContent>
      </Card>

      <Dialog open={deleteTarget !== null} onOpenChange={(open) => { if (!open) setDeleteTarget(null) }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t('common.confirm')}</DialogTitle>
            <DialogDescription>{t('consumers.confirmDelete')}</DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteTarget(null)}>{t('common.cancel')}</Button>
            <Button
              variant="destructive"
              onClick={() => deleteTarget && deleteMutation.mutate(deleteTarget)}
              disabled={deleteMutation.isPending}
            >
              {deleteMutation.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              {t('common.delete')}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
