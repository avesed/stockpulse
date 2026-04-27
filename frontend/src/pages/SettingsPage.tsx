import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Plus, Pencil, Trash2, Loader2 } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { Skeleton } from '@/components/ui/skeleton'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { getSettings, upsertSetting, deleteSetting } from '@/api/admin'
import { getErrorMessage } from '@/api/client'
import { showToast, showErrorToast } from '@/stores/toastStore'

export default function SettingsPage() {
  const { t } = useTranslation()
  const queryClient = useQueryClient()

  const [dialogOpen, setDialogOpen] = useState(false)
  const [editMode, setEditMode] = useState(false)
  const [formKey, setFormKey] = useState('')
  const [formValue, setFormValue] = useState('')

  const { data: settings, isLoading } = useQuery({
    queryKey: ['settings'],
    queryFn: getSettings,
  })

  const upsertMutation = useMutation({
    mutationFn: ({ key, value }: { key: string; value: string }) =>
      upsertSetting(key, value),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['settings'] })
      showToast(t('common.success'))
      closeDialog()
    },
    onError: (err: unknown) => showErrorToast(t('common.error'), getErrorMessage(err)),
  })

  const deleteMutation = useMutation({
    mutationFn: deleteSetting,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['settings'] })
      showToast(t('common.success'))
    },
    onError: (err: unknown) => showErrorToast(t('common.error'), getErrorMessage(err)),
  })

  const closeDialog = () => {
    setDialogOpen(false)
    setEditMode(false)
    setFormKey('')
    setFormValue('')
  }

  const openCreate = () => {
    setEditMode(false)
    setFormKey('')
    setFormValue('')
    setDialogOpen(true)
  }

  const openEdit = (key: string, value: string) => {
    setEditMode(true)
    setFormKey(key)
    setFormValue(value)
    setDialogOpen(true)
  }

  const handleSubmit = () => {
    upsertMutation.mutate({ key: formKey, value: formValue })
  }

  const handleDelete = (key: string) => {
    if (window.confirm(t('settings.confirmDelete'))) {
      deleteMutation.mutate(key)
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-3xl font-bold tracking-tight">{t('settings.title')}</h2>
        <Button onClick={openCreate}>
          <Plus className="mr-2 h-4 w-4" />
          {t('settings.addSetting')}
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-lg">{t('settings.title')}</CardTitle>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="space-y-3">
              {[1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-12 w-full" />
              ))}
            </div>
          ) : settings && settings.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b text-left">
                    <th className="pb-3 pr-4 font-medium">{t('common.key')}</th>
                    <th className="pb-3 pr-4 font-medium">{t('common.value')}</th>
                    <th className="pb-3 pr-4 font-medium">Updated</th>
                    <th className="pb-3 font-medium">{t('common.actions')}</th>
                  </tr>
                </thead>
                <tbody>
                  {settings.map((setting) => (
                    <tr key={setting.key} className="border-b last:border-0">
                      <td className="py-3 pr-4 font-mono text-xs">{setting.key}</td>
                      <td className="max-w-xs truncate py-3 pr-4">{setting.value}</td>
                      <td className="py-3 pr-4 text-muted-foreground text-xs">
                        {setting.updatedAt ?? '-'}
                      </td>
                      <td className="py-3">
                        <div className="flex gap-1">
                          <Button
                            size="icon"
                            variant="ghost"
                            className="h-8 w-8"
                            onClick={() => openEdit(setting.key, setting.value)}
                          >
                            <Pencil className="h-4 w-4" />
                          </Button>
                          <Button
                            size="icon"
                            variant="ghost"
                            className="h-8 w-8 text-destructive"
                            onClick={() => handleDelete(setting.key)}
                            disabled={deleteMutation.isPending}
                          >
                            <Trash2 className="h-4 w-4" />
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

      {/* Create/Edit Dialog */}
      <Dialog open={dialogOpen} onOpenChange={(open) => { if (!open) closeDialog() }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {editMode ? t('settings.editSetting') : t('settings.addSetting')}
            </DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label>{t('common.key')}</Label>
              <Input
                value={formKey}
                onChange={(e) => setFormKey(e.target.value)}
                disabled={editMode}
                placeholder="setting_key"
                className="font-mono"
              />
            </div>
            <div className="space-y-2">
              <Label>{t('common.value')}</Label>
              <Textarea
                value={formValue}
                onChange={(e) => setFormValue(e.target.value)}
                placeholder="Setting value"
                rows={3}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={closeDialog}>
              {t('common.cancel')}
            </Button>
            <Button onClick={handleSubmit} disabled={!formKey || !formValue || upsertMutation.isPending}>
              {upsertMutation.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
              {t('common.save')}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
