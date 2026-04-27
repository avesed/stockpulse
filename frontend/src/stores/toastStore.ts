import { toast } from '@/hooks/useToast'

export function showToast(title: string, description?: string) {
  toast({ title, description })
}

export function showErrorToast(title: string, description?: string) {
  toast({ title, description, variant: 'destructive' })
}
