import { useState } from 'react'
import { Link, Outlet, useLocation } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
import {
  LayoutDashboard,
  Database,
  Server,
  Users,
  BarChart3,
  Settings,
  ChevronLeft,
  ChevronRight,
  LogOut,
  Sun,
  Moon,
  Monitor,
  KeyRound,
  Loader2,
} from 'lucide-react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Separator } from '@/components/ui/separator'
import { ScrollArea } from '@/components/ui/scroll-area'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { useAuthStore } from '@/stores/authStore'
import { useThemeStore } from '@/stores/themeStore'
import { changePassword } from '@/api/auth'
import { getErrorMessage } from '@/api/client'
import { showToast, showErrorToast } from '@/stores/toastStore'

const navItems = [
  { path: '/', icon: LayoutDashboard, labelKey: 'nav.dashboard' },
  { path: '/collection', icon: Database, labelKey: 'nav.collection' },
  { path: '/providers', icon: Server, labelKey: 'nav.providers' },
  { path: '/consumers', icon: Users, labelKey: 'nav.consumers' },
  { path: '/stats', icon: BarChart3, labelKey: 'nav.stats' },
  { path: '/settings', icon: Settings, labelKey: 'nav.settings' },
]

export function AdminLayout() {
  const [collapsed, setCollapsed] = useState(false)
  const [pwdOpen, setPwdOpen] = useState(false)
  const [currentPwd, setCurrentPwd] = useState('')
  const [newPwd, setNewPwd] = useState('')
  const [confirmPwd, setConfirmPwd] = useState('')
  const [pwdError, setPwdError] = useState('')
  const [pwdSubmitting, setPwdSubmitting] = useState(false)
  const location = useLocation()
  const { t } = useTranslation()
  const { user, logout } = useAuthStore()
  const { theme, setTheme } = useThemeStore()

  const handleLogout = async () => {
    await logout()
  }

  const resetPwdForm = () => {
    setCurrentPwd('')
    setNewPwd('')
    setConfirmPwd('')
    setPwdError('')
  }

  const handleChangePassword = async () => {
    setPwdError('')
    if (newPwd !== confirmPwd) {
      setPwdError(t('changePassword.mismatch'))
      return
    }
    setPwdSubmitting(true)
    try {
      await changePassword({ currentPassword: currentPwd, newPassword: newPwd })
      showToast(t('common.success'), t('changePassword.success'))
      setPwdOpen(false)
      resetPwdForm()
    } catch (err) {
      setPwdError(getErrorMessage(err))
    } finally {
      setPwdSubmitting(false)
    }
  }

  const cycleTheme = () => {
    if (theme === 'light') setTheme('dark')
    else if (theme === 'dark') setTheme('system')
    else setTheme('light')
  }

  const themeIcon = theme === 'light' ? Sun : theme === 'dark' ? Moon : Monitor

  return (
    <TooltipProvider delayDuration={0}>
      <div className="flex h-screen overflow-hidden">
        {/* Sidebar */}
        <aside
          className={cn(
            'flex flex-col border-r bg-card transition-all duration-300',
            collapsed ? 'w-16' : 'w-64'
          )}
        >
          {/* Logo */}
          <div className="flex h-14 items-center border-b px-4">
            {!collapsed && (
              <Link to="/" className="flex items-center gap-2">
                <Database className="h-6 w-6 text-primary" />
                <span className="text-lg font-bold">StockPulse</span>
              </Link>
            )}
            {collapsed && (
              <Link to="/" className="mx-auto">
                <Database className="h-6 w-6 text-primary" />
              </Link>
            )}
          </div>

          {/* Nav */}
          <ScrollArea className="flex-1 py-2">
            <nav className="flex flex-col gap-1 px-2">
              {navItems.map((item) => {
                const isActive =
                  item.path === '/'
                    ? location.pathname === '/'
                    : location.pathname.startsWith(item.path)
                const Icon = item.icon

                if (collapsed) {
                  return (
                    <Tooltip key={item.path}>
                      <TooltipTrigger asChild>
                        <Link to={item.path}>
                          <Button
                            variant={isActive ? 'secondary' : 'ghost'}
                            size="icon"
                            className="w-full"
                          >
                            <Icon className="h-5 w-5" />
                          </Button>
                        </Link>
                      </TooltipTrigger>
                      <TooltipContent side="right">
                        {t(item.labelKey)}
                      </TooltipContent>
                    </Tooltip>
                  )
                }

                return (
                  <Link key={item.path} to={item.path}>
                    <Button
                      variant={isActive ? 'secondary' : 'ghost'}
                      className="w-full justify-start gap-3"
                    >
                      <Icon className="h-5 w-5" />
                      {t(item.labelKey)}
                    </Button>
                  </Link>
                )
              })}
            </nav>
          </ScrollArea>

          {/* Collapse toggle */}
          <Separator />
          <div className="p-2">
            <Button
              variant="ghost"
              size="icon"
              className="w-full"
              onClick={() => setCollapsed(!collapsed)}
            >
              {collapsed ? (
                <ChevronRight className="h-4 w-4" />
              ) : (
                <ChevronLeft className="h-4 w-4" />
              )}
            </Button>
          </div>
        </aside>

        {/* Main content */}
        <div className="flex flex-1 flex-col overflow-hidden">
          {/* Header */}
          <header className="flex h-14 items-center justify-between border-b px-6">
            <h1 className="text-lg font-semibold">StockPulse Admin</h1>
            <div className="flex items-center gap-2">
              <Button variant="ghost" size="icon" onClick={cycleTheme}>
                {(() => {
                  const ThemeIcon = themeIcon
                  return <ThemeIcon className="h-5 w-5" />
                })()}
              </Button>

              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="ghost" size="sm" className="gap-2">
                    <div className="flex h-7 w-7 items-center justify-center rounded-full bg-primary text-primary-foreground text-xs font-medium">
                      {(user?.displayName ?? user?.email ?? '?').charAt(0).toUpperCase()}
                    </div>
                    {!collapsed && (
                      <span className="text-sm">{user?.displayName ?? user?.email?.split('@')[0]}</span>
                    )}
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  <DropdownMenuLabel>{user?.displayName ?? user?.email?.split('@')[0]}</DropdownMenuLabel>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem onClick={() => { resetPwdForm(); setPwdOpen(true) }}>
                    <KeyRound className="mr-2 h-4 w-4" />
                    {t('changePassword.title')}
                  </DropdownMenuItem>
                  <DropdownMenuItem onClick={handleLogout}>
                    <LogOut className="mr-2 h-4 w-4" />
                    {t('common.logout')}
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          </header>

          {/* Page content */}
          <main className="flex-1 overflow-auto">
            <div className="container py-6">
              <Outlet />
            </div>
          </main>
        </div>

        <Dialog open={pwdOpen} onOpenChange={(open) => { setPwdOpen(open); if (!open) resetPwdForm() }}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>{t('changePassword.title')}</DialogTitle>
            </DialogHeader>
            <div className="space-y-4">
              {pwdError && (
                <div className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">
                  {pwdError}
                </div>
              )}
              <div className="space-y-2">
                <Label>{t('changePassword.currentPassword')}</Label>
                <Input type="password" value={currentPwd} onChange={(e) => setCurrentPwd(e.target.value)} />
              </div>
              <div className="space-y-2">
                <Label>{t('changePassword.newPassword')}</Label>
                <Input type="password" value={newPwd} onChange={(e) => setNewPwd(e.target.value)} />
              </div>
              <div className="space-y-2">
                <Label>{t('changePassword.confirmPassword')}</Label>
                <Input type="password" value={confirmPwd} onChange={(e) => setConfirmPwd(e.target.value)} />
              </div>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setPwdOpen(false)}>{t('common.cancel')}</Button>
              <Button onClick={handleChangePassword} disabled={!currentPwd || !newPwd || !confirmPwd || pwdSubmitting}>
                {pwdSubmitting && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                {t('common.save')}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>
    </TooltipProvider>
  )
}
