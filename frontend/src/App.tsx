import { lazy, Suspense, useEffect } from 'react'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { AdminLayout } from '@/components/layout/AdminLayout'
import { ProtectedRoute } from '@/components/layout/ProtectedRoute'
import { Toaster } from '@/components/ui/toaster'
import { useAuthStore } from '@/stores/authStore'
import { useThemeStore } from '@/stores/themeStore'
import { Skeleton } from '@/components/ui/skeleton'

const LoginPage = lazy(() => import('@/pages/LoginPage'))
const DashboardPage = lazy(() => import('@/pages/DashboardPage'))
const CollectionPage = lazy(() => import('@/pages/CollectionPage'))
const ProvidersPage = lazy(() => import('@/pages/ProvidersPage'))
const ConsumersPage = lazy(() => import('@/pages/ConsumersPage'))
const SettingsPage = lazy(() => import('@/pages/SettingsPage'))
const RequestStatsPage = lazy(() => import('@/pages/RequestStatsPage'))

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      retry: 1,
    },
  },
})

function PageLoader() {
  return (
    <div className="flex items-center justify-center p-12">
      <div className="space-y-4 w-64">
        <Skeleton className="h-8 w-full" />
        <Skeleton className="h-4 w-3/4" />
        <Skeleton className="h-4 w-1/2" />
      </div>
    </div>
  )
}

function AppInit({ children }: { children: React.ReactNode }) {
  const initAuth = useAuthStore((s) => s.initAuth)
  const initTheme = useThemeStore((s) => s.initTheme)

  useEffect(() => {
    initTheme()
    initAuth()
  }, [initAuth, initTheme])

  return <>{children}</>
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AppInit>
          <Suspense fallback={<PageLoader />}>
            <Routes>
              <Route path="/login" element={<LoginPage />} />
              <Route
                element={
                  <ProtectedRoute>
                    <AdminLayout />
                  </ProtectedRoute>
                }
              >
                <Route path="/" element={<DashboardPage />} />
                <Route path="/collection" element={<CollectionPage />} />
                <Route path="/providers" element={<ProvidersPage />} />
                <Route path="/consumers" element={<ConsumersPage />} />
                <Route path="/stats" element={<RequestStatsPage />} />
                <Route path="/settings" element={<SettingsPage />} />
              </Route>
            </Routes>
          </Suspense>
          <Toaster />
        </AppInit>
      </BrowserRouter>
    </QueryClientProvider>
  )
}
