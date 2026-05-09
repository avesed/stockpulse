import { describe, it, expect, vi, beforeEach } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import { renderWithProviders, userEvent } from '@/test/test-utils'
import LoginPage from '../LoginPage'
import { useAuthStore } from '@/stores/authStore'

vi.mock('@/stores/authStore', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/stores/authStore')>()
  return {
    ...actual,
    useAuthStore: vi.fn(),
  }
})

const mockLogin = vi.fn()

beforeEach(() => {
  vi.mocked(useAuthStore).mockImplementation((selector) => {
    const state = {
      user: null,
      token: null,
      isLoading: false,
      login: mockLogin,
      logout: vi.fn(),
      initAuth: vi.fn(),
      isAdmin: () => false,
      isAuthenticated: () => false,
    }
    return typeof selector === 'function' ? selector(state) : state
  })
  mockLogin.mockReset()
})

describe('LoginPage', () => {
  it('renders username and password inputs', () => {
    renderWithProviders(<LoginPage />)
    expect(screen.getByLabelText(/username/i)).toBeInTheDocument()
    expect(screen.getByLabelText(/password/i)).toBeInTheDocument()
  })

  it('renders submit button', () => {
    renderWithProviders(<LoginPage />)
    const btn = screen.getByRole('button', { name: /sign in|login|loading/i })
    expect(btn).toBeInTheDocument()
  })

  it('calls login on form submit', async () => {
    mockLogin.mockResolvedValue(undefined)
    renderWithProviders(<LoginPage />)

    const user = userEvent.setup()
    const usernameInput = screen.getByLabelText(/username/i)
    const passwordInput = screen.getByLabelText(/password/i)

    await user.clear(usernameInput)
    await user.type(usernameInput, 'testuser')
    await user.clear(passwordInput)
    await user.type(passwordInput, 'testpass')

    const btn = screen.getByRole('button', { name: /sign in|login/i })
    await user.click(btn)

    await waitFor(() => {
      expect(mockLogin).toHaveBeenCalledWith('testuser', 'testpass')
    })
  })

  it('shows error message on login failure', async () => {
    mockLogin.mockRejectedValue(new Error('Invalid credentials'))
    renderWithProviders(<LoginPage />)

    const user = userEvent.setup()
    const btn = screen.getByRole('button', { name: /sign in|login/i })
    await user.click(btn)

    await waitFor(() => {
      expect(screen.getByText(/invalid credentials/i)).toBeInTheDocument()
    })
  })
})
