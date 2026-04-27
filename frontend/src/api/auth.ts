import apiClient from '@/api/client'
import type { User, TokenResponse } from '@/types'

export async function login(data: {
  email: string
  password: string
}): Promise<TokenResponse> {
  const response = await apiClient.post<TokenResponse>('/auth/login', data)
  return response.data
}

export async function register(data: {
  email: string
  password: string
  displayName: string
}): Promise<TokenResponse> {
  const response = await apiClient.post<TokenResponse>('/auth/register', data)
  return response.data
}

export async function logout(refreshToken: string | null): Promise<void> {
  await apiClient.post('/auth/logout', { refreshToken })
}

export async function getMe(): Promise<User> {
  const response = await apiClient.get<User>('/auth/me')
  return response.data
}
