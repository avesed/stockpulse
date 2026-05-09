import { describe, it, expect, beforeEach } from 'vitest'
import {
  getAccessToken,
  getRefreshToken,
  setTokens,
  clearTokens,
  getErrorMessage,
} from '../client'

describe('Token management', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  it('getAccessToken returns null when not set', () => {
    expect(getAccessToken()).toBeNull()
  })

  it('setTokens stores both tokens', () => {
    setTokens('access-123', 'refresh-456')
    expect(getAccessToken()).toBe('access-123')
    expect(getRefreshToken()).toBe('refresh-456')
  })

  it('clearTokens removes both tokens', () => {
    setTokens('a', 'r')
    clearTokens()
    expect(getAccessToken()).toBeNull()
    expect(getRefreshToken()).toBeNull()
  })
})

describe('getErrorMessage', () => {
  it('extracts Error.message', () => {
    expect(getErrorMessage(new Error('test'))).toBe('test')
  })

  it('returns default for unknown type', () => {
    expect(getErrorMessage(42)).toBe('An unexpected error occurred')
  })

  it('returns default for null', () => {
    expect(getErrorMessage(null)).toBe('An unexpected error occurred')
  })
})
