import { describe, it, expect, beforeEach } from 'vitest'
import { getStoredTheme, setStoredTheme, getResolvedTheme } from '../theme'

describe('getStoredTheme', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  it('returns system when nothing stored', () => {
    expect(getStoredTheme()).toBe('system')
  })

  it('returns stored value', () => {
    localStorage.setItem('stockpulse-theme', 'dark')
    expect(getStoredTheme()).toBe('dark')
  })

  it('returns system for invalid stored value', () => {
    localStorage.setItem('stockpulse-theme', 'invalid')
    expect(getStoredTheme()).toBe('system')
  })
})

describe('setStoredTheme', () => {
  beforeEach(() => {
    localStorage.clear()
  })

  it('stores theme in localStorage', () => {
    setStoredTheme('dark')
    expect(localStorage.getItem('stockpulse-theme')).toBe('dark')
  })
})

describe('getResolvedTheme', () => {
  it('returns light for light', () => {
    expect(getResolvedTheme('light')).toBe('light')
  })

  it('returns dark for dark', () => {
    expect(getResolvedTheme('dark')).toBe('dark')
  })

  it('returns light or dark for system', () => {
    const result = getResolvedTheme('system')
    expect(['light', 'dark']).toContain(result)
  })
})
