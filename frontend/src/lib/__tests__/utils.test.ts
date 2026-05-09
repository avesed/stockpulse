import { describe, it, expect } from 'vitest'
import { cn, formatDate, formatNumber, getErrorMessage } from '../utils'

describe('cn', () => {
  it('merges tailwind classes', () => {
    expect(cn('px-2', 'py-1')).toBe('px-2 py-1')
  })

  it('resolves conflicting classes', () => {
    expect(cn('px-2', 'px-4')).toBe('px-4')
  })

  it('handles conditional classes', () => {
    expect(cn('base', false && 'hidden', 'extra')).toBe('base extra')
  })

  it('handles empty args', () => {
    expect(cn()).toBe('')
  })
})

describe('formatDate', () => {
  it('formats Date object', () => {
    const d = new Date('2024-03-15T00:00:00Z')
    const result = formatDate(d)
    expect(result).toContain('2024')
    expect(result).toContain('15')
  })

  it('formats ISO string', () => {
    const result = formatDate('2024-01-01T00:00:00Z')
    expect(result).toContain('2024')
  })
})

describe('formatNumber', () => {
  it('formats with default decimals', () => {
    expect(formatNumber(1234)).toBe('1,234')
  })

  it('formats with specified decimals', () => {
    expect(formatNumber(1234.567, 2)).toBe('1,234.57')
  })

  it('formats zero', () => {
    expect(formatNumber(0)).toBe('0')
  })
})

describe('getErrorMessage', () => {
  it('extracts Error message', () => {
    expect(getErrorMessage(new Error('test error'))).toBe('test error')
  })

  it('returns default for non-Error', () => {
    expect(getErrorMessage('string')).toBe('An unexpected error occurred')
    expect(getErrorMessage(42)).toBe('An unexpected error occurred')
    expect(getErrorMessage(null)).toBe('An unexpected error occurred')
  })
})
