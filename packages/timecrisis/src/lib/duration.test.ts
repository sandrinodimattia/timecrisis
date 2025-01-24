import { describe, it, expect } from 'vitest';

import { parseDuration } from './duration.js';

describe('parseDuration', () => {
  it('should parse milliseconds correctly', () => {
    expect(parseDuration('100ms')).toBe(100);
    expect(parseDuration('0ms')).toBe(0);
    expect(parseDuration('999ms')).toBe(999);
  });

  it('should parse seconds correctly', () => {
    expect(parseDuration('1s')).toBe(1000);
    expect(parseDuration('5s')).toBe(5000);
    expect(parseDuration('0s')).toBe(0);
  });

  it('should parse minutes correctly', () => {
    expect(parseDuration('1m')).toBe(60000);
    expect(parseDuration('5m')).toBe(300000);
    expect(parseDuration('0m')).toBe(0);
  });

  it('should parse hours correctly', () => {
    expect(parseDuration('1h')).toBe(3600000);
    expect(parseDuration('2h')).toBe(7200000);
    expect(parseDuration('0h')).toBe(0);
  });

  it('should parse days correctly', () => {
    expect(parseDuration('1d')).toBe(86400000);
    expect(parseDuration('2d')).toBe(172800000);
    expect(parseDuration('0d')).toBe(0);
  });

  it('should throw error for invalid duration format', () => {
    expect(() => parseDuration('invalid')).toThrow('Invalid duration format');
    expect(() => parseDuration('10x')).toThrow('Invalid duration format');
    expect(() => parseDuration('ms')).toThrow('Invalid duration format');
    expect(() => parseDuration('')).toThrow('Invalid duration format');
    expect(() => parseDuration('10')).toThrow('Invalid duration format');
    expect(() => parseDuration('m10')).toThrow('Invalid duration format');
    expect(() => parseDuration(' 10m')).toThrow('Invalid duration format');
    expect(() => parseDuration('10m ')).toThrow('Invalid duration format');
  });

  it('should handle leading zeros', () => {
    expect(parseDuration('01m')).toBe(60000);
    expect(parseDuration('001s')).toBe(1000);
    expect(parseDuration('00001ms')).toBe(1);
  });

  it('should handle large numbers', () => {
    expect(parseDuration('1000m')).toBe(60000000);
    expect(parseDuration('9999s')).toBe(9999000);
    expect(parseDuration('999999ms')).toBe(999999);
  });

  it('should handle all valid unit cases', () => {
    const units = ['ms', 's', 'm', 'h', 'd'];
    units.forEach((unit) => {
      expect(parseDuration(`1${unit}`)).toBeGreaterThan(0);
    });
  });
});
