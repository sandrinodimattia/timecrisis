/**
 * Convert a date to ISO string or null
 */
export function fromDate(date: Date | undefined | null): string | null {
  return date?.toISOString() ?? null;
}

/**
 * Convert an ISO string to date or undefined
 */
export function toDate(str: string | null): Date | undefined {
  if (!str) return undefined;
  try {
    return new Date(str);
  } catch {
    return undefined;
  }
}

/**
 * Convert a boolean to 1/0 for SQLite
 */
export function fromBoolean(value: boolean | undefined | null): number {
  return value ? 1 : 0;
}

/**
 * Convert a SQLite 1/0 to boolean
 */
export function toBoolean(value: number | null): boolean {
  return value === 1;
}

/**
 * JSON reviver that converts ISO dates to Date objects
 */
function dateReviver(key: string, value: unknown): unknown {
  if (typeof value === 'string') {
    const isoDateRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$/;
    if (isoDateRegex.test(value)) {
      return new Date(value);
    }
  }
  return value;
}

/**
 * Parse JSON with date reviver
 */
export function parseJSON(str: string | null): unknown {
  if (!str) return undefined;
  try {
    return JSON.parse(str, dateReviver);
  } catch {
    return undefined;
  }
}

/**
 * Serialize data to JSON
 */
export function serializeData(data: unknown): string {
  const str = JSON.stringify(data ?? null);
  return str;
}
