/**
 * Convert a duration string to milliseconds
 * @param duration Duration string to parse, eg: "10m"
 * @returns Duration in milliseconds
 */
export function parseDuration(duration: string): number {
  const regex = /^(\d+)(ms|s|m|h|d)$/;
  const match = duration.match(regex);
  if (!match) {
    throw new Error('Invalid duration format. Use number + unit (ms, s, m, h, d)');
  }

  const value = parseInt(match[1], 10);
  const unit = match[2];

  switch (unit) {
    case 'ms':
      return value;
    case 's':
      return value * 1000;
    case 'm':
      return value * 60 * 1000;
    case 'h':
      return value * 60 * 60 * 1000;
    case 'd':
      return value * 24 * 60 * 60 * 1000;
    default:
      throw new Error('Invalid duration unit');
  }
}
