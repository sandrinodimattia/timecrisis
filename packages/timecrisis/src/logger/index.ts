/**
 * Logger interface for Time Crisis
 */
export interface Logger {
  /**
   * Create a child logger with additional context
   */
  child(name: string): Logger;

  /**
   * Log an info message
   */
  info(message: string, context?: Record<string, unknown>): void;

  /**
   * Log a debug message
   */
  debug(message: string, context?: Record<string, unknown>): void;

  /**
   * Log a warning message
   */
  warn(message: string, context?: Record<string, unknown>): void;

  /**
   * Log an error message
   */
  error(message: string, context?: Record<string, unknown>): void;
}

/**
 * Empty logger implementation
 */
export class EmptyLogger implements Logger {
  child(): Logger {
    return this;
  }

  info(): void {}
  debug(): void {}
  warn(): void {}
  error(): void {}
}

// Export default noop logger
export const logger = new EmptyLogger();
