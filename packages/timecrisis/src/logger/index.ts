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

/**
 * Console logger
 */
export class ConsoleLogger implements Logger {
  constructor(private name?: string) {}

  child(name: string): Logger {
    if (this.name) {
      return new ConsoleLogger(`${this.name}.${name}`);
    } else {
      return new ConsoleLogger(name);
    }
  }

  info(message: string, context?: Record<string, unknown>): void {
    if (this.name) {
      console.info(`${new Date().toISOString()} - [${this.name}] ${message}`, context ?? '');
    } else {
      console.info(`${new Date().toISOString()} - ${message}`, context ?? '');
    }
  }
  debug(message: string, context?: Record<string, unknown>): void {
    if (this.name) {
      console.debug(`${new Date().toISOString()} - [${this.name}] ${message}`, context ?? '');
    } else {
      console.debug(`${new Date().toISOString()} - ${message}`, context ?? '');
    }
  }
  warn(message: string, context?: Record<string, unknown>): void {
    if (this.name) {
      console.warn(`${new Date().toISOString()} - [${this.name}] ${message}`, context ?? '');
    } else {
      console.warn(`${new Date().toISOString()} - ${message}`, context ?? '');
    }
  }
  error(message: string, context?: Record<string, unknown>): void {
    if (this.name) {
      console.error(`${new Date().toISOString()} - [${this.name}] ${message}`, context ?? '');
    } else {
      console.error(`${new Date().toISOString()} - ${message}`, context ?? '');
    }
  }
}

// Export default noop logger
export const logger = new EmptyLogger();
