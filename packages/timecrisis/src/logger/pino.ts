import pino from 'pino';
import { Logger } from './index';

export interface PinoLoggerOptions {
  /**
   * Pino logger instance to use
   * If not provided, a new instance will be created
   */
  instance?: pino.Logger;

  /**
   * Pino logger options
   * Only used if instance is not provided
   */
  options?: pino.LoggerOptions;
}

/**
 * Pino logger
 */
export class PinoLogger implements Logger {
  private logger: pino.Logger;

  constructor(opts: PinoLoggerOptions = {}) {
    this.logger = opts.instance || pino(opts.options);
  }

  child(name: string): Logger {
    return new PinoLogger({
      instance: this.logger.child({ name }),
    });
  }

  info(message: string, context?: Record<string, unknown>): void {
    if (context) {
      this.logger.info(context, message);
    } else {
      this.logger.info(message);
    }
  }

  debug(message: string, context?: Record<string, unknown>): void {
    if (context) {
      this.logger.debug(context, message);
    } else {
      this.logger.debug(message);
    }
  }

  warn(message: string, context?: Record<string, unknown>): void {
    if (context) {
      this.logger.warn(context, message);
    } else {
      this.logger.warn(message);
    }
  }

  error(message: string, context?: Record<string, unknown>): void {
    if (context) {
      this.logger.error(context, message);
    } else {
      this.logger.error(message);
    }
  }
}

/**
 * Create a new Pino logger instance
 */
export function createPinoLogger(opts: PinoLoggerOptions = {}): Logger {
  return new PinoLogger(opts);
}
