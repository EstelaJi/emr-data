import { ETLResult } from "../types";

export class Logger {
  private static instance: Logger;
  private logLevel: string;

  private constructor() {
    this.logLevel = process.env.LOG_LEVEL || "info";
  }

  public static getInstance(): Logger {
    if (!Logger.instance) {
      Logger.instance = new Logger();
    }
    return Logger.instance;
  }

  private shouldLog(level: string): boolean {
    const levels = ["error", "warn", "info", "debug"];
    const currentLevelIndex = levels.indexOf(this.logLevel);
    const messageLevelIndex = levels.indexOf(level);
    return messageLevelIndex <= currentLevelIndex;
  }

  public info(message: string, data?: any): void {
    if (this.shouldLog("info")) {
      console.log(`[INFO] ${new Date().toISOString()}: ${message}`, data || "");
    }
  }

  public warn(message: string, data?: any): void {
    if (this.shouldLog("warn")) {
      console.warn(
        `[WARN] ${new Date().toISOString()}: ${message}`,
        data || ""
      );
    }
  }

  public error(message: string, error?: any): void {
    if (this.shouldLog("error")) {
      console.error(
        `[ERROR] ${new Date().toISOString()}: ${message}`,
        error || ""
      );
    }
  }

  public debug(message: string, data?: any): void {
    if (this.shouldLog("debug")) {
      console.debug(
        `[DEBUG] ${new Date().toISOString()}: ${message}`,
        data || ""
      );
    }
  }

  public logETLResult(result: ETLResult): void {
    this.info("ETL Processing Result", {
      success: result.success,
      recordsRead: result.recordsRead,
      recordsProcessed: result.recordsProcessed,
      recordsFailed: result.recordsFailed,
      processingTime: `${result.processingTime}ms`,
    });

    if (result.errors.length > 0) {
      this.error("ETL Processing Errors", result.errors);
    }
  }
}

export const logger = Logger.getInstance();
