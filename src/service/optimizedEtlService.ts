import { BatchProcessor, BatchConfig, BatchResult } from "./batchProcessor";
import { FileUtils } from "../utils/fileUtils";
import { logger } from "../utils/logger";

export interface OptimizedETLConfig extends BatchConfig {
  enableParallelProcessing: boolean;
  enableProgressTracking: boolean;
}

export class OptimizedETLService {
  private batchProcessor: BatchProcessor;
  private config: OptimizedETLConfig;

  constructor(config: Partial<OptimizedETLConfig> = {}) {
    this.config = {
      batchSize: 5000,
      maxConcurrency: 3,
      enableTransactions: true,
      skipDuplicates: false,
      enableParallelProcessing: true,
      enableProgressTracking: true,
      ...config,
    };

    this.batchProcessor = new BatchProcessor(this.config);
  }

  async initialize(): Promise<void> {
    logger.info("🚀 Initializing Optimized ETL Service");

    if (this.config.enableProgressTracking) {
      this.batchProcessor.on("batchComplete", (data) => {
        logger.info(
          `📈 Progress: Batch ${data.batchNumber} completed in ${data.processingTime}ms`
        );
      });
    }
  }

  async processAllFiles(inputDir: string): Promise<BatchResult[]> {
    const csvFiles = FileUtils.getCSVFiles(inputDir);
    logger.info(`📁 Found ${csvFiles.length} CSV files:`, csvFiles);

    if (csvFiles.length === 0) {
      logger.warn("⚠️ No CSV files found");
      return [];
    }

    const results: BatchResult[] = [];

    if (this.config.enableParallelProcessing && csvFiles.length > 1) {
      logger.info("🔄 Starting parallel file processing");

      const caregiverFiles = csvFiles.filter(
        (file) =>
          file.toLowerCase().includes("caregiver") ||
          file.toLowerCase().includes("caregiver_data")
      );
      const carelogFiles = csvFiles.filter(
        (file) =>
          file.toLowerCase().includes("carelog") ||
          file.toLowerCase().includes("carelog_data")
      );

      const promises: Promise<BatchResult[]>[] = [];

      if (caregiverFiles.length > 0) {
        promises.push(
          this.batchProcessor.processFilesParallel(caregiverFiles, "caregiver")
        );
      }

      if (carelogFiles.length > 0) {
        promises.push(
          this.batchProcessor.processFilesParallel(carelogFiles, "carelog")
        );
      }

      const fileResults = await Promise.all(promises);
      results.push(...fileResults.flat());
    } else {
      logger.info("🔄 Starting sequential file processing");

      for (const filePath of csvFiles) {
        const dataType = await this.detectFileType(filePath);
        if (!dataType) {
          logger.error(`❌ Skipping file with unknown type: ${filePath}`);
          continue;
        }

        logger.info(`📄 Processing file: ${filePath} (${dataType})`);
        const result = await this.batchProcessor.processFile(
          filePath,
          dataType
        );
        results.push(result);
      }
    }

    return results;
  }

  private async detectFileType(
    filePath: string
  ): Promise<"caregiver" | "carelog" | null> {
    const fileName = filePath.toLowerCase();

    if (fileName.includes("caregiver") || fileName.includes("caregiver_data")) {
      return "caregiver";
    }

    if (fileName.includes("carelog") || fileName.includes("carelog_data")) {
      return "carelog";
    }

    return null;
  }

  getPerformanceStats(results: BatchResult[]): {
    totalRows: number;
    totalProcessed: number;
    totalFailed: number;
    totalTime: number;
    averageSpeed: number;
    successRate: number;
  } {
    const totalRows = results.reduce((sum, r) => sum + r.totalRows, 0);
    const totalProcessed = results.reduce((sum, r) => sum + r.processedRows, 0);
    const totalFailed = results.reduce((sum, r) => sum + r.failedRows, 0);
    const totalTime = results.reduce((sum, r) => sum + r.processingTime, 0);

    const averageSpeed =
      totalTime > 0 ? (totalProcessed / totalTime) * 1000 : 0; // records per second
    const successRate = totalRows > 0 ? (totalProcessed / totalRows) * 100 : 0;

    return {
      totalRows,
      totalProcessed,
      totalFailed,
      totalTime,
      averageSpeed,
      successRate,
    };
  }

  async shutdown(): Promise<void> {
    logger.info("🛑 Shutting down Optimized ETL Service");
    await this.batchProcessor.disconnect();
  }
}
