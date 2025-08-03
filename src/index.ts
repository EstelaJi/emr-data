import { OptimizedETLService } from "./service/optimizedEtlService";
import { logger } from "./utils/logger";

async function main() {
  const etlService = new OptimizedETLService({
    batchSize: 5000,
    maxConcurrency: 2,
    enableTransactions: false,
    skipDuplicates: true,
    enableParallelProcessing: false,
    enableProgressTracking: true,
    deadlockRetryConfig: {
      maxRetries: 3,
      baseDelayMs: 1000,
      maxDelayMs: 10000,
    },
  });

  try {
    await etlService.initialize();

    const inputDir = process.env.CSV_INPUT_DIR || "./data";
    logger.info(`🚀 Starting optimized ETL processing from: ${inputDir}`);

    await etlService.processAllFiles(inputDir);
  } catch (error) {
    logger.error("Error processing files:", error);
    process.exit(1);
  } finally {
    await etlService.shutdown();
  }
}

main();
