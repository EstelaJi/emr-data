import { OptimizedETLService } from "./service/optimizedEtlService";
import { logger } from "./utils/logger";

async function main() {
  const etlService = new OptimizedETLService({
    batchSize: 5000,
    maxConcurrency: 3,
    enableTransactions: true,
    skipDuplicates: false, // Temporarily disable to test
    enableParallelProcessing: true,
    enableProgressTracking: true,
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
