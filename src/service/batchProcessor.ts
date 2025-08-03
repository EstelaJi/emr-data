import { PrismaClient } from "@prisma/client";
import { Caregiver, Carelog, CSVRow } from "../types";
import { DataTransformer } from "../transformers/dataTransformer";
import { logger } from "../utils/logger";
import * as fs from "fs";
import csv from "csv-parser";
import { EventEmitter } from "events";

export interface BatchConfig {
  batchSize: number;
  maxConcurrency: number;
  enableTransactions: boolean;
  skipDuplicates: boolean;
}

export interface BatchResult {
  totalRows: number;
  processedRows: number;
  failedRows: number;
  processingTime: number;
  batchesProcessed: number;
  errors: string[];
}

export class BatchProcessor extends EventEmitter {
  private db: PrismaClient;
  private config: BatchConfig;

  constructor(config: Partial<BatchConfig> = {}) {
    super();
    this.db = new PrismaClient({
      datasources: {
        db: {
          url: process.env.DATABASE_URL,
        },
      },
      log: ["error", "warn"],
    });

    this.config = {
      batchSize: 5000,
      maxConcurrency: 3,
      enableTransactions: true,
      skipDuplicates: false,
      ...config,
    };
  }

  async processFilesParallel(
    filePaths: string[],
    dataType: "caregiver" | "carelog"
  ): Promise<BatchResult[]> {
    const results: BatchResult[] = [];
    const semaphore = new Semaphore(this.config.maxConcurrency);

    const promises = filePaths.map(async (filePath) => {
      await semaphore.acquire();
      try {
        const result = await this.processFile(filePath, dataType);
        results.push(result);
        return result;
      } finally {
        semaphore.release();
      }
    });

    await Promise.all(promises);
    return results;
  }

  async processFile(
    filePath: string,
    dataType: "caregiver" | "carelog"
  ): Promise<BatchResult> {
    const startTime = Date.now();
    let totalRows = 0;
    let processedRows = 0;
    let failedRows = 0;
    const errors: string[] = [];
    let batchesProcessed = 0;

    logger.info(`🚀 Starting batch processing: ${filePath}`);

    return new Promise((resolve, reject) => {
      let currentBatch: CSVRow[] = [];
      let activePromises = 0;

      const processBatch = async (batch: CSVRow[]) => {
        try {
          const batchStartTime = Date.now();

          if (dataType === "caregiver") {
            const caregivers = DataTransformer.transformCaregivers(batch);
            const insertedCount = await this.insertCaregiversBatch(caregivers);
            processedRows += insertedCount;
          } else {
            const carelogs = DataTransformer.transformCarelogs(batch);
            const insertedCount = await this.insertCarelogsBatch(carelogs);
            processedRows += insertedCount;
          }

          batchesProcessed++;
          const batchTime = Date.now() - batchStartTime;
          logger.info(
            `✅ Batch ${batchesProcessed}: ${batch.length} records in ${batchTime}ms`
          );

          this.emit("batchComplete", {
            batchNumber: batchesProcessed,
            recordsProcessed: batch.length,
            processingTime: batchTime,
          });
        } catch (error) {
          logger.error(`❌ Batch ${batchesProcessed + 1} failed:`, error);
          failedRows += batch.length;
          errors.push(`Batch ${batchesProcessed + 1}: ${error}`);
        } finally {
          activePromises--;
        }
      };

      fs.createReadStream(filePath)
        .pipe(csv())
        .on("data", (data: CSVRow) => {
          currentBatch.push(data);
          totalRows++;

          if (currentBatch.length >= this.config.batchSize) {
            const batchToProcess = [...currentBatch];
            currentBatch = [];

            if (activePromises >= this.config.maxConcurrency) {
              return;
            }

            activePromises++;
            processBatch(batchToProcess);
          }
        })
        .on("end", async () => {
          if (currentBatch.length > 0) {
            await processBatch(currentBatch);
          }

          while (activePromises > 0) {
            await new Promise((resolve) => setTimeout(resolve, 100));
          }

          const totalTime = Date.now() - startTime;
          const result: BatchResult = {
            totalRows,
            processedRows,
            failedRows,
            processingTime: totalTime,
            batchesProcessed,
            errors,
          };

          logger.info(`🎉 File processing completed: ${filePath}`);
          logger.info(
            `📊 Total: ${totalRows}, Processed: ${processedRows}, Failed: ${failedRows}, Time: ${totalTime}ms`
          );

          resolve(result);
        })
        .on("error", (error) => {
          logger.error(`File processing error: ${filePath}`, error);
          reject(error);
        });
    });
  }

  private async insertCaregiversBatch(
    caregivers: Caregiver[]
  ): Promise<number> {
    if (caregivers.length === 0) return 0;

    const transformedData = caregivers.map((caregiver) => {
      const result: any = {};
      for (const [key, value] of Object.entries(caregiver)) {
        const snakeKey = key.replace(/([A-Z])/g, "_$1").toLowerCase();

        if (key === "birthdayDate" || key === "onboardingDate") {
          result[snakeKey] =
            value instanceof Date ? value : value ? new Date(value) : null;
        } else {
          result[snakeKey] = value;
        }
      }
      return result;
    });

    try {
      if (this.config.enableTransactions) {
        return await this.db.$transaction(async (tx) => {
          const result = await tx.caregivers.createMany({
            data: transformedData,
            skipDuplicates: this.config.skipDuplicates,
          });
          return result.count;
        });
      } else {
        const result = await this.db.caregivers.createMany({
          data: transformedData,
          skipDuplicates: this.config.skipDuplicates,
        });
        return result.count;
      }
    } catch (error) {
      logger.error(`Database insert error:`, error);
      throw error;
    }
  }

  private async insertCarelogsBatch(carelogs: Carelog[]): Promise<number> {
    if (carelogs.length === 0) return 0;

    const transformedData = carelogs.map((carelog) => {
      const result: any = {};
      for (const [key, value] of Object.entries(carelog)) {
        const snakeKey = key.replace(/([A-Z])/g, "_$1").toLowerCase();

        if (
          key === "startDatetime" ||
          key === "endDatetime" ||
          key === "clockInActualDatetime" ||
          key === "clockOutActualDatetime"
        ) {
          result[snakeKey] =
            value instanceof Date ? value : value ? new Date(value) : null;
        } else {
          result[snakeKey] = value;
        }
      }
      return result;
    });

    try {
      if (this.config.enableTransactions) {
        return await this.db.$transaction(async (tx) => {
          // temporary disable foreign key constraints
          await tx.$executeRaw`SET session_replication_role = replica`;

          const result = await tx.carelog.createMany({
            data: transformedData,
            skipDuplicates: this.config.skipDuplicates,
          });

          // restore foreign key constraints
          await tx.$executeRaw`SET session_replication_role = DEFAULT`;

          return result.count;
        });
      } else {
        // temporary disable foreign key constraints
        await this.db.$executeRaw`SET session_replication_role = replica`;

        const result = await this.db.carelog.createMany({
          data: transformedData,
          skipDuplicates: this.config.skipDuplicates,
        });

        // restore foreign key constraints
        await this.db.$executeRaw`SET session_replication_role = DEFAULT`;

        return result.count;
      }
    } catch (error) {
      logger.error(`Database insert error:`, error);
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    await this.db.$disconnect();
  }
}

class Semaphore {
  private permits: number;
  private waitQueue: Array<() => void> = [];

  constructor(permits: number) {
    this.permits = permits;
  }

  async acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits--;
      return Promise.resolve();
    }

    return new Promise<void>((resolve) => {
      this.waitQueue.push(resolve);
    });
  }

  release(): void {
    this.permits++;
    if (this.waitQueue.length > 0) {
      const next = this.waitQueue.shift();
      if (next) {
        this.permits--;
        next();
      }
    }
  }
}
