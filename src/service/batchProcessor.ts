import { PrismaClient } from "@prisma/client";
import { Agency, Caregiver, Carelog, CSVRow, Franchisor, Location } from "../types";
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
  deadlockRetryConfig?: {
    maxRetries: number;
    baseDelayMs: number;
    maxDelayMs: number;
  };
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
      deadlockRetryConfig: {
        maxRetries: 3,
        baseDelayMs: 1000,
        maxDelayMs: 30000,
      },
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
      let batchQueue: CSVRow[][] = [];

      const processBatch = async (batch: CSVRow[]) => {
        try {
          const batchStartTime = Date.now();
          const batchSize = batch.length;

          if (dataType === "caregiver") {
            // For caregiver data, we need to extract and insert related entities first
            logger.info(`📊 Batch ${batchesProcessed + 1}: Starting with ${batchSize} CSV rows`);
            
              const franchisors = DataTransformer.extractFranchisors(batch);
         
            
              const agencies = DataTransformer.extractAgencies(batch);
            
              const locations = DataTransformer.extractLocations(batch);
            
              const caregivers = DataTransformer.transformCaregivers(batch);
              
              if (caregivers.length !== batchSize) {
                logger.warn(`⚠️ Batch ${batchesProcessed + 1}: Data loss detected! ${batchSize} CSV rows -> ${caregivers.length} caregivers`);
              }
            
            try {
              const insertedCount = await this.insertCaregiversBatchWithDependencies(
                franchisors,
                agencies,
                locations,
                caregivers
              );
              
              const skippedCount = caregivers.length - insertedCount;
              logger.info(`📊 Batch ${batchesProcessed + 1}: Inserted ${insertedCount}, Skipped ${skippedCount} (${(skippedCount / caregivers.length * 100).toFixed(2)}%)`);
              
              if (insertedCount === 0 && caregivers.length > 0) {
                logger.warn(`⚠️ Batch ${batchesProcessed + 1}: No caregivers inserted despite ${caregivers.length} valid caregivers`);
              }
              
              processedRows += insertedCount;
            } catch (error) {
              logger.error(`❌ Batch ${batchesProcessed + 1}: Database insertion failed:`, error);
              throw error;
            }
          } else {
              const carelogs = DataTransformer.transformCarelogs(batch);
              logger.info(`📊 Batch ${batchesProcessed + 1}: Transformed ${carelogs.length} carelogs from ${batchSize} CSV rows`);
              
              if (carelogs.length !== batchSize) {
                logger.warn(`⚠️ Batch ${batchesProcessed + 1}: Data loss detected! ${batchSize} CSV rows -> ${carelogs.length} carelogs`);
              }
            
            try {
              const insertedCount = await this.insertCarelogsBatch(carelogs);
              
              const skippedCount = carelogs.length - insertedCount;
              logger.info(`📊 Batch ${batchesProcessed + 1}: Inserted ${insertedCount}, Skipped ${skippedCount} (${(skippedCount / carelogs.length * 100).toFixed(2)}%)`);
              
              processedRows += insertedCount;
            } catch (error) {
              logger.error(`❌ Batch ${batchesProcessed + 1}: Database insertion failed:`, error);
              
              // Instead of throwing error, try to process in smaller chunks
              logger.info(`🔄 Attempting to process batch ${batchesProcessed + 1} in smaller chunks...`);
              
              const chunkSize = Math.max(10, Math.floor(carelogs.length / 10)); // Process in 10 chunks
              let totalInserted = 0;
              
              for (let i = 0; i < carelogs.length; i += chunkSize) {
                const chunk = carelogs.slice(i, i + chunkSize);
                try {
                  const chunkInserted = await this.insertCarelogsBatch(chunk);
                  totalInserted += chunkInserted;
                  logger.info(`📊 Chunk ${Math.floor(i/chunkSize) + 1}: Inserted ${chunkInserted}/${chunk.length}`);
                } catch (chunkError) {
                  logger.error(`❌ Chunk ${Math.floor(i/chunkSize) + 1} failed:`, chunkError);
                  failedRows += chunk.length;
                  errors.push(`Batch ${batchesProcessed + 1} Chunk ${Math.floor(i/chunkSize) + 1}: ${chunkError}`);
                }
              }
              
              processedRows += totalInserted;
              logger.info(`📊 Batch ${batchesProcessed + 1}: Total inserted after chunking: ${totalInserted}/${carelogs.length}`);
            }
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
          
          // Continue processing other batches instead of stopping
          logger.warn(`⚠️ Continuing with next batch despite failure in batch ${batchesProcessed + 1}`);
        } finally {
          activePromises--;
          
          // 处理队列中的批次
          if (batchQueue.length > 0 && activePromises < this.config.maxConcurrency) {
            const nextBatch = batchQueue.shift();
            if (nextBatch) {
              activePromises++;
              logger.info(`📊 Processing queued batch (remaining in queue: ${batchQueue.length})`);
              processBatch(nextBatch);
            }
          }
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

            // if the number of active promises is greater than the max concurrency, queue the batch
            if (activePromises >= this.config.maxConcurrency) {
              batchQueue.push(batchToProcess);
              logger.info(`📊 Queueing batch ${batchesProcessed + 1} (queue size: ${batchQueue.length})`);
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

          // process the remaining batches in the queue
          while (batchQueue.length > 0) {
            const nextBatch = batchQueue.shift();
            if (nextBatch) {
              activePromises++;
              await processBatch(nextBatch);
            }
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

  private async insertCaregiversBatchWithDependencies(
    franchisors: Franchisor[],
    agencies: Agency[],
    locations: Location[],
    caregivers: Caregiver[]
  ): Promise<number> {
    if (caregivers.length === 0) return 0;

    logger.info(`Starting caregiver batch insert with ${caregivers.length} caregivers`);
    logger.info(`Dependencies: ${franchisors.length} franchisors, ${agencies.length} agencies, ${locations.length} locations`);

    // Retry logic for deadlock handling
    const maxRetries = this.config.deadlockRetryConfig?.maxRetries || 3;
    const baseDelay = this.config.deadlockRetryConfig?.baseDelayMs || 1000;
    const maxDelay = this.config.deadlockRetryConfig?.maxDelayMs || 30000;

    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        // Pre-ensure franchisors, agencies, and locations exist (outside transaction to avoid deadlocks)
        
        // Insert franchisors first
        logger.info('Inserting franchisors...');
        for (const franchisor of franchisors) {
          try {
            await this.db.franchisor.upsert({
              where: { franchisorId: franchisor.franchisorId },
              update: {},
              create: {
                franchisorId: franchisor.franchisorId,
                name: franchisor.name,
              },
            });
          } catch (error) {
            // If upsert fails, try to find existing record
            const existing = await this.db.franchisor.findUnique({
              where: { franchisorId: franchisor.franchisorId }
            });
            if (!existing) {
              logger.error(`Failed to create franchisor ${franchisor.franchisorId}:`, error);
            }
          }
        }

        // Insert agencies
        logger.info('Inserting agencies...');
        for (const agency of agencies) {
          try {
            await this.db.agency.upsert({
              where: { 
                agencyId_franchisorId: {
                  agencyId: agency.agencyId,
                  franchisorId: agency.franchisorId
                }
              },
              update: {},
              create: {
                agencyId: agency.agencyId,
                name: agency.name,
                franchisorId: agency.franchisorId,
                subdomain: agency.subdomain,
              },
            });
          } catch (error) {
            // If upsert fails, try to find existing record using the composite key
            const existing = await this.db.agency.findUnique({
              where: { 
                agencyId_franchisorId: {
                  agencyId: agency.agencyId,
                  franchisorId: agency.franchisorId
                }
              }
            });
            if (!existing) {
              logger.error(`Failed to create agency ${agency.agencyId}:`, error);
            }
          }
        }

        // Insert locations
        logger.info('Inserting locations...');
        for (const location of locations) {
          try {
            await this.db.location.upsert({
              where: { locationId: location.locationId },
              update: {},
              create: {
                locationId: location.locationId,
                locationName: location.locationName,
              },
            });
          } catch (error) {
            // If upsert fails, try to find existing record
            const existing = await this.db.location.findUnique({
              where: { locationId: location.locationId }
            });
            if (!existing) {
              logger.error(`Failed to create location ${location.locationId}:`, error);
            }
          }
        }

        // Check for existing caregivers to understand duplicate situation
        const caregiverIds = caregivers.map(c => c.caregiverId).sort(); // Consistent ordering
        const existingCaregivers = await this.db.caregiver.findMany({
          where: {
            caregiverId: {
              in: caregiverIds
            }
          },
          select: {
            caregiverId: true
          },
          orderBy: {
            caregiverId: 'asc' // Consistent ordering
          }
        });
        const existingCaregiverIds = new Set(existingCaregivers.map(c => c.caregiverId));
        const newCaregivers = caregivers.filter(c => !existingCaregiverIds.has(c.caregiverId));
        const duplicateCaregivers = caregivers.filter(c => existingCaregiverIds.has(c.caregiverId));

        logger.info(`Caregiver analysis: ${caregivers.length} total, ${existingCaregivers.length} already exist, ${newCaregivers.length} new, ${duplicateCaregivers.length} duplicates`);

        if (duplicateCaregivers.length > 0) {
          logger.warn(`Found ${duplicateCaregivers.length} duplicate caregivers (${(duplicateCaregivers.length / caregivers.length * 100).toFixed(2)}%)`);
          // Log first few duplicates for debugging
          const sampleDuplicates = duplicateCaregivers.slice(0, 5).map(c => c.caregiverId);
          logger.warn(`Sample duplicate caregiver IDs: ${sampleDuplicates.join(', ')}`);
        }

        if (this.config.enableTransactions) {
          return await this.db.$transaction(async (tx) => {
            // Use a consistent lock ordering to prevent deadlocks
            // Always acquire locks in the same order: session -> caregiver
            
            // 1. Set session replication role first
            await tx.$executeRaw`SET session_replication_role = replica`;

            // 2. Insert caregivers with consistent ordering
            const caregiverData = newCaregivers
              .sort((a, b) => a.caregiverId.localeCompare(b.caregiverId)) // Consistent ordering
              .map((caregiver) => ({
                caregiverId: caregiver.caregiverId,
                externalId: caregiver.externalId,
                profileId: caregiver.profileId,
                franchisorId: caregiver.franchisorId,
                agencyId: caregiver.agencyId,
                locationId: caregiver.locationId,
                firstName: caregiver.firstName,
                lastName: caregiver.lastName,
                email: caregiver.email,
                phoneNumber: caregiver.phoneNumber,
                gender: caregiver.gender,
                applicant: caregiver.applicant,
                birthdayDate: caregiver.birthdayDate,
                onboardingDate: caregiver.onboardingDate,
                applicantStatus: caregiver.applicantStatus,
                status: caregiver.status,
              }));

            logger.info(`Attempting to insert ${caregiverData.length} new caregivers...`);
            
            if (caregiverData.length === 0) {
              logger.warn('No new caregivers to insert - all are duplicates');
              await tx.$executeRaw`SET session_replication_role = DEFAULT`;
              return 0;
            }

            const result = await tx.caregiver.createMany({
              data: caregiverData,
              skipDuplicates: this.config.skipDuplicates,
            });

            logger.info(`Successfully inserted ${result.count} caregivers (${caregiverData.length - result.count} skipped due to duplicates)`);

            // 3. Re-enable foreign key constraints
            await tx.$executeRaw`SET session_replication_role = DEFAULT`;

            return result.count;
          }, {
            maxWait: 10000, // 10 seconds max wait
            timeout: 30000, // 30 seconds timeout
            isolationLevel: 'ReadCommitted', // Use ReadCommitted to reduce lock contention
          });
        } else {
          // Non-transactional path with similar optimizations
          // Temporarily disable foreign key constraints for faster import
          await this.db.$executeRaw`SET session_replication_role = replica`;

          // Insert caregivers with consistent ordering
          const caregiverData = newCaregivers
            .sort((a, b) => a.caregiverId.localeCompare(b.caregiverId)) // Consistent ordering
            .map((caregiver) => ({
              caregiverId: caregiver.caregiverId,
              externalId: caregiver.externalId,
              profileId: caregiver.profileId,
              franchisorId: caregiver.franchisorId,
              agencyId: caregiver.agencyId,
              locationId: caregiver.locationId,
              firstName: caregiver.firstName,
              lastName: caregiver.lastName,
              email: caregiver.email,
              phoneNumber: caregiver.phoneNumber,
              gender: caregiver.gender,
              applicant: caregiver.applicant,
              birthdayDate: caregiver.birthdayDate,
              onboardingDate: caregiver.onboardingDate,
              applicantStatus: caregiver.applicantStatus,
              status: caregiver.status,
            }));

          logger.info(`Attempting to insert ${caregiverData.length} new caregivers...`);
          
          if (caregiverData.length === 0) {
            logger.warn('No new caregivers to insert - all are duplicates');
            await this.db.$executeRaw`SET session_replication_role = DEFAULT`;
            return 0;
          }

          const result = await this.db.caregiver.createMany({
            data: caregiverData,
            skipDuplicates: this.config.skipDuplicates,
          });

          logger.info(`Successfully inserted ${result.count} caregivers (${caregiverData.length - result.count} skipped due to duplicates)`);

          // Re-enable foreign key constraints
          await this.db.$executeRaw`SET session_replication_role = DEFAULT`;

          return result.count;
        }
      } catch (error) {
        lastError = error as Error;
        
        // Check if it's a deadlock error
        const isDeadlock = error instanceof Error && 
          (error.message.includes('deadlock') || 
           error.message.includes('40P01') ||
           error.message.includes('ShareLock'));
        
        if (isDeadlock && attempt < maxRetries) {
          const delay = Math.min(baseDelay * Math.pow(2, attempt - 1), maxDelay);
          logger.warn(`Deadlock detected on attempt ${attempt}. Retrying in ${delay}ms...`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
        
        // If it's not a deadlock or we've exhausted retries, throw the error
        logger.error(`❌Error inserting caregivers:`, error);
      }
    }

    // This should never be reached, but just in case
    throw lastError || new Error('Unknown error occurred during caregiver insertion');
  }

  private async insertCarelogsBatch(carelogs: Carelog[]): Promise<number> {
    if (carelogs.length === 0) return 0;

    logger.info(`Starting carelog batch insert with ${carelogs.length} carelogs`);

    // Retry logic for deadlock handling
    const maxRetries = this.config.deadlockRetryConfig?.maxRetries || 3;
    const baseDelay = this.config.deadlockRetryConfig?.baseDelayMs || 1000;
    const maxDelay = this.config.deadlockRetryConfig?.maxDelayMs || 30000;

    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        logger.info(`Attempt ${attempt}/${maxRetries} for carelog batch insert`);
        // Pre-ensure franchisors and agencies exist (outside transaction to avoid deadlocks)
        const franchisorIds = [...new Set(carelogs.map(c => c.franchisorId))];
        logger.info(`Ensuring ${franchisorIds.length} franchisors exist...`);
      

        if (this.config.enableTransactions) {
          return await this.db.$transaction(async (tx) => {
            // Use a consistent lock ordering to prevent deadlocks
            // Always acquire locks in the same order: session -> caregiver -> carelog
            
            // 1. Set session replication role first
            await tx.$executeRaw`SET session_replication_role = replica`;

            // 2. Get all unique caregiver IDs from carelogs and sort them for consistent ordering
            const caregiverIds = [...new Set(carelogs.map(c => c.caregiverId))].sort();
            logger.info(`Checking existence of ${caregiverIds.length} unique caregivers...`);

            // 3. Batch check caregiver existence with consistent ordering
            const existingCaregivers = await tx.caregiver.findMany({
              where: {
                caregiverId: {
                  in: caregiverIds
                }
              },
              select: {
                caregiverId: true
              },
              orderBy: {
                caregiverId: 'asc' // Consistent ordering
              }
            });

            const existingCaregiverIds = new Set(existingCaregivers.map(c => c.caregiverId));
            logger.info(`Found ${existingCaregiverIds.size} existing caregivers out of ${caregiverIds.length} required`);

            // 4. Filter out carelogs where caregiver doesn't exist
            const validCarelogs = [];
            const skippedCarelogs = [];
            
            for (const carelog of carelogs) {
              if (existingCaregiverIds.has(carelog.caregiverId)) {
                validCarelogs.push(carelog);
              } else {
                skippedCarelogs.push(carelog.carelogId);
              }
            }

            if (skippedCarelogs.length > 0) {
              logger.warn(`Skipping ${skippedCarelogs.length} carelogs due to missing caregivers: ${skippedCarelogs.slice(0, 10).join(', ')}${skippedCarelogs.length > 10 ? '...' : ''}`);
            }

            if (validCarelogs.length === 0) {
              logger.warn('No valid carelogs to insert - all caregivers missing');
              // Re-enable foreign key constraints before returning
              await tx.$executeRaw`SET session_replication_role = DEFAULT`;
              return 0;
            }

            // 5. Check for existing carelog IDs to avoid duplicates (with consistent ordering)
            const carelogIds = [...new Set(validCarelogs.map(c => c.carelogId))].sort();
            const existingCarelogs = await tx.carelog.findMany({
              where: {
                carelogId: {
                  in: carelogIds
                }
              },
              select: {
                carelogId: true
              },
              orderBy: {
                carelogId: 'asc' // Consistent ordering
              }
            });

            const existingCarelogIds = new Set(existingCarelogs.map(c => c.carelogId));
            const newCarelogs = validCarelogs.filter(c => !existingCarelogIds.has(c.carelogId));

            logger.info(`Found ${existingCarelogs.length} existing carelogs, ${newCarelogs.length} new carelogs to insert`);

            if (newCarelogs.length === 0) {
              logger.warn('No new carelogs to insert - all are duplicates');
              // Re-enable foreign key constraints before returning
              await tx.$executeRaw`SET session_replication_role = DEFAULT`;
              return 0;
            }

            // 6. Prepare carelog data with consistent ordering
            const carelogData = newCarelogs
              .sort((a, b) => a.carelogId.localeCompare(b.carelogId)) // Consistent ordering
              .map((carelog) => ({
                carelogId: carelog.carelogId,
                caregiverId: carelog.caregiverId,
                franchisorId: carelog.franchisorId,
                agencyId: carelog.agencyId,
                parentId: carelog.parentId,
                startDatetime: carelog.startDatetime,
                endDatetime: carelog.endDatetime,
                clockInActualDatetime: carelog.clockInActualDatetime,
                clockOutActualDatetime: carelog.clockOutActualDatetime,
                clockInMethod: carelog.clockInMethod,
                clockOutMethod: carelog.clockOutMethod,
                status: carelog.status,
                split: carelog.split,
                documentation: carelog.documentation,
                generalCommentCharCount: carelog.generalCommentCharCount,
              }));

            // 7. Insert carelogs
            const result = await tx.carelog.createMany({
              data: carelogData,
              skipDuplicates: true, // 强制使用skipDuplicates，即使配置中设置为false
            });

            logger.info(`Successfully inserted ${result.count} carelogs (${newCarelogs.length - result.count} skipped due to duplicates)`);

            // 8. Re-enable foreign key constraints
            await tx.$executeRaw`SET session_replication_role = DEFAULT`;

            return result.count;
          }, {
            maxWait: 10000, // 10 seconds max wait
            timeout: 30000, // 30 seconds timeout
            isolationLevel: 'ReadCommitted', // Use ReadCommitted to reduce lock contention
          });
        } else {
          // Non-transactional path with similar optimizations
          // Temporarily disable foreign key constraints for faster import
          await this.db.$executeRaw`SET session_replication_role = replica`;

          // Get all unique caregiver IDs from carelogs
          const caregiverIds = [...new Set(carelogs.map(c => c.caregiverId))].sort();
          logger.info(`Checking existence of ${caregiverIds.length} unique caregivers...`);

          // Batch check caregiver existence
          const existingCaregivers = await this.db.caregiver.findMany({
            where: {
              caregiverId: {
                in: caregiverIds
              }
            },
            select: {
              caregiverId: true
            },
            orderBy: {
              caregiverId: 'asc'
            }
          });

          const existingCaregiverIds = new Set(existingCaregivers.map(c => c.caregiverId));
          logger.info(`Found ${existingCaregiverIds.size} existing caregivers out of ${caregiverIds.length} required`);

          // Filter out carelogs where caregiver doesn't exist
          const validCarelogs = [];
          const skippedCarelogs = [];
          
          for (const carelog of carelogs) {
            if (existingCaregiverIds.has(carelog.caregiverId)) {
              validCarelogs.push(carelog);
            } else {
              skippedCarelogs.push(carelog.carelogId);
            }
          }

          if (skippedCarelogs.length > 0) {
            logger.warn(`Skipping ${skippedCarelogs.length} carelogs due to missing caregivers: ${skippedCarelogs.slice(0, 10).join(', ')}${skippedCarelogs.length > 10 ? '...' : ''}`);
          }

          if (validCarelogs.length === 0) {
            logger.warn('No valid carelogs to insert - all caregivers missing');
            // Re-enable foreign key constraints before returning
            await this.db.$executeRaw`SET session_replication_role = DEFAULT`;
            return 0;
          }

          // Check for existing carelog IDs to avoid duplicates
          const carelogIds = [...new Set(validCarelogs.map(c => c.carelogId))].sort();
          const existingCarelogs = await this.db.carelog.findMany({
            where: {
              carelogId: {
                in: carelogIds
              }
            },
            select: {
              carelogId: true
            },
            orderBy: {
              carelogId: 'asc'
            }
          });

          const existingCarelogIds = new Set(existingCarelogs.map(c => c.carelogId));
          const newCarelogs = validCarelogs.filter(c => !existingCarelogIds.has(c.carelogId));

          logger.info(`Found ${existingCarelogs.length} existing carelogs, ${newCarelogs.length} new carelogs to insert`);

          if (newCarelogs.length === 0) {
            logger.warn('No new carelogs to insert - all are duplicates');
            // Re-enable foreign key constraints before returning
            await this.db.$executeRaw`SET session_replication_role = DEFAULT`;
            return 0;
          }

          // Prepare carelog data
          const carelogData = newCarelogs
            .sort((a, b) => a.carelogId.localeCompare(b.carelogId))
            .map((carelog) => ({
              carelogId: carelog.carelogId,
              caregiverId: carelog.caregiverId,
              franchisorId: carelog.franchisorId,
              agencyId: carelog.agencyId,
              parentId: carelog.parentId,
              startDatetime: carelog.startDatetime,
              endDatetime: carelog.endDatetime,
              clockInActualDatetime: carelog.clockInActualDatetime,
              clockOutActualDatetime: carelog.clockOutActualDatetime,
              clockInMethod: carelog.clockInMethod,
              clockOutMethod: carelog.clockOutMethod,
              status: carelog.status,
              split: carelog.split,
              documentation: carelog.documentation,
              generalCommentCharCount: carelog.generalCommentCharCount,
            }));

          const result = await this.db.carelog.createMany({
            data: carelogData,
            skipDuplicates: true, // 强制使用skipDuplicates，即使配置中设置为false
          });

          logger.info(`Successfully inserted ${result.count} carelogs (${newCarelogs.length - result.count} skipped due to duplicates)`);

          // Re-enable foreign key constraints
          await this.db.$executeRaw`SET session_replication_role = DEFAULT`;

          return result.count;
        }
      } catch (error) {
        lastError = error as Error;
        
        // Check if it's a deadlock error
        const isDeadlock = error instanceof Error && 
          (error.message.includes('deadlock') || 
           error.message.includes('40P01') ||
           error.message.includes('ShareLock'));
        
        if (isDeadlock && attempt < maxRetries) {
          const delay = Math.min(baseDelay * Math.pow(2, attempt - 1), maxDelay);
          logger.warn(`Deadlock detected on attempt ${attempt}. Retrying in ${delay}ms...`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
        
        // If it's not a deadlock or we've exhausted retries, throw the error
        throw error;
      }
    }

    // This should never be reached, but just in case
    throw lastError || new Error('Unknown error occurred during carelog insertion');
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
