import * as fs from "fs";
import csv from "csv-parser";
import { CSVRow } from "../types";
import { logger } from "../utils/logger";

export class CSVReader {
  /**
   * read a single CSV file
   */
  static async readFile(filePath: string): Promise<CSVRow[]> {
    return new Promise((resolve, reject) => {
      const results: CSVRow[] = [];
      let rowCount = 0;

      logger.info(`Starting to read CSV file: ${filePath}`);

      fs.createReadStream(filePath)
        .pipe(csv())
        .on("data", (data: CSVRow) => {
          results.push(data);
          rowCount++;

          // each 1000 rows, log the progress
          if (rowCount % 1000 === 0) {
            logger.info(`Processed ${rowCount} rows from ${filePath}`);
          }
        })
        .on("end", () => {
          logger.info(
            `Finished reading CSV file: ${filePath}. Total rows: ${rowCount}`
          );
          resolve(results);
        })
        .on("error", (error) => {
          logger.error(`Error reading CSV file ${filePath}:`, error);
          reject(error);
        });
    });
  }

  static async readFiles(
    filePaths: string[]
  ): Promise<{ [fileName: string]: CSVRow[] }> {
    const results: { [fileName: string]: CSVRow[] } = {};

    logger.info(`Starting to read ${filePaths.length} CSV files`);

    for (const filePath of filePaths) {
      try {
        const fileName = filePath.split("/").pop() || filePath;
        results[fileName] = await this.readFile(filePath);
        logger.info(`Successfully read file: ${fileName}`);
      } catch (error) {
        logger.error(`Failed to read file: ${filePath}`, error);
        results[filePath] = [];
      }
    }

    return results;
  }

  /**
   * read a CSV file and return the headers
   */
  static async getHeaders(filePath: string): Promise<string[]> {
    return new Promise((resolve, reject) => {
      const headers: string[] = [];
      let isFirstRow = true;

      fs.createReadStream(filePath)
        .pipe(csv())
        .on("data", (data: CSVRow) => {
          if (isFirstRow) {
            headers.push(...Object.keys(data));
            isFirstRow = false;
          }
        })
        .on("end", () => {
          logger.info(`Headers from ${filePath}: ${headers.join(", ")}`);
          resolve(headers);
        })
        .on("error", (error) => {
          logger.error(`Error reading headers from ${filePath}:`, error);
          reject(error);
        });
    });
  }

  /**
   * read the first N rows of a CSV file for preview
   */
  static async previewFile(
    filePath: string,
    maxRows: number = 5
  ): Promise<CSVRow[]> {
    return new Promise((resolve, reject) => {
      const results: CSVRow[] = [];
      let rowCount = 0;

      fs.createReadStream(filePath)
        .pipe(csv())
        .on("data", (data: CSVRow) => {
          if (rowCount < maxRows) {
            results.push(data);
            rowCount++;
          } else {
            // stop reading
            return;
          }
        })
        .on("end", () => {
          logger.info(`Preview of ${filePath}: ${rowCount} rows`);
          resolve(results);
        })
        .on("error", (error) => {
          logger.error(`Error previewing ${filePath}:`, error);
          reject(error);
        });
    });
  }

  /**
   * validate a CSV file format
   */
  static async validateFile(filePath: string): Promise<{
    isValid: boolean;
    rowCount: number;
    headers: string[];
    errors: string[];
  }> {
    const errors: string[] = [];
    let rowCount = 0;
    let headers: string[] = [];
    let isFirstRow = true;

    return new Promise((resolve, reject) => {
      fs.createReadStream(filePath)
        .pipe(csv())
        .on("data", (data: CSVRow) => {
          if (isFirstRow) {
            headers = Object.keys(data);
            isFirstRow = false;

            // check if there are duplicate column names
            const uniqueHeaders = new Set(headers);
            if (uniqueHeaders.size !== headers.length) {
              errors.push("Duplicate column names found");
            }
          }

          rowCount++;

          // check if the number of columns in each row is consistent
          if (Object.keys(data).length !== headers.length) {
            errors.push(`Row ${rowCount} has inconsistent number of columns`);
          }
        })
        .on("end", () => {
          const isValid = errors.length === 0;
          logger.info(
            `File validation ${isValid ? "passed" : "failed"} for ${filePath}`
          );

          resolve({
            isValid,
            rowCount,
            headers,
            errors,
          });
        })
        .on("error", (error) => {
          logger.error(`Error validating ${filePath}:`, error);
          reject(error);
        });
    });
  }

  /**
   * get file statistics
   */
  static async getFileStats(filePath: string): Promise<{
    fileSize: number;
    rowCount: number;
    headers: string[];
    estimatedProcessingTime: number;
  }> {
    const stats = fs.statSync(filePath);
    const fileSize = stats.size;

    // read the first few rows to estimate the total number of rows
    const sampleRows = await this.previewFile(filePath, 100);
    const headers = Object.keys(sampleRows[0] || {});

    // estimate the total number of rows based on the file size and the number of sample rows
    const sampleSize = sampleRows.length;
    const sampleBytes = JSON.stringify(sampleRows).length;
    const estimatedRowCount = Math.floor((fileSize / sampleBytes) * sampleSize);

    // estimate the processing time (assuming each row takes about 1ms to process)
    const estimatedProcessingTime = estimatedRowCount * 1;

    return {
      fileSize,
      rowCount: estimatedRowCount,
      headers,
      estimatedProcessingTime,
    };
  }

  /**
   * Process CSV file in chunks and apply a callback function to each chunk
   */
  static async processFileInChunks(
    filePath: string,
    chunkSize: number = 1000,
    onChunk: (chunk: CSVRow[]) => Promise<void>
  ): Promise<{ totalRows: number; processedChunks: number }> {
    return new Promise((resolve, reject) => {
      let currentChunk: CSVRow[] = [];
      let totalRows = 0;
      let processedChunks = 0;

      logger.info(`Starting streaming processing of: ${filePath}`);

      fs.createReadStream(filePath)
        .pipe(csv())
        .on("data", async (data: CSVRow) => {
          currentChunk.push(data);
          totalRows++;

          // Process chunk when it reaches the specified size
          if (currentChunk.length >= chunkSize) {
            try {
              logger.info(`Processing chunk ${processedChunks + 1} with ${currentChunk.length} records...`);
              await onChunk([...currentChunk]);
              processedChunks++;
              currentChunk = [];
              
              logger.info(`✅ Successfully processed chunk ${processedChunks} (${totalRows} total rows processed)`);
            } catch (error) {
              logger.error(`❌ Error processing chunk ${processedChunks + 1}:`, error);
              reject(error);
            }
          }
        })
        .on("end", async () => {
          // Process remaining data in the last chunk
          if (currentChunk.length > 0) {
            try {
              await onChunk(currentChunk);
              processedChunks++;
              logger.info(`Processed final chunk ${processedChunks} (${totalRows} total rows)`);
            } catch (error) {
              logger.error(`Error processing final chunk:`, error);
              reject(error);
            }
          }

          logger.info(`Finished streaming processing: ${filePath}. Total rows: ${totalRows}`);
          resolve({ totalRows, processedChunks });
        })
        .on("error", (error) => {
          logger.error(`Error reading CSV file ${filePath}:`, error);
          reject(error);
        });
    });
  }


}
