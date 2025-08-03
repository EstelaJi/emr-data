import * as fs from "fs";
import * as path from "path";
import { logger } from "./logger";

export class FileUtils {
  static ensureDirectoryExists(dirPath: string) {
    if (!fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath);
    }
  }

  static getCSVFiles(directory: string): string[] {
    try {
      this.ensureDirectoryExists(directory);
      const files = fs.readdirSync(directory);
      return files
        .filter((file) => file.toLowerCase().endsWith(".csv"))
        .map((file) => path.join(directory, file));
    } catch (error) {
      logger.error(`Error reading  directory ${directory}:`, error);
      return [];
    }
  }
}
