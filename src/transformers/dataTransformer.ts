import { Agency, Caregiver, Carelog, CSVRow, Franchisor, Location, TransformRule } from "../types";
import { logger } from "../utils/logger";

export class DataTransformer {
  static transformData(data: CSVRow, rules: TransformRule[]): CSVRow {
    const transformedData: CSVRow = { ...data };

    for (const rule of rules) {
      if (transformedData.hasOwnProperty(rule.field)) {
        transformedData[rule.field] = rule.transform(
          transformedData[rule.field]
        );
      } else if (rule.required) {
        logger.error(
          `Field ${rule.field} is required but not found in the data`
        );
      }
    }
    return transformedData;
  }

  static transformDataset(data: CSVRow[], rules: TransformRule[]): CSVRow[] {
    return data.map((row) => this.transformData(row, rules));
  }

  // Extract unique franchisors from caregiver data
  static extractFranchisors(data: CSVRow[]): Franchisor[] {
    const franchisorMap = new Map<string, Franchisor>();
    
    data.forEach(row => {
      const franchisorId = String(row.franchisor_id || "");
      if (franchisorId && !franchisorMap.has(franchisorId)) {
        franchisorMap.set(franchisorId, {
          franchisorId,
          name: String(row.franchisor_name || "")
        });
      }
    });
    
    return Array.from(franchisorMap.values());
  }

  // Extract unique agencies from caregiver data
  static extractAgencies(data: CSVRow[]): Agency[] {
    const agencyMap = new Map<string, Agency>();
    
    data.forEach(row => {
      const agencyId = String(row.agency_id || "");
      const franchisorId = String(row.franchisor_id || "");
      
      if (agencyId && franchisorId) {
        const key = `${agencyId}-${franchisorId}`;
        if (!agencyMap.has(key)) {
          agencyMap.set(key, {
            agencyId,
            name: String(row.agency_name || ""),
            franchisorId,
            subdomain: String(row.subdomain || "")
          });
        }
      }
    });
    
    return Array.from(agencyMap.values());
  }

  // Extract unique locations from caregiver data
  static extractLocations(data: CSVRow[]): Location[] {
    const locationMap = new Map<string, Location>();
    
    data.forEach(row => {
      const locationId = String(row.locations_id || "");
      const locationName = String(row.location_name || "");
      
      if (locationId && locationName && !locationMap.has(locationId)) {
        locationMap.set(locationId, {
          locationId,
          locationName
        });
      }
    });
    
    return Array.from(locationMap.values());
  }

  static toCaregiverData(row: CSVRow): Caregiver {
    const rules = this.getCaregiverTransformRules();
    const transformedData = this.transformData(row, rules);
    return {
      caregiverId: transformedData.caregiver_id as string,
      externalId: transformedData.external_id as string,
      profileId: transformedData.profile_id as string,
      franchisorId: transformedData.franchisor_id as string,
      agencyId: transformedData.agency_id as string,
      locationId: transformedData.locations_id as string,
      firstName: transformedData.first_name as string,
      lastName: transformedData.last_name as string,
      email: transformedData.email as string,
      phoneNumber: transformedData.phone_number as string,
      gender: transformedData.gender as string,
      applicant: transformedData.applicant as boolean,
      birthdayDate: transformedData.birthday_date as Date | null,
      onboardingDate: transformedData.onboarding_date as Date | null,
      applicantStatus: transformedData.applicant_status as string,
      status: transformedData.status as string,
    };
  }

  static toCarelogData(row: CSVRow): Carelog {
    const rules = this.getCarelogTransformRules();
    const transformedData = this.transformData(row, rules);
    return {
      carelogId: transformedData.carelog_id as string,
      caregiverId: transformedData.caregiver_id as string,
      franchisorId: transformedData.franchisor_id as string,
      agencyId: transformedData.agency_id as string,
      parentId: transformedData.parent_id as string,
      startDatetime: transformedData.start_datetime as Date | null,
      endDatetime: transformedData.end_datetime as Date | null,
      clockInActualDatetime: transformedData.clock_in_actual_datetime as Date | null,
      clockOutActualDatetime:
        transformedData.clock_out_actual_datetime as Date | null,
      clockInMethod: transformedData.clock_in_method as string,
      clockOutMethod: transformedData.clock_out_method as string,
      status: transformedData.status as string,
      split: transformedData.split as boolean,
      documentation: transformedData.documentation as string,
      generalCommentCharCount:
        transformedData.general_comment_char_count as number,
    };
  }

  static transformCaregivers(data: CSVRow[]): Caregiver[] {
    const results: Caregiver[] = [];
    const invalidRows: { index: number; row: CSVRow; error: string }[] = [];
    
    data.forEach((row, index) => {
      try {
        const caregiver = this.toCaregiverData(row);
        if (caregiver) {
          results.push(caregiver);
        } else {
          invalidRows.push({ index, row, error: 'Transformation returned null' });
        }
      } catch (error) {
        invalidRows.push({ index, row, error: error instanceof Error ? error.message : String(error) });
      }
    });
    
    if (invalidRows.length > 0) {
      console.log(`⚠️ DataTransformer: ${invalidRows.length} invalid rows out of ${data.length} total rows`);
      console.log(`⚠️ Sample invalid rows:`, invalidRows.slice(0, 3));
    }
    
    return results;
  }

  static transformCarelogs(data: CSVRow[]): Carelog[] {
    return data.map((row) => this.toCarelogData(row));
  }

  static getCaregiverTransformRules(): TransformRule[] {
    return [
      {
        field: "franchisor_id",
        transform: (value: any) => String(value || ""),
        required: true,
      },
      {
        field: "agency_id",
        transform: (value: any) => String(value || ""),
        required: true,
      },
      {
        field: "profile_id",
        transform: (value: any) => String(value || ""),
        required: true,
      },
      {
        field: "caregiver_id",
        transform: (value: any) => String(value || ""),
        required: true,
      },
      {
        field: "external_id",
        transform: (value: any) => String(value || ""),
        required: false,
      },
      {
        field: "first_name",
        transform: (value: any) => String(value || ""),
        required: true,
      },
      {
        field: "last_name",
        transform: (value: any) => String(value || ""),
        required: true,
      },
      {
        field: "email",
        transform: (value: any) => {
          if (!value || value.trim() === '') {
            return `no-email-${Date.now()}-${Math.random().toString(36).substr(2, 9)}@placeholder.com`;
          }
          return String(value);
        },
        required: false,
      },
      {
        field: "phone_number",
        transform: (value: any) => String(value || ""),
        required: false,
      },
      {
        field: "gender",
        transform: (value: any) => String(value || ""),
        required: false,
      },
      {
        field: "applicant",
        transform: (value: any) => {
          if (typeof value === "boolean") return value;
          if (typeof value === "string") return value.toLowerCase() === "true";
          return false;
        },
        required: true,
      },
      {
        field: "birthday_date",
        transform: (value: any) => {
          if (!value || value === "" || value === "None") return null;
          try {
            const date = new Date(value);
            return isNaN(date.getTime()) ? null : date;
          } catch {
            return null;
          }
        },
        required: false,
      },
      {
        field: "onboarding_date",
        transform: (value: any) => {
          if (!value || value === "" || value === "None") return null;
          try {
            const date = new Date(value);
            return isNaN(date.getTime()) ? null : date;
          } catch {
            return null;
          }
        },
        required: false,
      },
      {
        field: "locations_id",
        transform: (value: any) => String(value || ""),
        required: false,
      },
      {
        field: "applicant_status",
        transform: (value: any) => String(value || ""),
        required: true,
      },
      {
        field: "status",
        transform: (value: any) => String(value || ""),
        required: true,
      },
    ];
  }

  static getCarelogTransformRules(): TransformRule[] {
    return [
      {
        field: "franchisor_id",
        transform: (value: any) => String(value || ""),
        required: true,
      },
      {
        field: "agency_id",
        transform: (value: any) => String(value || ""),
        required: true,
      },
      {
        field: "carelog_id",
        transform: (value: any) => String(value || ""),
        required: true,
      },
      {
        field: "caregiver_id",
        transform: (value: any) => String(value || ""),
        required: true,
      },
      {
        field: "parent_id",
        transform: (value: any) => String(value || ""),
        required: false,
      },
      {
        field: "start_datetime",
        transform: (value: any) => {
          if (!value || value === "" || value === "None") return null;
          try {
            const date = new Date(value);
            return isNaN(date.getTime()) ? null : date;
          } catch {
            return null;
          }
        },
        required: true,
      },
      {
        field: "end_datetime",
        transform: (value: any) => {
          if (!value || value === "" || value === "None") return null;
          try {
            const date = new Date(value);
            return isNaN(date.getTime()) ? null : date;
          } catch {
            return null;
          }
        },
        required: true,
      },
      {
        field: "clock_in_actual_datetime",
        transform: (value: any) => {
          if (!value || value === "" || value === "None") return null;
          try {
            const date = new Date(value);
            return isNaN(date.getTime()) ? null : date;
          } catch {
            return null;
          }
        },
        required: false,
      },
      {
        field: "clock_out_actual_datetime",
        transform: (value: any) => {
          if (!value || value === "" || value === "None") return null;
          try {
            const date = new Date(value);
            return isNaN(date.getTime()) ? null : date;
          } catch {
            return null;
          }
        },
        required: false,
      },
      {
        field: "clock_in_method",
        transform: (value: any) => String(value || ""),
        required: false,
      },
      {
        field: "clock_out_method",
        transform: (value: any) => String(value || ""),
        required: false,
      },
      {
        field: "status",
        transform: (value: any) => String(value || ""),
        required: true,
      },
      {
        field: "split",
        transform: (value: any) => {
          if (typeof value === "boolean") return value;
          if (typeof value === "string") return value.toLowerCase() === "true";
          return false;
        },
        required: true,
      },
      {
        field: "documentation",
        transform: (value: any) => String(value || ""),
        required: false,
      },
      {
        field: "general_comment_char_count",
        transform: (value: any) => {
          const num = Number(value);
          return isNaN(num) ? 0 : num;
        },
        required: false,
      },
    ];
  }
}
