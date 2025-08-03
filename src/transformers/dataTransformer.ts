import { Caregiver, Carelog, CSVRow, TransformRule } from "../types";
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

  static toCaregiverData(row: CSVRow): Caregiver {
    const rules = this.getCaregiverTransformRules();
    const transformedData = this.transformData(row, rules);
    return {
      caregiverId: transformedData.caregiver_id as string,
      externalId: transformedData.external_id as string,
      profileId: transformedData.profile_id as string,
      franchisorId: transformedData.franchisor_id as string,
      agencyId: transformedData.agency_id as string,
      subdomain: transformedData.subdomain as string,
      firstName: transformedData.first_name as string,
      lastName: transformedData.last_name as string,
      email: transformedData.email as string,
      phoneNumber: transformedData.phone_number as string,
      gender: transformedData.gender as string,
      applicant: transformedData.applicant as boolean,
      birthdayDate: transformedData.birthday_date as Date | null,
      onboardingDate: transformedData.onboarding_date as Date | null,
      locationName: transformedData.location_name as string,
      locationsId: transformedData.locations_id as string,
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
    return data.map((row) => this.toCaregiverData(row));
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
        field: "subdomain",
        transform: (value: any) => String(value || ""),
        required: false,
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
        transform: (value: any) => String(value || ""),
        required: true,
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
        field: "location_name",
        transform: (value: any) => String(value || ""),
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
