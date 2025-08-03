export interface CSVRow {
  [key: string]: string | number | boolean | null;
}

export interface ETLResult {
  success: boolean;
  recordsRead: number;
  recordsProcessed: number;
  recordsFailed: number;
  processingTime: number;
  errors: string[];
}

export interface TransformRule {
  field: string;
  transform: (value: any) => any;
  required?: boolean;
}

export interface Caregiver {
  caregiverId: string;
  externalId?: string;
  profileId: string;
  franchisorId: string;
  agencyId: string;
  subdomain: string;
  firstName: string;
  lastName: string;
  email?: string;
  phoneNumber?: string;
  gender?: string;
  applicant: boolean;
  birthdayDate?: Date | null;
  onboardingDate?: Date | null;
  locationName?: string;
  locationsId?: string;
  applicantStatus: string;
  status: string;
}

export interface Carelog {
  carelogId: string;
  caregiverId: string;
  franchisorId: string;
  agencyId: string;
  parentId?: string;
  startDatetime: Date | null;
  endDatetime: Date | null;
  clockInActualDatetime?: Date | null;
  clockOutActualDatetime?: Date | null;
  clockInMethod?: string;
  clockOutMethod?: string;
  status: string;
  split: boolean;
  documentation?: string;
  generalCommentCharCount?: number;
}