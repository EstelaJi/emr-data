-- CreateTable
CREATE TABLE "Caregivers" (
    "id" TEXT NOT NULL,
    "caregiver_id" TEXT NOT NULL,
    "external_id" TEXT,
    "profile_id" TEXT NOT NULL,
    "franchisor_id" TEXT NOT NULL,
    "agency_id" TEXT NOT NULL,
    "subdomain" TEXT,
    "first_name" TEXT NOT NULL,
    "last_name" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "phone_number" TEXT,
    "gender" TEXT,
    "applicant" BOOLEAN NOT NULL,
    "birthday_date" TIMESTAMP(3),
    "onboarding_date" TIMESTAMP(3),
    "location_name" TEXT,
    "locations_id" TEXT,
    "applicant_status" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Caregivers_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Carelog" (
    "id" TEXT NOT NULL,
    "carelog_id" TEXT NOT NULL,
    "caregiver_id" TEXT NOT NULL,
    "franchisor_id" TEXT NOT NULL,
    "agency_id" TEXT NOT NULL,
    "parent_id" TEXT,
    "start_datetime" TIMESTAMP(3) NOT NULL,
    "end_datetime" TIMESTAMP(3) NOT NULL,
    "clock_in_actual_datetime" TIMESTAMP(3),
    "clock_out_actual_datetime" TIMESTAMP(3),
    "clock_in_method" TEXT,
    "clock_out_method" TEXT,
    "status" TEXT NOT NULL,
    "split" BOOLEAN NOT NULL,
    "documentation" TEXT,
    "general_comment_char_count" INTEGER,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Carelog_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Caregivers_caregiver_id_key" ON "Caregivers"("caregiver_id");

-- CreateIndex
CREATE UNIQUE INDEX "Carelog_carelog_id_key" ON "Carelog"("carelog_id");

-- AddForeignKey
ALTER TABLE "Carelog" ADD CONSTRAINT "Carelog_caregiver_id_fkey" FOREIGN KEY ("caregiver_id") REFERENCES "Caregivers"("caregiver_id") ON DELETE RESTRICT ON UPDATE CASCADE;
