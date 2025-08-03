-- CreateTable
CREATE TABLE "Franchisor" (
    "id" TEXT NOT NULL,
    "franchisorId" TEXT NOT NULL,
    "name" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Franchisor_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Agency" (
    "id" TEXT NOT NULL,
    "agencyId" TEXT NOT NULL,
    "name" TEXT,
    "franchisorId" TEXT NOT NULL,
    "subdomain" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Agency_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Location" (
    "id" TEXT NOT NULL,
    "locationId" TEXT NOT NULL,
    "locationName" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Location_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Caregiver" (
    "id" TEXT NOT NULL,
    "caregiverId" TEXT NOT NULL,
    "externalId" TEXT,
    "profileId" TEXT NOT NULL,
    "franchisorId" TEXT NOT NULL,
    "agencyId" TEXT NOT NULL,
    "locationId" TEXT,
    "firstName" TEXT NOT NULL,
    "lastName" TEXT NOT NULL,
    "email" TEXT,
    "phoneNumber" TEXT,
    "gender" TEXT,
    "applicant" BOOLEAN NOT NULL,
    "birthdayDate" TIMESTAMP(3),
    "onboardingDate" TIMESTAMP(3),
    "applicantStatus" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Caregiver_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Carelog" (
    "id" TEXT NOT NULL,
    "carelogId" TEXT NOT NULL,
    "caregiverId" TEXT NOT NULL,
    "franchisorId" TEXT NOT NULL,
    "agencyId" TEXT NOT NULL,
    "parentId" TEXT,
    "startDatetime" TIMESTAMP(3),
    "endDatetime" TIMESTAMP(3),
    "clockInActualDatetime" TIMESTAMP(3),
    "clockOutActualDatetime" TIMESTAMP(3),
    "clockInMethod" TEXT,
    "clockOutMethod" TEXT,
    "status" TEXT NOT NULL,
    "split" BOOLEAN NOT NULL,
    "documentation" TEXT,
    "generalCommentCharCount" INTEGER,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Carelog_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Franchisor_franchisorId_key" ON "Franchisor"("franchisorId");

-- CreateIndex
CREATE UNIQUE INDEX "Agency_agencyId_franchisorId_key" ON "Agency"("agencyId", "franchisorId");

-- CreateIndex
CREATE UNIQUE INDEX "Location_locationId_key" ON "Location"("locationId");

-- CreateIndex
CREATE UNIQUE INDEX "Caregiver_caregiverId_key" ON "Caregiver"("caregiverId");

-- CreateIndex
CREATE UNIQUE INDEX "Caregiver_caregiverId_franchisorId_agencyId_key" ON "Caregiver"("caregiverId", "franchisorId", "agencyId");

-- CreateIndex
CREATE UNIQUE INDEX "Carelog_carelogId_key" ON "Carelog"("carelogId");

-- AddForeignKey
ALTER TABLE "Agency" ADD CONSTRAINT "Agency_franchisorId_fkey" FOREIGN KEY ("franchisorId") REFERENCES "Franchisor"("franchisorId") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Caregiver" ADD CONSTRAINT "Caregiver_franchisorId_fkey" FOREIGN KEY ("franchisorId") REFERENCES "Franchisor"("franchisorId") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Caregiver" ADD CONSTRAINT "Caregiver_agencyId_franchisorId_fkey" FOREIGN KEY ("agencyId", "franchisorId") REFERENCES "Agency"("agencyId", "franchisorId") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Caregiver" ADD CONSTRAINT "Caregiver_locationId_fkey" FOREIGN KEY ("locationId") REFERENCES "Location"("locationId") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Carelog" ADD CONSTRAINT "Carelog_caregiverId_fkey" FOREIGN KEY ("caregiverId") REFERENCES "Caregiver"("caregiverId") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Carelog" ADD CONSTRAINT "Carelog_franchisorId_fkey" FOREIGN KEY ("franchisorId") REFERENCES "Franchisor"("franchisorId") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Carelog" ADD CONSTRAINT "Carelog_agencyId_franchisorId_fkey" FOREIGN KEY ("agencyId", "franchisorId") REFERENCES "Agency"("agencyId", "franchisorId") ON DELETE RESTRICT ON UPDATE CASCADE;
