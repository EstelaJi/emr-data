-- DropForeignKey
ALTER TABLE "Caregiver" DROP CONSTRAINT "Caregiver_agencyId_fkey";

-- DropForeignKey
ALTER TABLE "Carelog" DROP CONSTRAINT "Carelog_agencyId_fkey";

-- DropIndex
DROP INDEX "Agency_agencyId_key";

-- AddForeignKey
ALTER TABLE "Caregiver" ADD CONSTRAINT "Caregiver_agencyId_franchisorId_fkey" FOREIGN KEY ("agencyId", "franchisorId") REFERENCES "Agency"("agencyId", "franchisorId") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Carelog" ADD CONSTRAINT "Carelog_agencyId_franchisorId_fkey" FOREIGN KEY ("agencyId", "franchisorId") REFERENCES "Agency"("agencyId", "franchisorId") ON DELETE RESTRICT ON UPDATE CASCADE;
