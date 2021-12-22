package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.afus.AfusTransformationPipelineBuilder.logger
import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.AfusOwner

object AfusOwnerTransformations {

  def mapAfusOwnerData(rawRecord: RawRecord): Option[AfusOwner] = {
    rawRecord.getOptionalNumber("st_owner_id") match {
      case None =>
        MissingOwnerIdError(s"Record has less than 1 value for field st_owner_id").log
        None
      case Some(owner_id) =>
        try {
          Some(
            AfusOwner(
              ownerId = owner_id,
              afusOcHouseholdPersonCount = rawRecord.getOptionalNumber("fu_oc_people_household"),
              afusOcHouseholdAdultCount = rawRecord.getOptionalNumber("fu_oc_adults_household"),
              afusOcHouseholdChildCount = rawRecord.getOptionalNumber("fu_oc_children_household"),
              afusOcPrimaryAddressChange = rawRecord.getOptionalBoolean("fu_oc_address_change"),
              afusOcPrimaryAddressChangeDate =
                rawRecord.getOptionalDate("fu_oc_address1_change_date"),
              afusOcPrimaryResidenceOwnership = rawRecord.getOptionalNumber("fu_oc_address1_own"),
              afusOcPrimaryResidenceOwnershipOtherDescription =
                rawRecord.getOptionalStripped("fu_oc_address1_own_other"),
              afusOcPrimaryResidenceState = rawRecord.getOptionalStripped("fu_oc_address1_state"),
              afusOcPrimaryResidenceCensusDivision =
                rawRecord.getOptionalNumber("fu_oc_address1_division"),
              afusOcPrimaryResidenceTimePercentage =
                rawRecord.getOptionalNumber("fu_oc_address1_pct"),
              afusOcSecondaryResidence = rawRecord.getOptionalNumber("fu_oc_address2_yn"),
              afusOcSecondaryAddressChange = rawRecord.getOptionalBoolean("fu_oc_address2_change"),
              afusOcSecondaryResidenceChangeDate =
                rawRecord.getOptionalDate("fu_oc_address2_change_date"),
              afusOcSecondaryResidenceOwnership = rawRecord.getOptionalNumber("fu_oc_address2_own"),
              afusOcSecondaryResidenceOwnershipOtherDescription =
                rawRecord.getOptionalStripped("fu_oc_address2_own_other"),
              afusOcSecondaryResidenceState = rawRecord.getOptionalStripped("fu_oc_address2_state"),
              afusOcSecondaryResidenceTimePercentage =
                rawRecord.getOptionalNumber("fu_oc_2nd_address_pct"),
              afusOdAgeRangeYears = rawRecord.getOptionalNumber("fu_od_age"),
              afusOdMaxEducation = rawRecord.getOptionalNumber("fu_od_education"),
              afusOdMaxEducationOtherDescription =
                rawRecord.getOptionalStripped("fu_od_education_other"),
              afusOdAnnualIncomeRangeUsd = rawRecord.getOptionalNumber("fu_od_income")
            )
          )
        } catch {
          case e: AFUSTransformationError =>
            e.log
            None
        }
    }
  }
}
