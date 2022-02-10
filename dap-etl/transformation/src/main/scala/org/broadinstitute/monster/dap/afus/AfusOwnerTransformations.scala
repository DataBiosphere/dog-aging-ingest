package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.afus.AfusTransformationPipelineBuilder.logger
import org.broadinstitute.monster.dap.common.OwnerTransformations.getCensusDivision
import org.broadinstitute.monster.dap.common.{MissingOwnerIdError, RawRecord, TransformationError}
import org.broadinstitute.monster.dogaging.jadeschema.table.AfusOwner

object AfusOwnerTransformations {

  def mapAfusOwnerData(rawRecord: RawRecord): Option[AfusOwner] = {
    rawRecord.getOptionalNumber("st_owner_id") match {
      case None =>
        MissingOwnerIdError(s"Record has less than 1 value for field st_owner_id").log
        None
      case Some(owner_id) =>
        val primaryAddressChange = rawRecord.getOptionalBoolean("fu_oc_address_change")
        val primaryAddressOwnership =
          if (primaryAddressChange.contains(true)) rawRecord.getOptionalNumber("fu_oc_address1_own")
          else None
        val secondaryAddress = rawRecord.getOptionalNumber("fu_oc_address2_yn")
        val secondaryAddressChange =
          if (secondaryAddress.getOrElse(0) != 0)
            rawRecord.getOptionalBoolean("fu_oc_address2_change")
          else None
        val secondaryAddressOwnership =
          if (secondaryAddressChange.contains(true))
            rawRecord.getOptionalNumber("fu_oc_address2_own")
          else None
        try {
          Some(
            AfusOwner(
              ownerId = owner_id,
              afusOcHouseholdPersonCount = rawRecord.getOptionalNumber("fu_oc_people_household"),
              afusOcHouseholdAdultCount = rawRecord.getOptionalNumber("fu_oc_adults_household"),
              afusOcHouseholdChildCount = rawRecord.getOptionalNumber("fu_oc_children_household"),
              afusOcPrimaryResidenceChange = primaryAddressChange,
              afusOcPrimaryResidenceChangeDate =
                if (primaryAddressChange.contains(true))
                  rawRecord.getOptionalDate("fu_oc_address1_change_date")
                else None,
              afusOcPrimaryResidenceOwnership = primaryAddressOwnership,
              afusOcPrimaryResidenceOwnershipOtherDescription =
                if (primaryAddressOwnership.contains(98))
                  rawRecord.getOptionalStripped("fu_oc_address1_own_other")
                else None,
              afusOcPrimaryResidenceState =
                if (primaryAddressChange.contains(true))
                  rawRecord.getOptionalStripped("fu_oc_address1_state")
                else None,
              afusOcPrimaryResidenceCensusDivision =
                if (primaryAddressChange.contains(true))
                  rawRecord.getOptional("fu_oc_address1_division").flatMap {
                    getCensusDivision(_)
                  }
                else None,
              afusOcPrimaryResidenceTimePercentage =
                if (primaryAddressChange.contains(true))
                  rawRecord.getOptionalNumber("fu_oc_address1_pct")
                else None,
              afusOcSecondaryResidence = secondaryAddress,
              afusOcSecondaryResidenceChange = secondaryAddressChange,
              afusOcSecondaryResidenceChangeDate =
                if (secondaryAddressChange.contains(true))
                  rawRecord.getOptionalDate("fu_oc_address2_change_date")
                else None,
              afusOcSecondaryResidenceOwnership = secondaryAddressOwnership,
              afusOcSecondaryResidenceOwnershipOtherDescription =
                if (secondaryAddressOwnership.contains(98))
                  rawRecord.getOptionalStripped("fu_oc_address2_own_other")
                else None,
              afusOcSecondaryResidenceState =
                if (secondaryAddressChange.contains(true))
                  rawRecord.getOptionalStripped("fu_oc_address2_state")
                else None,
              afusOcSecondaryResidenceTimePercentage =
                if (secondaryAddressChange.contains(true))
                  rawRecord.getOptionalNumber("fu_oc_2nd_address_pct")
                else None,
              afusOdAgeRangeYears = rawRecord.getOptionalNumber("fu_od_age"),
              afusOdMaxEducation = rawRecord.getOptionalNumber("fu_od_education"),
              afusOdMaxEducationOtherDescription =
                rawRecord.getOptionalStripped("fu_od_education_other"),
              afusOdAnnualIncomeRangeUsd = rawRecord.getOptionalNumber("fu_od_income")
            )
          )
        } catch {
          case e: TransformationError =>
            e.log
            None
        }
    }
  }
}
