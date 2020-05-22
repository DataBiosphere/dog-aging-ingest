package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesOwner

object OwnerTransformations {

  /** Parse all owner-related fields out of a raw RedCap record. */
  def mapOwner(rawRecord: RawRecord): HlesOwner = {
    val secondaryAddress = rawRecord.getBoolean("oc_address2_yn")
    val raceField = "od_race"

    HlesOwner(
      // FIXME: Once DAP figures out a name for a dedicated owner ID, use that.
      ownerId = rawRecord.id,
      odAgeRangeYears = rawRecord.getOptionalNumber("od_age"),
      odMaxEducation = rawRecord.getOptionalNumber("od_education"),
      odMaxEducationOther = rawRecord.getOptional("od_education_other"),
      odRaceWhite = rawRecord.containsValue(raceField, "1"),
      odRaceBlackOrAfricanAmerican = rawRecord.containsValue(raceField, "2"),
      odRaceAsian = rawRecord.containsValue(raceField, "3"),
      odRaceAmericanIndian = rawRecord.containsValue(raceField, "4"),
      odRaceAlaskaNative = rawRecord.containsValue(raceField, "5"),
      odRaceNativeHawaiian = rawRecord.containsValue(raceField, "6"),
      odRaceOtherPacificIslander = rawRecord.containsValue(raceField, "7"),
      odRaceOther = rawRecord.containsValue(raceField, "98"),
      odRaceOtherDescription = rawRecord.getOptional("od_race_other"),
      odHispanic = rawRecord.getOptionalBoolean("od_hispanic_yn"),
      odAnnualIncomeRangeUsd = rawRecord.getOptionalNumber("od_income"),
      ocHouseholdPersonCount = rawRecord.getOptionalNumber("oc_people_household"),
      ocHouseholdAdultCount = rawRecord.getOptionalNumber("oc_adults_household"),
      ocHouseholdChildCount = rawRecord.getOptionalNumber("oc_children_household"),
      ssHouseholdDogCount = rawRecord.getOptionalNumber("ss_num_dogs_hh"),
      ocPrimaryResidenceState = rawRecord.getOptional("oc_address1_state"),
      ocPrimaryResidenceCensusDivision = rawRecord.getOptionalNumber("oc_address1_division"),
      ocPrimaryResidenceZip = rawRecord.getOptionalNumber("oc_address1_zip"),
      ocPrimaryResidenceOwnership = rawRecord.getOptionalNumber("oc_address1_own"),
      ocPrimaryResidenceOwnershipOther = rawRecord.getOptional("oc_address1_own_other"),
      ocSecondaryResidenceState =
        if (secondaryAddress) rawRecord.getOptional("oc_address2_state") else None,
      ocSecondaryResidenceZip =
        if (secondaryAddress) rawRecord.getOptionalNumber("oc_address2_zip") else None,
      ocSecondaryResidenceOwnership =
        if (secondaryAddress) rawRecord.getOptionalNumber("oc_address2_own") else None,
      ocSecondaryResidenceOwnershipOther =
        if (secondaryAddress) rawRecord.getOptional("oc_address2_own_other") else None
    )
  }
}
