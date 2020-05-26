package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesOwner

object OwnerTransformations {

  /** Parse all owner-related fields out of a raw RedCap record. */
  def mapOwner(rawRecord: RawRecord): HlesOwner = {
    val secondaryAddress = rawRecord.getBoolean("oc_address2_yn")
    val raceValues = rawRecord.fields.get("od_race")

    HlesOwner(
      // FIXME: Once DAP figures out a name for a dedicated owner ID, use that.
      ownerId = rawRecord.id,
      odAgeRangeYears = rawRecord.getOptionalNumber("od_age"),
      odMaxEducation = rawRecord.getOptionalNumber("od_education"),
      odMaxEducationOther = rawRecord.getOptional("od_education_other"),
      odRaceWhite = raceValues.map(_.contains("1")),
      odRaceBlackOrAfricanAmerican = raceValues.map(_.contains("2")),
      odRaceAsian = raceValues.map(_.contains("3")),
      odRaceAmericanIndian = raceValues.map(_.contains("4")),
      odRaceAlaskaNative = raceValues.map(_.contains("5")),
      odRaceNativeHawaiian = raceValues.map(_.contains("6")),
      odRaceOtherPacificIslander = raceValues.map(_.contains("7")),
      odRaceOther = raceValues.map(_.contains("98")),
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
