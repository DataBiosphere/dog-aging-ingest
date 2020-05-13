package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesOwner

object OwnerTransformations {

  /** Parse all owner-related fields out of a raw RedCap record. */
  def mapOwner(rawRecord: RawRecord): HlesOwner = {
    val secondaryAddress = rawRecord.getBoolean("oc_address2_yn")
    HlesOwner(
      // FIXME: Once DAP figures out a name for a dedicated owner ID, use that.
      ownerId = rawRecord.id,
      odAgeRangeYears = rawRecord.getOptional("od_age"),
      odMaxEducation = rawRecord.getOptional("od_education"),
      odMaxEducationOther = rawRecord.getOptional("od_education_other"),
      odRace = rawRecord.getArray("od_race"),
      odRaceOther = rawRecord.getOptional("od_race_other"),
      odHispanic = rawRecord.getOptionalBoolean("od_hispanic_yn"),
      odAnnualIncomeRangeUsd = rawRecord.getOptional("od_income"),
      ocHouseholdPersonCount = rawRecord.getOptional("oc_people_household"),
      ocHouseholdAdultCount = rawRecord.getOptional("oc_adults_household"),
      ocHouseholdChildCount = rawRecord.getOptional("oc_children_household"),
      ssHouseholdDogCount = rawRecord.getOptional("ss_num_dogs_hh"),
      ocPrimaryResidenceState = rawRecord.getOptional("oc_address1_state"),
      ocPrimaryResidenceCensusDivision = rawRecord.getOptional("oc_address1_division"),
      ocPrimaryResidenceZip = rawRecord.getOptional("oc_address1_zip"),
      ocPrimaryResidenceOwnership = rawRecord.getOptional("oc_address1_own"),
      ocPrimaryResidenceOwnershipOther = rawRecord.getOptional("oc_address1_own_other"),
      ocSecondaryResidenceState =
        if (secondaryAddress) rawRecord.getOptional("oc_address2_state") else None,
      ocSecondaryResidenceZip =
        if (secondaryAddress) rawRecord.getOptional("oc_address2_zip") else None,
      ocSecondaryResidenceOwnership =
        if (secondaryAddress) rawRecord.getOptional("oc_address2_own") else None,
      ocSecondaryResidenceOwnershipOther =
        if (secondaryAddress) rawRecord.getOptional("oc_address2_own_other") else None
    )
  }
}
