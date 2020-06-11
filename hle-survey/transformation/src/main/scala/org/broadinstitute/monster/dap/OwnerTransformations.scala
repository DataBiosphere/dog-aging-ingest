package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesOwner

object OwnerTransformations {

  /** Parse all owner-related fields out of a raw RedCap record. */
  def mapOwner(rawRecord: RawRecord): HlesOwner = {
    val education = rawRecord.getOptionalNumber("od_education")

    val raceValues = rawRecord.fields.get("od_race")
    val otherRace = raceValues.map(_.contains("98"))

    val primaryAddressOwnership = rawRecord.getOptionalNumber("oc_address1_own")
    val secondaryAddress = rawRecord.getOptionalBoolean("oc_address2_yn")
    val secondaryAddressOwnership = secondaryAddress.flatMap {
      if (_) rawRecord.getOptionalNumber("oc_address2_own") else None
    }
    
    // Parsing the oc_primary_residence_census_division
    // format is: "Division <N>: <some-description>"
    val censusDivisionPattern: Regex = "(Division \\d+:)"
    
    /**
      * Extract the Division value from the full text string.
      *
      * @param divisionString the text string to be parsed
      * @return an integer of the division number
    **/
    def getCensusDivision(divisionString: String): (integer) = {
      val matches = censusDivisionPattern
        .findFirstMatchIn(divisionString)
        .getOrElse(
          throw new Exception(
            s"ownerTransformations: error while parsing census division id from $divisionString"
          )
        )
      val censusDivisionId = matches.group(1)
      censusDivisionId
    }

    HlesOwner(
      ownerId = rawRecord.getRequired("st_owner_id").toLong,
      odAgeRangeYears = rawRecord.getOptionalNumber("od_age"),
      odMaxEducation = education,
      odMaxEducationOtherDescription = if (education.contains(98)) {
        rawRecord.getOptional("od_education_other")
      } else {
        None
      },
      odRaceWhite = raceValues.map(_.contains("1")),
      odRaceBlackOrAfricanAmerican = raceValues.map(_.contains("2")),
      odRaceAsian = raceValues.map(_.contains("3")),
      odRaceAmericanIndian = raceValues.map(_.contains("4")),
      odRaceAlaskaNative = raceValues.map(_.contains("5")),
      odRaceNativeHawaiian = raceValues.map(_.contains("6")),
      odRaceOtherPacificIslander = raceValues.map(_.contains("7")),
      odRaceOther = otherRace,
      odRaceOtherDescription = otherRace.flatMap {
        if (_) rawRecord.getOptional("od_race_other") else None
      },
      odHispanic = rawRecord.getOptionalBoolean("od_hispanic_yn"),
      odAnnualIncomeRangeUsd = rawRecord.getOptionalNumber("od_income"),
      ocHouseholdPersonCount = rawRecord.getOptionalNumber("oc_people_household"),
      ocHouseholdAdultCount = rawRecord.getOptionalNumber("oc_adults_household"),
      ocHouseholdChildCount = rawRecord.getOptionalNumber("oc_children_household"),
      ssHouseholdDogCount = rawRecord.getOptionalNumber("ss_num_dogs_hh"),
      ocPrimaryResidenceState = rawRecord.getOptional("oc_address1_state"),
      ocPrimaryResidenceCensusDivision = getCensusDivision(rawRecord.getOptional("oc_address1_division")),

            ocSecondaryResidenceState = secondaryAddress.flatMap {
        if (_) rawRecord.getOptional("oc_address2_state") else None
      },
      
      ocPrimaryResidenceZip = rawRecord.getOptional("oc_address1_zip"),
      ocPrimaryResidenceOwnership = primaryAddressOwnership,
      ocPrimaryResidenceOwnershipOtherDescription = if (primaryAddressOwnership.contains(98)) {
        rawRecord.getOptional("oc_address1_own_other")
      } else {
        None
      },
      ocSecondaryResidence = secondaryAddress,
      ocSecondaryResidenceState = secondaryAddress.flatMap {
        if (_) rawRecord.getOptional("oc_address2_state") else None
      },
      ocSecondaryResidenceZip = secondaryAddress.flatMap {
        if (_) rawRecord.getOptional("oc_address2_zip") else None
      },
      ocSecondaryResidenceOwnership = secondaryAddressOwnership,
      ocSecondaryResidenceOwnershipOtherDescription = if (secondaryAddressOwnership.contains(98)) {
        rawRecord.getOptional("oc_address2_own_other")
      } else {
        None
      }
    )
  }
}
