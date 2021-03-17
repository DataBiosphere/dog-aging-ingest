package org.broadinstitute.monster.dap.common

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesOwner

import scala.util.matching.Regex

object OwnerTransformations {

  // Parsing the oc_primary_residence_census_division
  // format is: "Division <N>: <some-description>"
  val censusDivisionPattern: Regex = "Division (\\d+):".r

  /**
    * Extract the Division value from the full text string.
    *
    * @param divisionString the text string to be parsed
    * @return an integer of the division number
    */
  def getCensusDivision(divisionString: String): Option[Long] =
    censusDivisionPattern
      .findFirstMatchIn(divisionString)
      .map(_.group(1).toLong)

  /** Parse all owner-related fields out of a raw RedCap record. */
  def mapOwner(rawRecord: RawRecord): Option[HlesOwner] = {
    val raceValues = rawRecord.fields.get("od_race")
    val otherRace = raceValues.map(_.contains("98"))

    val primaryAddressOwnership = rawRecord.getOptionalNumber("oc_address1_own")
    val secondaryAddress = rawRecord.getOptionalBoolean("oc_address2_yn")
    val secondaryAddressOwnership = secondaryAddress.flatMap {
      if (_) rawRecord.getOptionalNumber("oc_address2_own") else None
    }

    rawRecord.getOptional("st_owner_id") match {
      case None =>
        // we log an error for missing st_owner_id during dog transformation, so we just skip the record here.
        None
      case Some(owner_id) =>
        Some(
          HlesOwner(
            ownerId = owner_id.toLong,
            odAgeRangeYears = rawRecord.getOptionalNumber("od_age"),
            odMaxEducation = rawRecord.getOptionalNumber("od_education"),
            odRaceWhite = raceValues.map(_.contains("1")),
            odRaceBlackOrAfricanAmerican = raceValues.map(_.contains("2")),
            odRaceAsian = raceValues.map(_.contains("3")),
            odRaceAmericanIndian = raceValues.map(_.contains("4")),
            odRaceAlaskaNative = raceValues.map(_.contains("5")),
            odRaceNativeHawaiian = raceValues.map(_.contains("6")),
            odRaceOtherPacificIslander = raceValues.map(_.contains("7")),
            odRaceOther = otherRace,
            odRaceOtherDescription = otherRace.flatMap {
              if (_) rawRecord.getOptionalStripped("od_race_other") else None
            },
            odHispanic = rawRecord.getOptionalBoolean("od_hispanic_yn"),
            odAnnualIncomeRangeUsd = rawRecord.getOptionalNumber("od_income"),
            ocHouseholdPersonCount = rawRecord.getOptionalNumber("oc_people_household"),
            ocHouseholdAdultCount = rawRecord.getOptionalNumber("oc_adults_household"),
            ocHouseholdChildCount = rawRecord.getOptionalNumber("oc_children_household"),
            ssHouseholdDogCount = rawRecord.getOptionalNumber("ss_num_dogs_hh"),
            ocPrimaryResidenceState = rawRecord.getOptional("oc_address1_state"),
            ocPrimaryResidenceCensusDivision =
              rawRecord.getOptional("oc_address1_division").flatMap {
                getCensusDivision(_)
              },
            ocPrimaryResidenceOwnership = primaryAddressOwnership,
            ocPrimaryResidenceOwnershipOtherDescription =
              if (primaryAddressOwnership.contains(98)) {
                rawRecord.getOptionalStripped("oc_address1_own_other")
              } else {
                None
              },
            ocPrimaryResidenceTimePercentage = secondaryAddress.flatMap {
              if (_) rawRecord.getOptionalNumber("oc_address1_pct") else None
            },
            ocSecondaryResidence = secondaryAddress,
            ocSecondaryResidenceState = secondaryAddress.flatMap {
              if (_) rawRecord.getOptional("oc_address2_state") else None
            },
            ocSecondaryResidenceOwnership = secondaryAddressOwnership,
            ocSecondaryResidenceOwnershipOtherDescription =
              if (secondaryAddressOwnership.contains(98)) {
                rawRecord.getOptionalStripped("oc_address2_own_other")
              } else {
                None
              },
            ocSecondaryResidenceTimePercentage = secondaryAddress.flatMap {
              if (_) rawRecord.getOptionalNumber("oc_2nd_address_pct") else None
            }
          )
        )
    }
  }
}
