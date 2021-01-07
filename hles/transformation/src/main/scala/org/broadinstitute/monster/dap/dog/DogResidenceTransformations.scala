package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogResidences
import org.broadinstitute.monster.dap.OwnerTransformations

object DogResidenceTransformations {

  /** Parse all residence-related fields out of a raw RedCap record. */
  def mapDogResidences(rawRecord: RawRecord): HlesDogResidences = {
    val hasSecondaryResidence = rawRecord.getOptionalBoolean("oc_address2_yn")
    val hasTertiaryResidences = rawRecord.getOptionalBoolean("dd_2nd_residence_yn")
    val tertiaryResidenceCount = hasTertiaryResidences.map { yn =>
      if (yn) rawRecord.getRequired("dd_2nd_residence_nbr").toInt else 0
    }
    val tertiaryResidenceCutoff = tertiaryResidenceCount.getOrElse(0)

    val tertiaryResidences = List.tabulate(tertiaryResidenceCutoff) { i =>
      val prefix = f"dd_2nd_residence_${i + 1}%02d"
      val state = rawRecord.getOptional(s"${prefix}_st")
      val weeks = rawRecord.getOptionalNumber(s"${prefix}_time")
      (state, weeks)
    }

    val primaryOwned = rawRecord.getOptionalNumber("oc_address1_own")
    val secondaryOwned = hasSecondaryResidence.flatMap {
      if (_) rawRecord.getOptionalNumber("oc_address2_own") else None
    }

    HlesDogResidences(
      ocPrimaryResidenceState = rawRecord.getOptional("oc_address1_state"),
      ocPrimaryResidenceCensusDivision = rawRecord.getOptional("oc_address1_division").flatMap {
        OwnerTransformations.getCensusDivision(_)
      },
      ocPrimaryResidenceOwnership = primaryOwned,
      ocPrimaryResidenceOwnershipOtherDescription =
        if (primaryOwned.contains(98)) rawRecord.getOptional("oc_address1_own_other") else None,
      ocPrimaryResidenceTimePercentage = hasSecondaryResidence.flatMap {
        if (_) rawRecord.getOptionalNumber("oc_address1_pct") else None
      },
      ocSecondaryResidence = hasSecondaryResidence,
      ocSecondaryResidenceState = hasSecondaryResidence.flatMap {
        if (_) rawRecord.getOptional("oc_address2_state") else None
      },
      ocSecondaryResidenceOwnership = secondaryOwned,
      ocSecondaryResidenceOwnershipOtherDescription = if (secondaryOwned.contains(98)) {
        rawRecord.getOptional("oc_address2_own_other")
      } else {
        None
      },
      ocSecondaryResidenceTimePercentage = hasSecondaryResidence.flatMap {
        if (_) rawRecord.getOptionalNumber("oc_2nd_address_pct") else None
      },
      ddAlternateRecentResidenceCount = tertiaryResidenceCount.map(_.toLong),
      ddAlternateRecentResidence1State =
        if (tertiaryResidenceCutoff >= 1) tertiaryResidences(0)._1 else None,
      ddAlternateRecentResidence1Weeks =
        if (tertiaryResidenceCutoff >= 1) tertiaryResidences(0)._2 else None,
      ddAlternateRecentResidence2State =
        if (tertiaryResidenceCutoff >= 2) tertiaryResidences(1)._1 else None,
      ddAlternateRecentResidence2Weeks =
        if (tertiaryResidenceCutoff >= 2) tertiaryResidences(1)._2 else None,
      ddAlternateRecentResidence3State =
        if (tertiaryResidenceCutoff >= 3) tertiaryResidences(2)._1 else None,
      ddAlternateRecentResidence3Weeks =
        if (tertiaryResidenceCutoff >= 3) tertiaryResidences(2)._2 else None,
      ddAlternateRecentResidence4State =
        if (tertiaryResidenceCutoff >= 4) tertiaryResidences(3)._1 else None,
      ddAlternateRecentResidence4Weeks =
        if (tertiaryResidenceCutoff >= 4) tertiaryResidences(3)._2 else None,
      ddAlternateRecentResidence5State =
        if (tertiaryResidenceCutoff >= 5) tertiaryResidences(4)._1 else None,
      ddAlternateRecentResidence5Weeks =
        if (tertiaryResidenceCutoff >= 5) tertiaryResidences(4)._2 else None,
      ddAlternateRecentResidence6State =
        if (tertiaryResidenceCutoff >= 6) tertiaryResidences(5)._1 else None,
      ddAlternateRecentResidence6Weeks =
        if (tertiaryResidenceCutoff >= 6) tertiaryResidences(5)._2 else None,
      ddAlternateRecentResidence7State =
        if (tertiaryResidenceCutoff >= 7) tertiaryResidences(6)._1 else None,
      ddAlternateRecentResidence7Weeks =
        if (tertiaryResidenceCutoff >= 7) tertiaryResidences(6)._2 else None,
      ddAlternateRecentResidence8State =
        if (tertiaryResidenceCutoff >= 8) tertiaryResidences(7)._1 else None,
      ddAlternateRecentResidence8Weeks =
        if (tertiaryResidenceCutoff >= 8) tertiaryResidences(7)._2 else None,
      ddAlternateRecentResidence9State =
        if (tertiaryResidenceCutoff >= 9) tertiaryResidences(8)._1 else None,
      ddAlternateRecentResidence9Weeks =
        if (tertiaryResidenceCutoff >= 9) tertiaryResidences(8)._2 else None,
      ddAlternateRecentResidence10State =
        if (tertiaryResidenceCutoff >= 10) tertiaryResidences(9)._1 else None,
      ddAlternateRecentResidence10Weeks =
        if (tertiaryResidenceCutoff >= 10) tertiaryResidences(9)._2 else None,
      ddAlternateRecentResidence11State =
        if (tertiaryResidenceCutoff >= 11) tertiaryResidences(10)._1 else None,
      ddAlternateRecentResidence11Weeks =
        if (tertiaryResidenceCutoff >= 11) tertiaryResidences(10)._2 else None,
      ddAlternateRecentResidence12State =
        if (tertiaryResidenceCutoff >= 12) tertiaryResidences(11)._1 else None,
      ddAlternateRecentResidence12Weeks =
        if (tertiaryResidenceCutoff >= 12) tertiaryResidences(11)._2 else None,
      ddAlternateRecentResidence13State =
        if (tertiaryResidenceCutoff >= 13) tertiaryResidences(12)._1 else None,
      ddAlternateRecentResidence13Weeks =
        if (tertiaryResidenceCutoff >= 13) tertiaryResidences(12)._2 else None
    )
  }
}
