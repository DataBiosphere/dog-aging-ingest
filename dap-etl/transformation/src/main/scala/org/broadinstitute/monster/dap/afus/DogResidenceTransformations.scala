package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.AfusDogResidences

object DogResidenceTransformations {

  def mapDogResidences(rawRecord: RawRecord): AfusDogResidences = {
    val otherLocation = rawRecord.getOptionalBoolean("fu_dd_2nd_residence_yn")
    val primaryAddressChange = rawRecord.getOptionalBoolean("fu_oc_address_change")
    val primaryAddressOwnership =
      if (primaryAddressChange.contains(true)) rawRecord.getOptionalNumber("fu_oc_address1_own")
      else None
    val secondaryAddressChange = rawRecord.getOptionalBoolean("fu_oc_address2_change")
    val secondaryAddressOwnership =
      if (secondaryAddressChange.contains(true)) rawRecord.getOptionalNumber("fu_oc_address2_own")
      else None

    AfusDogResidences(
      afusDdAlternateRecentResidenceCount =
        if (otherLocation.contains(true)) rawRecord.getOptionalNumber("fu_dd_2nd_residence_nbr")
        else Some(0),
      afusOcPrimaryResidenceState =
        if (primaryAddressChange.contains(true)) rawRecord.getOptional("fu_oc_address1_state")
        else None,
      afusOcPrimaryResidenceCensusDivision =
        if (primaryAddressChange.contains(true))
          rawRecord.getOptionalNumber("fu_oc_address1_division")
        else None,
      afusOcPrimaryResidenceOwnership = primaryAddressOwnership,
      afusOcPrimaryResidenceOwnershipOtherDescription =
        if (primaryAddressOwnership.contains(98))
          rawRecord.getOptionalStripped("fu_oc_address1_own_other")
        else None,
      afusOcPrimaryResidenceTimePercentage =
        if (primaryAddressChange.contains(true)) rawRecord.getOptionalNumber("fu_oc_address1_pct")
        else None,
      afusOcSecondaryResidence = rawRecord.getOptionalNumber("fu_oc_address2_yn"),
      afusOcSecondaryResidenceState =
        if (secondaryAddressChange.contains(true)) rawRecord.getOptional("fu_oc_address2_state")
        else None,
      afusOcSecondaryResidenceOwnership = secondaryAddressOwnership,
      afusOcSecondaryResidenceOwnershipOtherDescription =
        if (secondaryAddressOwnership.contains(98))
          rawRecord.getOptionalStripped("fu_oc_address2_own_other")
        else None,
      afusOcSecondaryResidenceTimePercentage =
        if (secondaryAddressChange.contains(true))
          rawRecord.getOptionalNumber("fu_oc_address1_own_other")
        else None,
      afusDdAlternateRecentResidence1State = rawRecord.getOptionalNumber("fu_oc_address1_pct"),
      afusDdAlternateRecentResidence1Weeks = rawRecord.getOptionalNumber("fu_oc_address2_yn"),
      afusDdAlternateRecentResidence2State = rawRecord.getOptionalNumber("fu_oc_address2_state"),
      afusDdAlternateRecentResidence2Weeks = rawRecord.getOptionalNumber("fu_oc_address2_own"),
      afusDdAlternateRecentResidence3State =
        rawRecord.getOptionalNumber("fu_oc_address2_own_other"),
      afusDdAlternateRecentResidence3Weeks = rawRecord.getOptionalNumber("fu_oc_2nd_address_pct"),
      afusDdAlternateRecentResidence4State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_01_st"),
      afusDdAlternateRecentResidence4Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_01_time"),
      afusDdAlternateRecentResidence5State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_02_st"),
      afusDdAlternateRecentResidence5Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_02_time"),
      afusDdAlternateRecentResidence6State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_03_st"),
      afusDdAlternateRecentResidence6Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_03_time"),
      afusDdAlternateRecentResidence7State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_04_st"),
      afusDdAlternateRecentResidence7Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_04_time"),
      afusDdAlternateRecentResidence8State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_05_st"),
      afusDdAlternateRecentResidence8Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_05_time"),
      afusDdAlternateRecentResidence9State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_06_st"),
      afusDdAlternateRecentResidence9Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_06_time"),
      afusDdAlternateRecentResidence10State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_07_st"),
      afusDdAlternateRecentResidence10Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_07_time"),
      afusDdAlternateRecentResidence11State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_08_st"),
      afusDdAlternateRecentResidence11Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_08_time"),
      afusDdAlternateRecentResidence12State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_09_st"),
      afusDdAlternateRecentResidence12Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_09_time"),
      afusDdAlternateRecentResidence13State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_10_st"),
      afusDdAlternateRecentResidence13Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_10_time")
    )
  }
}
