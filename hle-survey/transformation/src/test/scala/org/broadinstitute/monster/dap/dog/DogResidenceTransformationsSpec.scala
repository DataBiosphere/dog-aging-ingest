package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DogResidenceTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "DogResidenceTransformations"

  it should "map residence-related fields with one address" in {
    val oneAddress = Map[String, Array[String]](
      "oc_address2_yn" -> Array("0"),
      "dd_2nd_residence_yn" -> Array("0"),
      "oc_address1_state" -> Array("MA"),
      "oc_address1_zip" -> Array("02114"),
      "oc_address1_own" -> Array("98"),
      "oc_address1_own_other" -> Array("Squatter's rights"),
      "oc_address1_pct" -> Array("2")
    )

    val oneAddrOut = DogResidenceTransformations.mapDogResidences(RawRecord(1, oneAddress))
    oneAddrOut.ocPrimaryResidenceState.value shouldBe "MA"
    oneAddrOut.ocPrimaryResidenceZip.value shouldBe "02114"
    oneAddrOut.ocPrimaryResidenceOwnership.value shouldBe 98L
    oneAddrOut.ocPrimaryResidenceOwnershipOtherDescription.value shouldBe "Squatter's rights"
    // Not a typo: Time percentage not carried forward if only 1 address.
    oneAddrOut.ocPrimaryResidenceTimePercentage shouldBe None
    oneAddrOut.ocSecondaryResidence.value shouldBe false
  }

  it should "map residence-related demographics fields" in {
    val manyAddresses = Map[String, Array[String]](
      "oc_address2_yn" -> Array("1"),
      "dd_2nd_residence_yn" -> Array("1"),
      "dd_2nd_residence_nbr" -> Array("2"),
      "oc_address1_state" -> Array("MA"),
      "oc_address1_division" -> Array("Division 3: East North Central"),
      "oc_address1_zip" -> Array("02114"),
      "oc_address1_own" -> Array("1"),
      "oc_address1_pct" -> Array("1"),
      "oc_address2_state" -> Array("MA"),
      "oc_address2_zip" -> Array("02115"),
      "oc_address2_own" -> Array("98"),
      "oc_address2_own_other" -> Array("Foo"),
      "oc_2nd_address_pct" -> Array("3"),
      "dd_2nd_residence_01_st" -> Array("NH"),
      "dd_2nd_residence_01_zip" -> Array("00000"),
      "dd_2nd_residence_01_time" -> Array("1"),
      "dd_2nd_residence_02_st" -> Array("VT"),
      "dd_2nd_residence_02_zip" -> Array("00001"),
      "dd_2nd_residence_02_time" -> Array("2"),
      "dd_2nd_residence_03_st" -> Array("CA"),
      "dd_2nd_residence_03_zip" -> Array("99999"),
      "dd_2nd_residence_03_time" -> Array("3")
    )

    val manyAddrOut = DogResidenceTransformations.mapDogResidences(RawRecord(1, manyAddresses))
    manyAddrOut.ocPrimaryResidenceState.value shouldBe "MA"
    manyAddrOut.ocPrimaryResidenceCensusDivision.value shouldBe 3L
    manyAddrOut.ocPrimaryResidenceZip.value shouldBe "02114"
    manyAddrOut.ocPrimaryResidenceOwnership.value shouldBe 1L
    manyAddrOut.ocPrimaryResidenceTimePercentage.value shouldBe 1L
    manyAddrOut.ocSecondaryResidence.value shouldBe true
    manyAddrOut.ocSecondaryResidenceState.value shouldBe "MA"
    manyAddrOut.ocSecondaryResidenceZip.value shouldBe "02115"
    manyAddrOut.ocSecondaryResidenceOwnership.value shouldBe 98L
    manyAddrOut.ocSecondaryResidenceOwnershipOtherDescription.value shouldBe "Foo"
    manyAddrOut.ocSecondaryResidenceTimePercentage.value shouldBe 3L
    manyAddrOut.ddAlternateRecentResidence1State.value shouldBe "NH"
    manyAddrOut.ddAlternateRecentResidence1Zip.value shouldBe "00000"
    manyAddrOut.ddAlternateRecentResidence1Weeks.value shouldBe 1L
    manyAddrOut.ddAlternateRecentResidence2State.value shouldBe "VT"
    manyAddrOut.ddAlternateRecentResidence2Zip.value shouldBe "00001"
    manyAddrOut.ddAlternateRecentResidence2Weeks.value shouldBe 2L
    // The data says there are only 2 alternate residences, so the data for
    // the 3rd should not carry forward.
    manyAddrOut.ddAlternateRecentResidence3State shouldBe None
    manyAddrOut.ddAlternateRecentResidence3Zip shouldBe None
    manyAddrOut.ddAlternateRecentResidence3Weeks shouldBe None
  }
}
