package org.broadinstitute.monster.dap

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OwnerTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "OwnerTransformations"

  private val exampleOwnerFields = Map[String, Array[String]](
    "od_age" -> Array("5"),
    "od_education" -> Array("3"),
    "od_education_other" -> Array("other education"),
    "od_race" -> Array("1", "4", "98"),
    "od_race_other" -> Array("some description"),
    "od_hispanic_yn" -> Array("1"),
    "od_income" -> Array("2"),
    "oc_people_household" -> Array("2"),
    "oc_adults_household" -> Array("2"),
    "oc_children_household" -> Array("2"),
    "ss_num_dogs_hh" -> Array("2"),
    "oc_address1_state" -> Array("OH"),
    "oc_address1_division" -> Array("3"),
    "oc_address1_zip" -> Array("01111"),
    "oc_address1_own" -> Array("1"),
    "oc_address1_own_other" -> Array("1"),
    "oc_address2_yn" -> Array("1"),
    "oc_address2_state" -> Array("MA"),
    "oc_address2_zip" -> Array("02222"),
    "oc_address2_own" -> Array("2"),
    "oc_address2_own_other" -> Array("1")
  )

  it should "correctly map owner values when all values are defined" in {
    val exampleOwnerRecord = RawRecord(id = 1, exampleOwnerFields)
    val output = OwnerTransformations.mapOwner(exampleOwnerRecord)

    output.ownerId shouldBe 1
    // owner demographic info
    output.odAgeRangeYears shouldBe Some(5)
    output.odMaxEducation shouldBe Some(3)
    output.odMaxEducationOther shouldBe Some("other education")
    output.odRaceWhite shouldBe Some(true)
    output.odRaceBlackOrAfricanAmerican shouldBe Some(false)
    output.odRaceAsian shouldBe Some(false)
    output.odRaceAmericanIndian shouldBe Some(true)
    output.odRaceAlaskaNative shouldBe Some(false)
    output.odRaceNativeHawaiian shouldBe Some(false)
    output.odRaceOtherPacificIslander shouldBe Some(false)
    output.odRaceOther shouldBe Some(true)
    output.odRaceOtherDescription shouldBe Some("some description")
    output.odHispanic shouldBe Some(true)
    // household info fields
    output.odAnnualIncomeRangeUsd shouldBe Some(2)
    output.ocHouseholdPersonCount shouldBe Some(2)
    output.ocHouseholdAdultCount shouldBe Some(2)
    output.ocHouseholdChildCount shouldBe Some(2)
    output.ssHouseholdDogCount shouldBe Some(2)
    // residence fields
    output.ocPrimaryResidenceState shouldBe Some("OH")
    output.ocPrimaryResidenceCensusDivision shouldBe Some(3)
    output.ocPrimaryResidenceZip shouldBe Some(1111)
    output.ocPrimaryResidenceOwnership shouldBe Some("1")
    output.ocPrimaryResidenceOwnershipOther shouldBe Some("1")
    output.ocSecondaryResidenceState shouldBe Some("MA")
    output.ocSecondaryResidenceZip shouldBe Some(2222)
    output.ocSecondaryResidenceOwnership shouldBe Some("2")
    output.ocSecondaryResidenceOwnershipOther shouldBe Some("1")

  }

  it should "correctly map residence fields when there is no secondary residence" in {
    val exampleOwnerRecord =
      RawRecord(id = 1, exampleOwnerFields + ("oc_address2_yn" -> Array("0")))
    val output = OwnerTransformations.mapOwner(exampleOwnerRecord)

    output.ocSecondaryResidenceState shouldBe None
    output.ocSecondaryResidenceZip shouldBe None
    output.ocSecondaryResidenceOwnership shouldBe None
    output.ocSecondaryResidenceOwnershipOther shouldBe None
  }
}
