package org.broadinstitute.monster.dap

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OwnerTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "OwnerTransformations"

  private val exampleOwnerFields = Map[String, Array[String]](
    "od_age" -> Array("5"),
    "od_education" -> Array("3"),
    "od_education_other" -> Array("10"),
    "od_race" -> Array("1"),
    "od_race_other" -> Array("1"),
    "od_hispanic_yn" -> Array("0"),
    "od_income" -> Array("10"),
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

  it should "map all owner column values" in {
    val exampleOwnerRecord = RawRecord(id = 1, fields = exampleOwnerFields)
    val actual = OwnerTransformations.mapOwner(exampleOwnerRecord)

    actual.ownerId shouldBe 1
    actual.odAgeRangeYears shouldBe Some("5")
    actual.odMaxEducation shouldBe Some("3")
    actual.odMaxEducationOther shouldBe Some("10")
    actual.odRace.sameElements(Array("1")) shouldBe true
    //actual.odRace shouldBe Array("1")
    actual.odRaceOther shouldBe Some("1")
    actual.odHispanic shouldBe Some(false)
    actual.odAnnualIncomeRangeUsd shouldBe Some("10")
    actual.ocHouseholdPersonCount shouldBe Some("2")
    actual.ocHouseholdAdultCount shouldBe Some("2")
    actual.ocHouseholdChildCount shouldBe Some("2")
    actual.ssHouseholdDogCount shouldBe Some("2")
    actual.ocPrimaryResidenceState shouldBe Some("OH")
    actual.ocPrimaryResidenceCensusDivision shouldBe Some("3")
    actual.ocPrimaryResidenceZip shouldBe Some("01111")
    actual.ocPrimaryResidenceOwnership shouldBe Some("1")
    actual.ocPrimaryResidenceOwnershipOther shouldBe Some("1")
    actual.ocSecondaryResidenceState shouldBe Some("MA")
    actual.ocSecondaryResidenceZip shouldBe Some("02222")
    actual.ocSecondaryResidenceOwnership shouldBe Some("2")
    actual.ocSecondaryResidenceOwnershipOther shouldBe Some("1")

  }

//  it should "return none for secondary address fields" in {
//  }

}
