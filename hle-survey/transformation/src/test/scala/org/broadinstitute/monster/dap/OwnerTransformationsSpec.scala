package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesOwner
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OwnerTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "OwnerTransformations"

  private val exampleOwnerFields = Map[String, Array[String]](
    "st_owner_id" -> Array("10"),
    "od_age" -> Array("5"),
    "od_education" -> Array("98"),
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
    "oc_address1_division" -> Array("Division 3: East North Central"),
    "oc_address1_zip" -> Array("32837-4949"),
    "oc_address1_own" -> Array("98"),
    "oc_address1_own_other" -> Array("some text"),
    "oc_address2_yn" -> Array("1"),
    "oc_address2_state" -> Array("MA"),
    "oc_address2_zip" -> Array("02222"),
    "oc_address2_own" -> Array("98"),
    "oc_address2_own_other" -> Array("some text")
  )

  it should "correctly map owner values when all values are defined" in {
    val exampleOwnerRecord = RawRecord(id = 1, exampleOwnerFields)
    val output = OwnerTransformations.mapOwner(exampleOwnerRecord)

    output.ownerId shouldBe 10
    // owner demographic info
    output.odAgeRangeYears shouldBe Some(5)
    output.odMaxEducation shouldBe Some(98)
    output.odMaxEducationOtherDescription shouldBe Some("other education")
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
    output.ocPrimaryResidenceCensusDivision shouldBe Some("Division 3: East North Central")
    output.ocPrimaryResidenceZip shouldBe Some("32837-4949")
    output.ocPrimaryResidenceOwnership shouldBe Some(98)
    output.ocPrimaryResidenceOwnershipOtherDescription shouldBe Some("some text")
    output.ocSecondaryResidence shouldBe Some(true)
    output.ocSecondaryResidenceState shouldBe Some("MA")
    output.ocSecondaryResidenceZip shouldBe Some("02222")
    output.ocSecondaryResidenceOwnership shouldBe Some(98)
    output.ocSecondaryResidenceOwnershipOtherDescription shouldBe Some("some text")

  }

  it should "correctly map residence fields when there is no secondary residence" in {
    val exampleOwnerRecord =
      RawRecord(id = 1, exampleOwnerFields + ("oc_address2_yn" -> Array("0")))
    val output = OwnerTransformations.mapOwner(exampleOwnerRecord)

    output.ocSecondaryResidenceState shouldBe None
    output.ocSecondaryResidenceZip shouldBe None
    output.ocSecondaryResidenceOwnership shouldBe None
    output.ocSecondaryResidenceOwnershipOtherDescription shouldBe None
  }

  it should "correctly map owner data when optional fields are null" in {
    val emptyRecord = RawRecord(id = 1, Map[String, Array[String]]("st_owner_id" -> Array("5")))

    OwnerTransformations.mapOwner(emptyRecord) shouldBe HlesOwner.init(ownerId = 5)
  }
}
