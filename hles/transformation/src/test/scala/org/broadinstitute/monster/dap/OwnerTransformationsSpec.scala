package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesOwner
import org.scalatest.FailedStatus
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OwnerTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "OwnerTransformations"

  private val exampleOwnerFields = Map[String, Array[String]](
    "st_owner_id" -> Array("10"),
    "od_age" -> Array("5"),
    "od_education" -> Array("2"),
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
    "oc_address1_own" -> Array("98"),
    "oc_address1_own_other" -> Array("some text"),
    "oc_address1_pct" -> Array("1"),
    "oc_address2_yn" -> Array("1"),
    "oc_address2_state" -> Array("MA"),
    "oc_address2_own" -> Array("98"),
    "oc_address2_own_other" -> Array("some text"),
    "oc_2nd_address_pct" -> Array("2")
  )

  it should "correctly map owner values when all values are defined" in {
    val exampleOwnerRecord = RawRecord(id = 1, exampleOwnerFields)
    val output = OwnerTransformations.mapOwner(exampleOwnerRecord)

    val truth = {
      Some(
        HlesOwner(
          ownerId = 10,
          // owner demographic info
          odAgeRangeYears = Some(5),
          odMaxEducation = Some(2),
          odRaceWhite = Some(true),
          odRaceBlackOrAfricanAmerican = Some(false),
          odRaceAsian = Some(false),
          odRaceAmericanIndian = Some(true),
          odRaceAlaskaNative = Some(false),
          odRaceNativeHawaiian = Some(false),
          odRaceOtherPacificIslander = Some(false),
          odRaceOther = Some(true),
          odRaceOtherDescription = Some("some description"),
          odHispanic = Some(true),
          // household info fields
          odAnnualIncomeRangeUsd = Some(2),
          ocHouseholdPersonCount = Some(2),
          ocHouseholdAdultCount = Some(2),
          ocHouseholdChildCount = Some(2),
          ssHouseholdDogCount = Some(2),
          // residence fields
          ocPrimaryResidenceState = Some("OH"),
          ocPrimaryResidenceCensusDivision = Some(3),
          ocPrimaryResidenceOwnership = Some(98),
          ocPrimaryResidenceOwnershipOtherDescription = Some("some text"),
          ocPrimaryResidenceTimePercentage = Some(1),
          ocSecondaryResidence = Some(true),
          ocSecondaryResidenceState = Some("MA"),
          ocSecondaryResidenceOwnership = Some(98),
          ocSecondaryResidenceOwnershipOtherDescription = Some("some text"),
          ocSecondaryResidenceTimePercentage = Some(2)
        )
      )
    }
    output shouldBe truth
  }

  it should "correctly map residence fields when there is no secondary residence" in {
    val exampleOwnerRecord =
      RawRecord(id = 1, exampleOwnerFields + ("oc_address2_yn" -> Array("0")))
    val output = OwnerTransformations.mapOwner(exampleOwnerRecord)

    output match {
      case None => FailedStatus
      case Some(owner) =>
        owner.ocPrimaryResidenceTimePercentage shouldBe None
        owner.ocSecondaryResidenceState shouldBe None
        owner.ocSecondaryResidenceOwnership shouldBe None
        owner.ocSecondaryResidenceOwnershipOtherDescription shouldBe None
    }
  }

  it should "correctly map owner data when optional fields are null" in {
    val emptyRecord = RawRecord(id = 1, Map[String, Array[String]]("st_owner_id" -> Array("5")))

    OwnerTransformations.mapOwner(emptyRecord) shouldBe Some(HlesOwner.init(ownerId = 5))
  }

  it should "should not map owner when st_owner_id is missing" in {
    val emptyRecord = RawRecord(id = 2, Map())

    OwnerTransformations.mapOwner(emptyRecord) shouldBe None
  }
}
