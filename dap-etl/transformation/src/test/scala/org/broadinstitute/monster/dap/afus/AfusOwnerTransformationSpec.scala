package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.RawRecord
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.time.LocalDate

class AfusOwnerTransformationSpec extends AnyFlatSpec {
  behavior of "AfusTransformations"

  private val exampleAfusFields = Map[String, Array[String]](
    "st_owner_id" -> Array("12345"),
    "fu_oc_people_household" -> Array("3"),
    "fu_oc_adults_household" -> Array("2"),
    "fu_oc_children_household" -> Array("1"),
    "fu_oc_address_change" -> Array("1"),
    "fu_oc_address1_change_date" -> Array("2021-05-20"),
    "fu_oc_address1_own" -> Array("98"),
    "fu_oc_address1_own_other" -> Array("Other occupancy "),
    "fu_oc_address1_state" -> Array("OH"),
    "fu_oc_address1_division" -> Array("Division 3: East North Central"),
    "fu_oc_address1_pct" -> Array("4"),
    "fu_oc_address2_yn" -> Array("1"),
    "fu_oc_address2_change" -> Array("1"),
    "fu_oc_address2_change_date" -> Array("2021-10-20"),
    "fu_oc_address2_own" -> Array("98"),
    "fu_oc_address2_own_other" -> Array("Live rent-free "),
    "fu_oc_address2_state" -> Array("NJ"),
    "fu_oc_2nd_address_pct" -> Array("2"),
    "fu_od_age" -> Array("3"),
    "fu_od_education" -> Array("8"),
    "fu_od_education_other" -> Array("TCNJ ALUM"),
    "fu_od_income" -> Array("5")
  )

  it should "correctly maps AFUS owner values" in {
    val exampleAfusRecord = RawRecord(id = 1, exampleAfusFields)
    val output = AfusOwnerTransformations.mapAfusOwnerData(exampleAfusRecord).get

    output.ownerId shouldBe 12345L
    output.afusOcHouseholdPersonCount shouldBe Some(3)
    output.afusOcHouseholdAdultCount shouldBe Some(2)
    output.afusOcHouseholdChildCount shouldBe Some(1)
    output.afusOcPrimaryResidenceChange shouldBe Some(true)
    output.afusOcPrimaryResidenceChangeDate shouldBe Some(LocalDate.of(2021, 5, 20))
    output.afusOcPrimaryResidenceOwnership shouldBe Some(98)
    output.afusOcPrimaryResidenceOwnershipOtherDescription shouldBe Some("Other occupancy")
    output.afusOcPrimaryResidenceState shouldBe Some("OH")
    output.afusOcPrimaryResidenceCensusDivision shouldBe Some(3)
    output.afusOcPrimaryResidenceTimePercentage shouldBe Some(4)
    output.afusOcSecondaryResidence shouldBe Some(1)
    output.afusOcSecondaryResidenceChange shouldBe Some(true)
    output.afusOcSecondaryResidenceChangeDate shouldBe Some(LocalDate.of(2021, 10, 20))
    output.afusOcSecondaryResidenceOwnership shouldBe Some(98)
    output.afusOcSecondaryResidenceOwnershipOtherDescription shouldBe Some("Live rent-free")
    output.afusOcSecondaryResidenceState shouldBe Some("NJ")
    output.afusOcSecondaryResidenceTimePercentage shouldBe Some(2)
    output.afusOdAgeRangeYears shouldBe Some(3)
    output.afusOdMaxEducation shouldBe Some(8)
    output.afusOdMaxEducationOtherDescription shouldBe Some("TCNJ ALUM")
    output.afusOdAnnualIncomeRangeUsd shouldBe Some(5)
  }

  it should "not map afus_owner when st_owner_id is missing" in {
    val exampleAfusRecord = RawRecord(id = 1, exampleAfusFields - "st_owner_id")
    val output = AfusOwnerTransformations.mapAfusOwnerData(exampleAfusRecord)
    output shouldBe None
  }
}
