package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesCancerCondition
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CancerTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "CancerTransformations"

  private val exampleCancerFields = Map[String, Array[String]](
    "study_id" -> Array("10"),
    "hs_dx_cancer_year" -> Array("2020"),
    "hs_dx_cancer_mo" -> Array("10"),
    "hs_dx_cancer_surg" -> Array("2"),
    "hs_dx_cancer_fu" -> Array("1"),
    "hs_dx_cancer_loc" -> Array("1", "4", "98"),
    "hs_dx_cancer_loc_other" -> Array("other cancer loc"),
    "hs_dx_cancer_type" -> Array("1", "10", "98"),
    "hs_dx_cancer_type_other" -> Array("other cancer type"),
    "hs_dx_cancer_leuk" -> Array("2", "98"),
    "hs_dx_cancer_leuk_other" -> Array("other leuk"),
    "hs_dx_cancer_lymph" -> Array("3", "98"),
    "hs_dx_cancer_lymph_other" -> Array("other lymph")
  )

  it should "correctly map owner values when all values are defined" in {
    val exampleCancerRecord = RawRecord(id = 1, exampleCancerFields)
    val output = CancerTransformations.mapCancerConditions(exampleCancerRecord)

    output.dogId shouldBe 10
    output.hsInitialDiagnosisYear shouldBe Some(2020)
    output.hsInitialDiagnosisMonth shouldBe Some(10)
    output.hsRequiredSurgeryOrHospitalization shouldBe Some(2)
    output.hsFollowUpOngoing shouldBe Some(true)
    // cancer locations
    output.hsCancerLocationsAdrenalGland shouldBe Some(true)
    output.hsCancerLocationsBlood shouldBe Some(true)
    output.hsCancerLocationsUnknown shouldBe Some(false)
    output.hsCancerLocationsOther shouldBe Some(true)
    output.hsCancerLocationsOtherDescription shouldBe Some("other cancer loc")
    // cancer types
    output.hsCancerTypesAdenoma shouldBe Some(true)
    output.hsCancerTypesHemangioma shouldBe Some(true)
    output.hsCancerTypesUnknown shouldBe Some(false)
    output.hsCancerTypesOther shouldBe Some(true)
    output.hsCancerTypesOtherDescription shouldBe Some("other cancer type")
    // leukemia
    output.hsLeukemiaTypesChronic shouldBe Some(true)
    output.hsLeukemiaTypesUnknown shouldBe Some(false)
    output.hsLeukemiaTypesOther shouldBe Some(true)
    output.hsLeukemiaTypesOtherDescription shouldBe Some("other leuk")
    // lymphoma
    output.hsLymphomaLymphosarcomaTypesTZone shouldBe Some(true)
    output.hsLymphomaLymphosarcomaTypesUnknown shouldBe Some(false)
    output.hsLymphomaLymphosarcomaTypesOther shouldBe Some(true)
    output.hsLymphomaLymphosarcomaTypesOtherDescription shouldBe Some("other lymph")
  }

  it should "correctly map cancer data when optional fields are null" in {
    val emptyRecord = RawRecord(id = 1, Map[String, Array[String]]("study_id" -> Array("5")))

    CancerTransformations.mapCancerConditions(emptyRecord) shouldBe HlesCancerCondition.init(dogId =
      5
    )
  }
}
