package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesHealthCondition

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HealthTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "HealthTransformations"

  private val singleInfectiousDiseaseFields = Map[String, Array[String]](
    "study_id" -> Array("10"),
    "hs_dx_infectious_yn" -> Array("1"),
    "hs_dx_anaplasmosis" -> Array("1"), // anaplasmosis is 0
    "hs_dx_anaplasmosis_month" -> Array("2"),
    "hs_dx_anaplasmosis_year" -> Array("2020"),
    "hs_dx_anaplasmosis_surg" -> Array("3"),
    "hs_dx_anaplasmosis_fu" -> Array("1")
  )

  private val multipleInfectiousDiseaseFields = Map[String, Array[String]](
    "study_id" -> Array("10"),
    "hs_dx_infectious_yn" -> Array("1"),
    // anaplasmosis is 0
    "hs_dx_anaplasmosis" -> Array("1"),
    "hs_dx_anaplasmosis_month" -> Array("2"),
    "hs_dx_anaplasmosis_year" -> Array("2020"),
    "hs_dx_anaplasmosis_surg" -> Array("3"),
    "hs_dx_anaplasmosis_fu" -> Array("1"),
    // plague is 30
    "hs_dx_plague" -> Array("1"),
    "hs_dx_plague_month" -> Array("5"),
    "hs_dx_plague_year" -> Array("2020"),
    "hs_dx_plague_surg" -> Array("4"),
    "hs_dx_plague_fu" -> Array("0"),
    // infect_other is 98
    "hs_dx_infect_other" -> Array("1"),
    "hs_dx_infect_other_spec" -> Array("falafel"),
    "hs_dx_infect_other_month" -> Array("10"),
    "hs_dx_infect_other_year" -> Array("2020"),
    "hs_dx_infect_other_surg" -> Array("1"),
    "hs_dx_infect_other_fu" -> Array("1")
  )

  it should "correctly map infectious disease values when values are defined for a single disease" in {
    val exampleInfectiousDiseaseRecord = RawRecord(id = 1, singleInfectiousDiseaseFields)
    val output = HealthTransformations.mapHealthConditions(exampleInfectiousDiseaseRecord)

    output.foreach { row =>
      row.dogId shouldBe 10
      row.hsConditionType shouldBe 1L
      row.hsCondition shouldBe 0
      row.hsConditionOtherDescription shouldBe None
      row.hsConditionIsCongenital shouldBe false
      row.hsConditionCause shouldBe None
      row.hsConditionCauseOtherDescription shouldBe None
      row.hsDiagnosisYear shouldBe Some(2020)
      row.hsDiagnosisMonth shouldBe Some(2)
      row.hsRequiredSurgeryOrHospitalization shouldBe Some(3)
      row.hsFollowUpOngoing shouldBe Some(true)
    }
  }

  it should "correctly map infectious disease values when values are defined for multiple diseases" in {
    val exampleInfectiousDiseaseRecord = RawRecord(id = 1, multipleInfectiousDiseaseFields)
    val output = HealthTransformations.mapHealthConditions(exampleInfectiousDiseaseRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 10L,
        // 1 for infectious disease
        hsConditionType = 1L,
        // 0 for anaplasmosis
        hsCondition = 0,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 10L,
        // 1 for infectious disease
        hsConditionType = 1L,
        // 30 for plague
        hsCondition = 30,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(5),
        hsRequiredSurgeryOrHospitalization = Some(4),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 10L,
        // 1 for infectious disease
        hsConditionType = 1L,
        // 98 for infect_other
        hsCondition = 98,
        hsConditionOtherDescription = Some("falafel"),
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(10),
        hsRequiredSurgeryOrHospitalization = Some(1),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map health status data when fields are null" in {
    val emptyRecord = RawRecord(1, Map.empty)

    HealthTransformations.mapHealthConditions(emptyRecord) shouldBe List.empty
  }
}
