package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesHealthCondition

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HealthTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "HealthTransformations"

  private val singleInfectiousDisease = Map[String, Array[String]](
    "study_id" -> Array("10"),
    "hs_dx_infectious_yn" -> Array("1"),
    // anaplasmosis is 0
    "hs_dx_anaplasmosis" -> Array("1"),
    "hs_dx_anaplasmosis_month" -> Array("2"),
    "hs_dx_anaplasmosis_year" -> Array("2020"),
    "hs_dx_anaplasmosis_surg" -> Array("3"),
    "hs_dx_anaplasmosis_fu" -> Array("1")
  )

  private val multipleInfectiousDisease = Map[String, Array[String]](
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

  private val singleEyeDisease = Map[String, Array[String]](
    "study_id" -> Array("10"),
    "hs_dx_eye_yn" -> Array("1"),
    // cat is 0
    "hs_dx_cat" -> Array("1"),
    "hs_dx_cat_month" -> Array("2"),
    "hs_dx_cat_year" -> Array("2020"),
    "hs_dx_cat_surg" -> Array("3"),
    "hs_dx_cat_fu" -> Array("1")
  )

  private val singleEyeDiseaseBlindCase1 = Map[String, Array[String]](
    "study_id" -> Array("10"),
    "hs_dx_eye_yn" -> Array("1"),
    // blind is 1 (need to check the special case)
    "hs_dx_blind" -> Array("1"),
    "hs_dx_blind_month" -> Array("5"),
    "hs_dx_blind_year" -> Array("2020"),
    "hs_dx_blind_surg" -> Array("4"),
    "hs_dx_blind_fu" -> Array("0"),
    "hs_dx_eye_cause_yn" -> Array("0")
  )

  private val singleEyeDiseaseBlindCase2 = Map[String, Array[String]](
    "study_id" -> Array("10"),
    "hs_dx_eye_yn" -> Array("1"),
    // blind is 1 (need to check the special case)
    "hs_dx_blind" -> Array("1"),
    "hs_dx_blind_month" -> Array("5"),
    "hs_dx_blind_year" -> Array("2020"),
    "hs_dx_blind_surg" -> Array("4"),
    "hs_dx_blind_fu" -> Array("0"),
    "hs_dx_eye_cause_yn" -> Array("1"),
    "hs_dx_eye_cause" -> Array("5")
  )

  private val singleEyeDiseaseBlindCase3 = Map[String, Array[String]](
    "study_id" -> Array("10"),
    "hs_dx_eye_yn" -> Array("1"),
    // blind is 1 (need to check the special case)
    "hs_dx_blind" -> Array("1"),
    "hs_dx_blind_month" -> Array("5"),
    "hs_dx_blind_year" -> Array("2020"),
    "hs_dx_blind_surg" -> Array("4"),
    "hs_dx_blind_fu" -> Array("0"),
    "hs_dx_eye_cause_yn" -> Array("1"),
    "hs_dx_eye_cause" -> Array("98"),
    "hs_dx_eye_cause_other" -> Array("hummus")
  )

  private val multipleEyeDisease = Map[String, Array[String]](
    "study_id" -> Array("10"),
    "hs_dx_eye_yn" -> Array("1"),
    // cat is 0
    "hs_dx_cat" -> Array("1"),
    "hs_dx_cat_month" -> Array("2"),
    "hs_dx_cat_year" -> Array("2020"),
    "hs_dx_cat_surg" -> Array("3"),
    "hs_dx_cat_fu" -> Array("1"),
    // blind is 1 (need to check the special case)
    "hs_dx_blind" -> Array("1"),
    "hs_dx_blind_month" -> Array("5"),
    "hs_dx_blind_year" -> Array("2020"),
    "hs_dx_blind_surg" -> Array("4"),
    "hs_dx_blind_fu" -> Array("0"),
    "hs_dx_eye_cause_yn" -> Array("1"),
    "hs_dx_eye_cause" -> Array("98"),
    "hs_dx_eye_cause_other" -> Array("hummus"),
    // other is 98
    "hs_dx_eye_other" -> Array("1"),
    "hs_dx_eye_other_spec" -> Array("falafel"),
    "hs_dx_eye_other_month" -> Array("10"),
    "hs_dx_eye_other_year" -> Array("2020"),
    "hs_dx_eye_other_surg" -> Array("1"),
    "hs_dx_eye_other_fu" -> Array("1")
  )

  it should "correctly map infectious disease values when values are defined for a single infectious disease" in {
    val exampleInfectiousDiseaseRecord = RawRecord(id = 1, singleInfectiousDisease)
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

  it should "correctly map infectious disease values when values are defined for multiple infectious diseases" in {
    val exampleInfectiousDiseaseRecord = RawRecord(id = 1, multipleInfectiousDisease)
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
        hsCondition = 30L,
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
        hsCondition = 98L,
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

  it should "correctly map eye disease values when values are defined for a single eye disease" in {
    val exampleEyeDiseaseRecord = RawRecord(id = 1, singleEyeDisease)
    val output = HealthTransformations.mapHealthConditions(exampleEyeDiseaseRecord)

    output.foreach { row =>
      row.dogId shouldBe 10
      row.hsConditionType shouldBe 2L
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

  it should "correctly map eye disease values when values are defined for different cases of blindness" in {
    val exampleEyeDiseaseRecord1 = RawRecord(id = 1, singleEyeDiseaseBlindCase1)
    val output1 = HealthTransformations.mapHealthConditions(exampleEyeDiseaseRecord1)

    output1.foreach { row =>
      row.dogId shouldBe 10
      row.hsConditionType shouldBe 2L
      row.hsCondition shouldBe 1L
      row.hsConditionOtherDescription shouldBe None
      row.hsConditionIsCongenital shouldBe false
      row.hsConditionCause shouldBe None
      row.hsConditionCauseOtherDescription shouldBe None
      row.hsDiagnosisYear shouldBe Some(2020)
      row.hsDiagnosisMonth shouldBe Some(5)
      row.hsRequiredSurgeryOrHospitalization shouldBe Some(4)
      row.hsFollowUpOngoing shouldBe Some(false)
    }

    val exampleEyeDiseaseRecord2 = RawRecord(id = 2, singleEyeDiseaseBlindCase2)
    val output2 = HealthTransformations.mapHealthConditions(exampleEyeDiseaseRecord2)

    output2.foreach { row =>
      row.dogId shouldBe 10
      row.hsConditionType shouldBe 2L
      row.hsCondition shouldBe 1L
      row.hsConditionOtherDescription shouldBe None
      row.hsConditionIsCongenital shouldBe false
      row.hsConditionCause shouldBe Some(5)
      row.hsConditionCauseOtherDescription shouldBe None
      row.hsDiagnosisYear shouldBe Some(2020)
      row.hsDiagnosisMonth shouldBe Some(5)
      row.hsRequiredSurgeryOrHospitalization shouldBe Some(4)
      row.hsFollowUpOngoing shouldBe Some(false)
    }

    val exampleEyeDiseaseRecord3 = RawRecord(id = 2, singleEyeDiseaseBlindCase3)
    val output3 = HealthTransformations.mapHealthConditions(exampleEyeDiseaseRecord3)

    output3.foreach { row =>
      row.dogId shouldBe 10
      row.hsConditionType shouldBe 2L
      row.hsCondition shouldBe 1L
      row.hsConditionOtherDescription shouldBe None
      row.hsConditionIsCongenital shouldBe false
      row.hsConditionCause shouldBe Some(98)
      row.hsConditionCauseOtherDescription shouldBe Some("hummus")
      row.hsDiagnosisYear shouldBe Some(2020)
      row.hsDiagnosisMonth shouldBe Some(5)
      row.hsRequiredSurgeryOrHospitalization shouldBe Some(4)
      row.hsFollowUpOngoing shouldBe Some(false)
    }
  }

  it should "correctly map eye disease values when values are defined for multiple eye diseases" in {
    val exampleEyeDiseaseRecord = RawRecord(id = 1, multipleEyeDisease)
    val output = HealthTransformations.mapHealthConditions(exampleEyeDiseaseRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 10L,
        // 2 for eye disease
        hsConditionType = 2L,
        // 0 for cat
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
        // 2 for eye disease
        hsConditionType = 2L,
        // 1 for blind
        hsCondition = 1L,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsConditionCause = Some(98),
        hsConditionCauseOtherDescription = Some("hummus"),
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(5),
        hsRequiredSurgeryOrHospitalization = Some(4),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 10L,
        // 2 for eye disease
        hsConditionType = 2L,
        // 98 for eye_other
        hsCondition = 98L,
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
