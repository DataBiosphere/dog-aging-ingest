package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesHealthCondition

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HealthTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "HealthTransformations"
  import HealthTransformations.{conditionTypes, conditions}

  it should "correctly map infectious disease values when values are defined for a single infectious disease" in {
    val singleInfectiousDisease = Map[String, Array[String]](
      "study_id" -> Array("10"),
      "hs_dx_infectious_yn" -> Array("1"),
      "hs_dx_anaplasmosis" -> Array("1"),
      "hs_dx_anaplasmosis_month" -> Array("2"),
      "hs_dx_anaplasmosis_year" -> Array("2020"),
      "hs_dx_anaplasmosis_surg" -> Array("3"),
      "hs_dx_anaplasmosis_fu" -> Array("1")
    )
    val exampleInfectiousDiseaseRecord = RawRecord(id = 1, singleInfectiousDisease)
    val output = HealthTransformations.mapHealthConditions(exampleInfectiousDiseaseRecord)

    output.foreach { row =>
      row.dogId shouldBe 10
      row.hsConditionType shouldBe conditionTypes.apply("infectious")
      row.hsCondition shouldBe conditions.apply("anaplasmosis")
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
    val multipleInfectiousDisease = Map[String, Array[String]](
      "study_id" -> Array("10"),
      "hs_dx_infectious_yn" -> Array("1"),
      "hs_dx_anaplasmosis" -> Array("1"),
      "hs_dx_anaplasmosis_month" -> Array("2"),
      "hs_dx_anaplasmosis_year" -> Array("2020"),
      "hs_dx_anaplasmosis_surg" -> Array("3"),
      "hs_dx_anaplasmosis_fu" -> Array("1"),
      "hs_dx_plague" -> Array("1"),
      "hs_dx_plague_month" -> Array("5"),
      "hs_dx_plague_year" -> Array("2020"),
      "hs_dx_plague_surg" -> Array("4"),
      "hs_dx_plague_fu" -> Array("0"),
      "hs_dx_infect_other" -> Array("1"),
      "hs_dx_infect_other_spec" -> Array("falafel"),
      "hs_dx_infect_other_month" -> Array("10"),
      "hs_dx_infect_other_year" -> Array("2020"),
      "hs_dx_infect_other_surg" -> Array("1"),
      "hs_dx_infect_other_fu" -> Array("1")
    )
    val exampleInfectiousDiseaseRecord = RawRecord(id = 1, multipleInfectiousDisease)
    val output = HealthTransformations.mapHealthConditions(exampleInfectiousDiseaseRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 10L,
        hsConditionType = conditionTypes.apply("infectious"),
        hsCondition = conditions.apply("anaplasmosis"),
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
        hsConditionType = conditionTypes.apply("infectious"),
        hsCondition = conditions.apply("plague"),
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
        hsConditionType = conditionTypes.apply("infectious"),
        hsCondition = conditions.apply("infect_other"),
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
    val singleEyeDisease = Map[String, Array[String]](
      "study_id" -> Array("10"),
      "hs_dx_eye_yn" -> Array("1"),
      "hs_dx_cat" -> Array("1"),
      "hs_dx_cat_month" -> Array("2"),
      "hs_dx_cat_year" -> Array("2020"),
      "hs_dx_cat_surg" -> Array("3"),
      "hs_dx_cat_fu" -> Array("1")
    )
    val exampleEyeDiseaseRecord = RawRecord(id = 1, singleEyeDisease)
    val output = HealthTransformations.mapHealthConditions(exampleEyeDiseaseRecord)

    output.foreach { row =>
      row.dogId shouldBe 10
      row.hsConditionType shouldBe conditionTypes.apply("eye")
      row.hsCondition shouldBe conditions.apply("cat")
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
    val singleEyeDiseaseBlindCase1 = Map[String, Array[String]](
      "study_id" -> Array("10"),
      "hs_dx_eye_yn" -> Array("1"),
      "hs_dx_blind" -> Array("1"),
      "hs_dx_blind_month" -> Array("5"),
      "hs_dx_blind_year" -> Array("2020"),
      "hs_dx_blind_surg" -> Array("4"),
      "hs_dx_blind_fu" -> Array("0"),
      "hs_dx_eye_cause_yn" -> Array("0")
    )
    val exampleEyeDiseaseRecord1 = RawRecord(id = 1, singleEyeDiseaseBlindCase1)
    val output1 = HealthTransformations.mapHealthConditions(exampleEyeDiseaseRecord1)

    output1.foreach { row =>
      row.dogId shouldBe 10
      row.hsConditionType shouldBe conditionTypes.apply("eye")
      row.hsCondition shouldBe conditions.apply("blind")
      row.hsConditionOtherDescription shouldBe None
      row.hsConditionIsCongenital shouldBe false
      row.hsConditionCause shouldBe None
      row.hsConditionCauseOtherDescription shouldBe None
      row.hsDiagnosisYear shouldBe Some(2020)
      row.hsDiagnosisMonth shouldBe Some(5)
      row.hsRequiredSurgeryOrHospitalization shouldBe Some(4)
      row.hsFollowUpOngoing shouldBe Some(false)
    }

    val singleEyeDiseaseBlindCase2 = Map[String, Array[String]](
      "study_id" -> Array("10"),
      "hs_dx_eye_yn" -> Array("1"),
      "hs_dx_blind" -> Array("1"),
      "hs_dx_blind_month" -> Array("5"),
      "hs_dx_blind_year" -> Array("2020"),
      "hs_dx_blind_surg" -> Array("4"),
      "hs_dx_blind_fu" -> Array("0"),
      "hs_dx_eye_cause_yn" -> Array("1"),
      "hs_dx_eye_cause" -> Array("5")
    )
    val exampleEyeDiseaseRecord2 = RawRecord(id = 2, singleEyeDiseaseBlindCase2)
    val output2 = HealthTransformations.mapHealthConditions(exampleEyeDiseaseRecord2)

    output2.foreach { row =>
      row.dogId shouldBe 10
      row.hsConditionType shouldBe conditionTypes.apply("eye")
      row.hsCondition shouldBe conditions.apply("blind")
      row.hsConditionOtherDescription shouldBe None
      row.hsConditionIsCongenital shouldBe false
      row.hsConditionCause shouldBe Some(5)
      row.hsConditionCauseOtherDescription shouldBe None
      row.hsDiagnosisYear shouldBe Some(2020)
      row.hsDiagnosisMonth shouldBe Some(5)
      row.hsRequiredSurgeryOrHospitalization shouldBe Some(4)
      row.hsFollowUpOngoing shouldBe Some(false)
    }

    val singleEyeDiseaseBlindCase3 = Map[String, Array[String]](
      "study_id" -> Array("10"),
      "hs_dx_eye_yn" -> Array("1"),
      "hs_dx_blind" -> Array("1"),
      "hs_dx_blind_month" -> Array("5"),
      "hs_dx_blind_year" -> Array("2020"),
      "hs_dx_blind_surg" -> Array("4"),
      "hs_dx_blind_fu" -> Array("0"),
      "hs_dx_eye_cause_yn" -> Array("1"),
      "hs_dx_eye_cause" -> Array("98"),
      "hs_dx_eye_cause_other" -> Array("hummus")
    )
    val exampleEyeDiseaseRecord3 = RawRecord(id = 2, singleEyeDiseaseBlindCase3)
    val output3 = HealthTransformations.mapHealthConditions(exampleEyeDiseaseRecord3)

    output3.foreach { row =>
      row.dogId shouldBe 10
      row.hsConditionType shouldBe conditionTypes.apply("eye")
      row.hsCondition shouldBe conditions.apply("blind")
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
    val multipleEyeDisease = Map[String, Array[String]](
      "study_id" -> Array("10"),
      "hs_dx_eye_yn" -> Array("1"),
      "hs_dx_cat" -> Array("1"),
      "hs_dx_cat_month" -> Array("2"),
      "hs_dx_cat_year" -> Array("2020"),
      "hs_dx_cat_surg" -> Array("3"),
      "hs_dx_cat_fu" -> Array("1"),
      "hs_dx_blind" -> Array("1"),
      "hs_dx_blind_month" -> Array("5"),
      "hs_dx_blind_year" -> Array("2020"),
      "hs_dx_blind_surg" -> Array("4"),
      "hs_dx_blind_fu" -> Array("0"),
      "hs_dx_eye_cause_yn" -> Array("1"),
      "hs_dx_eye_cause" -> Array("98"),
      "hs_dx_eye_cause_other" -> Array("hummus"),
      "hs_dx_eye_other" -> Array("1"),
      "hs_dx_eye_other_spec" -> Array("falafel"),
      "hs_dx_eye_other_month" -> Array("10"),
      "hs_dx_eye_other_year" -> Array("2020"),
      "hs_dx_eye_other_surg" -> Array("1"),
      "hs_dx_eye_other_fu" -> Array("1")
    )
    val exampleEyeDiseaseRecord = RawRecord(id = 1, multipleEyeDisease)
    val output = HealthTransformations.mapHealthConditions(exampleEyeDiseaseRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 10L,
        hsConditionType = conditionTypes.apply("eye"),
        hsCondition = conditions.apply("cat"),
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
        hsConditionType = conditionTypes.apply("eye"),
        hsCondition = conditions.apply("blind"),
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
        hsConditionType = conditionTypes.apply("eye"),
        hsCondition = conditions.apply("eye_other"),
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

  it should "correctly map congenital eye disorders when values are defined for a single congenital eye disorder" in {
    val singleCongenitalEyeDisorder = Map[String, Array[String]](
      "study_id" -> Array("10"),
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_disorders_yn" -> Array("1"),
      "hs_cg_eye_cat" -> Array("1"),
      "hs_cg_eye_cat_month" -> Array("2"),
      "hs_cg_eye_cat_year" -> Array("2020"),
      "hs_cg_eye_cat_surg" -> Array("3"),
      "hs_cg_eye_cat_fu" -> Array("1")
    )
    val exampleEyeDisorderRecord = RawRecord(id = 1, singleCongenitalEyeDisorder)
    val output = HealthTransformations.mapHealthConditions(exampleEyeDisorderRecord)

    output.foreach { row =>
      row.dogId shouldBe 10
      row.hsConditionType shouldBe conditionTypes.apply("eye")
      row.hsCondition shouldBe conditions.apply("blind")
      row.hsConditionOtherDescription shouldBe None
      row.hsConditionIsCongenital shouldBe true
      row.hsConditionCause shouldBe None
      row.hsConditionCauseOtherDescription shouldBe None
      row.hsDiagnosisYear shouldBe Some(2020)
      row.hsDiagnosisMonth shouldBe Some(2)
      row.hsRequiredSurgeryOrHospitalization shouldBe Some(3)
      row.hsFollowUpOngoing shouldBe Some(true)
    }
  }

  it should "correctly map congenital eye disorders when values are defined for multiple congenital eye disorders" in {
    val multipleCongenitalEyeDisorder = Map[String, Array[String]](
      "study_id" -> Array("10"),
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_eye_disorders_yn" -> Array("1"),
      "hs_cg_eye_cat" -> Array("1"),
      "hs_cg_eye_cat_month" -> Array("2"),
      "hs_cg_eye_cat_year" -> Array("2020"),
      "hs_cg_eye_cat_surg" -> Array("3"),
      "hs_cg_eye_cat_fu" -> Array("1"),
      "hs_cg_eye_other" -> Array("1"),
      "hs_cg_eye_other_spec" -> Array("olives"),
      "hs_cg_eye_other_month" -> Array("2"),
      "hs_cg_eye_other_year" -> Array("2020"),
      "hs_cg_eye_other_surg" -> Array("3"),
      "hs_cg_eye_other_fu" -> Array("0")
    )
    val exampleEyeDisorderRecord = RawRecord(id = 1, multipleCongenitalEyeDisorder)
    val output = HealthTransformations.mapHealthConditions(exampleEyeDisorderRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 10L,
        hsConditionType = conditionTypes.apply("eye"),
        hsCondition = conditions.apply("cat"),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 10L,
        hsConditionType = conditionTypes.apply("eye"),
        hsCondition = conditions.apply("eye_other"),
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map health status data when fields are null" in {
    val emptyRecord = RawRecord(1, Map.empty)

    HealthTransformations.mapHealthConditions(emptyRecord) shouldBe List.empty
  }
}
