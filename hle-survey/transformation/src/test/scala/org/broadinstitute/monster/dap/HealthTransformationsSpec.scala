package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dap.healthcondition.{HealthCondition, HealthConditionType}
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesHealthCondition
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HealthTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "HealthTransformations"

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
    val truth = List(
      HlesHealthCondition(
        dogId = 10L,
        hsConditionType = HealthConditionType.Infection.value,
        hsCondition = HealthCondition.Anaplasmosis.value,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs truth
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
        hsConditionType = HealthConditionType.Infection.value,
        hsCondition = HealthCondition.Anaplasmosis.value,
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
        hsConditionType = HealthConditionType.Infection.value,
        hsCondition = HealthCondition.Plague.value,
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
        hsConditionType = HealthConditionType.Infection.value,
        hsCondition = HealthCondition.OtherInfection.value,
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
    val truth = List(
      HlesHealthCondition(
        dogId = 10L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = HealthCondition.Cataracts.value,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs truth
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
    val truth1 = List(
      HlesHealthCondition(
        dogId = 10L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = HealthCondition.Blindness.value,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(5),
        hsRequiredSurgeryOrHospitalization = Some(4),
        hsFollowUpOngoing = Some(false)
      )
    )

    output1 should contain theSameElementsAs (truth1)

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
    val truth2 = List(
      HlesHealthCondition(
        dogId = 10L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = HealthCondition.Blindness.value,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsConditionCause = Some(5),
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(5),
        hsRequiredSurgeryOrHospitalization = Some(4),
        hsFollowUpOngoing = Some(false)
      )
    )

    output2 should contain theSameElementsAs (truth2)

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
    val truth3 = List(
      HlesHealthCondition(
        dogId = 10L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = HealthCondition.Blindness.value,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsConditionCause = Some(98),
        hsConditionCauseOtherDescription = Some("hummus"),
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(5),
        hsRequiredSurgeryOrHospitalization = Some(4),
        hsFollowUpOngoing = Some(false)
      )
    )

    output3 should contain theSameElementsAs (truth3)
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
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = HealthCondition.Cataracts.value,
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
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = HealthCondition.Blindness.value,
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
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = HealthCondition.OtherEye.value,
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
      "hs_cg_eye_disorders_yn" -> Array("1"),
      "hs_cg_eye_cat" -> Array("1"),
      "hs_cg_eye_cat_month" -> Array("2"),
      "hs_cg_eye_cat_year" -> Array("2020"),
      "hs_cg_eye_cat_surg" -> Array("3"),
      "hs_cg_eye_cat_fu" -> Array("1")
    )
    val exampleEyeDisorderRecord = RawRecord(id = 1, singleCongenitalEyeDisorder)
    val output = HealthTransformations.mapHealthConditions(exampleEyeDisorderRecord)
    val truth = List(
      HlesHealthCondition(
        dogId = 10L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = HealthCondition.Cataracts.value,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs truth
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
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = HealthCondition.Cataracts.value,
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
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = HealthCondition.OtherEye.value,
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

  it should "correctly map other congenital disorders when values are defined for it" in {
    val singleCongenitalOtherDisorder = Map[String, Array[String]](
      "study_id" -> Array("10"),
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_other_yn" -> Array("1"),
      "hs_cg_other_spec" -> Array("spongebob"),
      "hs_cg_other_month" -> Array("2"),
      "hs_cg_other_year" -> Array("2020"),
      "hs_cg_other_surg" -> Array("1"),
      "hs_cg_other_fu" -> Array("0")
    )
    val exampleOtherDisorderRecord = RawRecord(id = 1, singleCongenitalOtherDisorder)
    val output = HealthTransformations.mapHealthConditions(exampleOtherDisorderRecord)
    val truth = List(
      HlesHealthCondition(
        dogId = 10L,
        hsConditionType = HealthConditionType.OtherCongenital.value,
        hsCondition = HealthCondition.OtherCG.value,
        hsConditionOtherDescription = Some("spongebob"),
        hsConditionIsCongenital = true,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(1),
        hsFollowUpOngoing = Some(false)
      )
    )

    output should contain theSameElementsAs truth
  }

  it should "correctly map health status data when fields are null" in {
    val emptyRecord = RawRecord(1, Map.empty)

    HealthTransformations.mapHealthConditions(emptyRecord) shouldBe List.empty
  }
}
