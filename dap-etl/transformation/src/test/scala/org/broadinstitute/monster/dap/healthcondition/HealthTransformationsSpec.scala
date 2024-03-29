package org.broadinstitute.monster.dap.healthcondition

import org.broadinstitute.monster.dap.common.{HealthTransformations, RawRecord}
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesHealthCondition
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HealthTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "HealthTransformations"

  it should "correctly include congential health condition type when no specific condition present " in {
    val conditions = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_eye_disorders_yn" -> Array("1"),
      "hs_cg_skin_disorders_yn" -> Array("1"),
      "hs_cg_resp_disorders_yn" -> Array("1"),
      "hs_cg_resp_st_nares" -> Array("1"),
      "hs_dx_skin_yn" -> Array("1"),
      "hs_dx_infectious_yn" -> Array("1"),
      "hs_dx_anaplasmosis" -> Array("1")
    )
    val exampleRecord = RawRecord(id = 1, conditions)

    val output = HealthTransformations.mapHealthConditions(exampleRecord)

    val truth = List(
      HlesHealthCondition( // hs_cg_eye_disorders_yn
        dogId = 1L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = None,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = None,
        hsDiagnosisMonth = None,
        hsRequiredSurgeryOrHospitalization = None,
        hsFollowUpOngoing = None
      ),
      HlesHealthCondition( // hs_cg_skin_disorders_yn
        dogId = 1L,
        hsConditionType = HealthConditionType.Skin.value,
        hsCondition = None,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = None,
        hsDiagnosisMonth = None,
        hsRequiredSurgeryOrHospitalization = None,
        hsFollowUpOngoing = None
      ),
      HlesHealthCondition( // hs_cg_resp_disorders_yn + hs_dx_anaplasmosis
        dogId = 1L,
        hsConditionType = HealthConditionType.Respiratory.value,
        hsCondition = Some(601L),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = None,
        hsDiagnosisMonth = None,
        hsRequiredSurgeryOrHospitalization = None,
        hsFollowUpOngoing = None
      ),
      HlesHealthCondition( //  hs_dx_skin_yn
        dogId = 1L,
        hsConditionType = HealthConditionType.Skin.value,
        hsCondition = None,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = None,
        hsDiagnosisMonth = None,
        hsRequiredSurgeryOrHospitalization = None,
        hsFollowUpOngoing = None
      ),
      HlesHealthCondition( //  hs_dx_infectious_yn + hs_dx_infectious_yn
        dogId = 1L,
        hsConditionType = HealthConditionType.Infection.value,
        hsCondition = Some(1601L),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = None,
        hsDiagnosisMonth = None,
        hsRequiredSurgeryOrHospitalization = None,
        hsFollowUpOngoing = None
      )
    )
    output should contain theSameElementsAs truth
  }

  it should "correctly include dx health condition type when no specific condition present " in {
    val congentialEyeConditionNoConditionFilled = Map[String, Array[String]](
      "hs_dx_skin_yn" -> Array("1")
    )
    val exampleRecord = RawRecord(id = 1, congentialEyeConditionNoConditionFilled)

    val output = HealthTransformations.mapHealthConditions(exampleRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Skin.value,
        hsCondition = None,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = None,
        hsDiagnosisMonth = None,
        hsRequiredSurgeryOrHospitalization = None,
        hsFollowUpOngoing = None
      )
    )
    output should contain theSameElementsAs truth
  }

  it should "correctly map infectious disease values when values are defined for a single infectious disease" in {
    val singleInfectiousDisease = Map[String, Array[String]](
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Infection.value,
        hsCondition = Some(HealthCondition.Anaplasmosis.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Infection.value,
        hsCondition = Some(HealthCondition.Anaplasmosis.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Infection.value,
        hsCondition = Some(HealthCondition.Plague.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(5),
        hsRequiredSurgeryOrHospitalization = Some(4),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Infection.value,
        hsCondition = Some(HealthCondition.OtherInfection.value),
        hsConditionOtherDescription = Some("falafel"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = Some(HealthCondition.Cataracts.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = Some(HealthCondition.Blindness.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = Some(99),
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(5),
        hsRequiredSurgeryOrHospitalization = Some(4),
        hsFollowUpOngoing = Some(false)
      )
    )

    output1 should contain theSameElementsAs (truth1)

    val singleEyeDiseaseBlindCase2 = Map[String, Array[String]](
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
        dogId = 2L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = Some(HealthCondition.Blindness.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = Some(5),
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(5),
        hsRequiredSurgeryOrHospitalization = Some(4),
        hsFollowUpOngoing = Some(false)
      )
    )

    output2 should contain theSameElementsAs (truth2)

    val singleEyeDiseaseBlindCase3 = Map[String, Array[String]](
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
        dogId = 2L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = Some(HealthCondition.Blindness.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = Some(98),
        hsConditionCauseOtherDescription = Some("hummus"),
        hsNeurologicalConditionVestibularDiseaseType = None,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = Some(HealthCondition.Cataracts.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = Some(HealthCondition.Blindness.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = Some(98),
        hsConditionCauseOtherDescription = Some("hummus"),
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(5),
        hsRequiredSurgeryOrHospitalization = Some(4),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = Some(HealthCondition.OtherEye.value),
        hsConditionOtherDescription = Some("falafel"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = Some(HealthCondition.Cataracts.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = Some(HealthCondition.Cataracts.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Eye.value,
        hsCondition = Some(HealthCondition.OtherEye.value),
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.OtherCongenital.value,
        hsCondition = Some(HealthCondition.OtherCG.value),
        hsConditionOtherDescription = Some("spongebob"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(1),
        hsFollowUpOngoing = Some(false)
      )
    )

    output should contain theSameElementsAs truth
  }

  it should "correctly map congenital ear disorders when values are defined" in {
    val multipleCongenitalEarDisorder = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_ear_disorders_yn" -> Array("1"),
      "hs_cg_ear_deaf" -> Array("1"),
      "hs_cg_ear_deaf_month" -> Array("2"),
      "hs_cg_ear_deaf_year" -> Array("2020"),
      "hs_cg_ear_deaf_surg" -> Array("3"),
      "hs_cg_ear_deaf_fu" -> Array("1"),
      "hs_cg_ear_other" -> Array("1"),
      "hs_cg_ear_other_spec" -> Array("olives"),
      "hs_cg_ear_other_month" -> Array("2"),
      "hs_cg_ear_other_year" -> Array("2020"),
      "hs_cg_ear_other_surg" -> Array("3"),
      "hs_cg_ear_other_fu" -> Array("0")
    )
    val exampleEarDisorderRecord = RawRecord(id = 1, multipleCongenitalEarDisorder)
    val output = HealthTransformations.mapHealthConditions(exampleEarDisorderRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Ear.value,
        hsCondition = Some(HealthCondition.Deafness.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Ear.value,
        hsCondition = Some(HealthCondition.OtherEar.value),
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map ear disease values when values are defined" in {
    val multipleEarDisease = Map[String, Array[String]](
      "hs_dx_ear_yn" -> Array("1"),
      "hs_dx_ear_deaf" -> Array("1"),
      "hs_dx_ear_deaf_month" -> Array("2"),
      "hs_dx_ear_deaf_year" -> Array("2020"),
      "hs_dx_ear_deaf_surg" -> Array("3"),
      "hs_dx_ear_deaf_fu" -> Array("1"),
      "hs_dx_ear_em" -> Array("1"),
      "hs_dx_ear_em_month" -> Array("2"),
      "hs_dx_ear_em_year" -> Array("2020"),
      "hs_dx_ear_em_surg" -> Array("3"),
      "hs_dx_ear_em_fu" -> Array("1"),
      "hs_dx_ear_other" -> Array("1"),
      "hs_dx_ear_other_spec" -> Array("ohno"),
      "hs_dx_ear_other_month" -> Array("2"),
      "hs_dx_ear_other_year" -> Array("2020"),
      "hs_dx_ear_other_surg" -> Array("3"),
      "hs_dx_ear_other_fu" -> Array("1")
    )
    val exampleEarDiseaseRecord = RawRecord(id = 1, multipleEarDisease)
    val output = HealthTransformations.mapHealthConditions(exampleEarDiseaseRecord)
    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Ear.value,
        hsCondition = Some(HealthCondition.Deafness.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Ear.value,
        hsCondition = Some(HealthCondition.EarMites.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Ear.value,
        hsCondition = Some(HealthCondition.OtherEar.value),
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs truth
  }

  it should "correctly map congenital oral disorders when values are defined" in {
    val multipleCongenitalOralDisorder = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_oral_disorders_yn" -> Array("1"),
      "hs_cg_oral_cl_lip" -> Array("1"),
      "hs_cg_oral_cl_lip_month" -> Array("2"),
      "hs_cg_oral_cl_lip_year" -> Array("2020"),
      "hs_cg_oral_cl_lip_surg" -> Array("3"),
      "hs_cg_oral_cl_lip_fu" -> Array("1"),
      "hs_cg_oral_cl_pal" -> Array("1"),
      "hs_cg_oral_cl_pal_month" -> Array("2"),
      "hs_cg_oral_cl_pal_year" -> Array("2020"),
      "hs_cg_oral_cl_pal_surg" -> Array("3"),
      "hs_cg_oral_cl_pal_fu" -> Array("1"),
      "hs_cg_oral_teeth" -> Array("1"),
      "hs_cg_oral_teeth_month" -> Array("2"),
      "hs_cg_oral_teeth_year" -> Array("2020"),
      "hs_cg_oral_teeth_surg" -> Array("3"),
      "hs_cg_oral_teeth_fu" -> Array("1"),
      "hs_cg_oral_other" -> Array("1"),
      "hs_cg_oral_other_spec" -> Array("wisdom teeth"),
      "hs_cg_oral_other_month" -> Array("2"),
      "hs_cg_oral_other_year" -> Array("2020"),
      "hs_cg_oral_other_surg" -> Array("3"),
      "hs_cg_oral_other_fu" -> Array("1")
    )
    val exampleCongenitalOralDisorder = RawRecord(id = 1, multipleCongenitalOralDisorder)
    val output = HealthTransformations.mapHealthConditions(exampleCongenitalOralDisorder)
    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.CleftLip.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.CleftPalate.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.MissingTeeth.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.OtherOral.value),
        hsConditionOtherDescription = Some("wisdom teeth"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs truth
  }

  it should "correctly map oral disease values when values are defined" in {
    val multipleOralDisease = Map[String, Array[String]](
      "hs_dx_oral_yn" -> Array("1"),
      "hs_dx_oral_dc" -> Array("1"),
      "hs_dx_oral_dc_month" -> Array("2"),
      "hs_dx_oral_dc_year" -> Array("2020"),
      "hs_dx_oral_dc_surg" -> Array("3"),
      "hs_dx_oral_dc_fu" -> Array("1"),
      "hs_dx_oral_et" -> Array("1"),
      "hs_dx_oral_et_month" -> Array("2"),
      "hs_dx_oral_et_year" -> Array("2020"),
      "hs_dx_oral_et_surg" -> Array("3"),
      "hs_dx_oral_et_fu" -> Array("1"),
      "hs_dx_oral_ft" -> Array("1"),
      "hs_dx_oral_ft_month" -> Array("2"),
      "hs_dx_oral_ft_year" -> Array("2020"),
      "hs_dx_oral_ft_surg" -> Array("3"),
      "hs_dx_oral_ft_fu" -> Array("1"),
      "hs_dx_oral_ging" -> Array("1"),
      "hs_dx_oral_ging_month" -> Array("2"),
      "hs_dx_oral_ging_year" -> Array("2020"),
      "hs_dx_oral_ging_surg" -> Array("3"),
      "hs_dx_oral_ging_fu" -> Array("1"),
      "hs_dx_oral_mm" -> Array("1"),
      "hs_dx_oral_mm_month" -> Array("2"),
      "hs_dx_oral_mm_year" -> Array("2020"),
      "hs_dx_oral_mm_surg" -> Array("3"),
      "hs_dx_oral_mm_fu" -> Array("1"),
      "hs_dx_oral_of" -> Array("1"),
      "hs_dx_oral_of_month" -> Array("2"),
      "hs_dx_oral_of_year" -> Array("2020"),
      "hs_dx_oral_of_surg" -> Array("3"),
      "hs_dx_oral_of_fu" -> Array("1"),
      "hs_dx_oral_ob" -> Array("1"),
      "hs_dx_oral_ob_month" -> Array("2"),
      "hs_dx_oral_ob_year" -> Array("2020"),
      "hs_dx_oral_ob_surg" -> Array("3"),
      "hs_dx_oral_ob_fu" -> Array("1"),
      "hs_dx_oral_rd" -> Array("1"),
      "hs_dx_oral_rd_month" -> Array("2"),
      "hs_dx_oral_rd_year" -> Array("2020"),
      "hs_dx_oral_rd_surg" -> Array("3"),
      "hs_dx_oral_rd_fu" -> Array("1"),
      "hs_dx_oral_si" -> Array("1"),
      "hs_dx_oral_si_month" -> Array("2"),
      "hs_dx_oral_si_year" -> Array("2020"),
      "hs_dx_oral_si_surg" -> Array("3"),
      "hs_dx_oral_si_fu" -> Array("1"),
      "hs_dx_oral_ub" -> Array("1"),
      "hs_dx_oral_ub_month" -> Array("2"),
      "hs_dx_oral_ub_year" -> Array("2020"),
      "hs_dx_oral_ub_surg" -> Array("3"),
      "hs_dx_oral_ub_fu" -> Array("1"),
      "hs_dx_oral_other" -> Array("1"),
      "hs_dx_oral_other_spec" -> Array("cavities"),
      "hs_dx_oral_other_month" -> Array("2"),
      "hs_dx_oral_other_year" -> Array("2020"),
      "hs_dx_oral_other_surg" -> Array("3"),
      "hs_dx_oral_other_fu" -> Array("1")
    )
    val exampleOralDisease = RawRecord(id = 1, multipleOralDisease)
    val output = HealthTransformations.mapHealthConditions(exampleOralDisease)
    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.DentalCalculus.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.ExtractedTeeth.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.FracturedTeeth.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.Gingivitis.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.MasticatoryMyositis.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.OronasalFistula.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.Overbite.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.RetainedDeciduousTeeth.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.Sialocele.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.Underbite.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Oral.value,
        hsCondition = Some(HealthCondition.OtherOral.value),
        hsConditionOtherDescription = Some("cavities"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs truth
  }

  it should "correctly map trauma values when values are defined" in {
    val multipleTrauma = Map[String, Array[String]](
      "hs_dx_trauma_yn" -> Array("1"),
      "hs_dx_trauma_dogbite" -> Array("1"),
      "hs_dx_trauma_dogbite_month" -> Array("2"),
      "hs_dx_trauma_dogbite_year" -> Array("2020"),
      "hs_dx_trauma_dogbite_surg" -> Array("3"),
      "hs_dx_trauma_dogbite_fu" -> Array("1"),
      "hs_dx_trauma_frac" -> Array("1"),
      "hs_dx_trauma_frac_month" -> Array("2"),
      "hs_dx_trauma_frac_year" -> Array("2019"),
      "hs_dx_trauma_frac_surg" -> Array("2"),
      "hs_dx_trauma_frac_fu" -> Array("1"),
      "hs_dx_trauma_frac_spec" -> Array("1", "3"),
      "hs_dx_trauma_other" -> Array("1"),
      "hs_dx_trauma_other_spec" -> Array("ohno"),
      "hs_dx_trauma_other_month" -> Array("2"),
      "hs_dx_trauma_other_year" -> Array("2020"),
      "hs_dx_trauma_other_surg" -> Array("3"),
      "hs_dx_trauma_other_fu" -> Array("1")
    )
    val exampleTraumaRecord = RawRecord(id = 1, multipleTrauma)
    val output = HealthTransformations.mapHealthConditions(exampleTraumaRecord)
    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Trauma.value,
        hsCondition = Some(HealthCondition.DogBite.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Trauma.value,
        hsCondition = Some(HealthCondition.FractureLongLimb.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2019),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(2),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Trauma.value,
        hsCondition = Some(HealthCondition.FractureSpine.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2019),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(2),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Trauma.value,
        hsCondition = Some(HealthCondition.OtherTrauma.value),
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs truth
  }

  it should "correctly map congenital skin disorders when values are defined" in {
    val multipleCongenitalSkinDisorder = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_skin_disorders_yn" -> Array("1"),
      "hs_cg_skin_dcysts" -> Array("1"),
      "hs_cg_skin_dcysts_month" -> Array("2"),
      "hs_cg_skin_dcysts_year" -> Array("2020"),
      "hs_cg_skin_dcysts_surg" -> Array("3"),
      "hs_cg_skin_dcysts_fu" -> Array("1"),
      "hs_cg_skin_other" -> Array("1"),
      "hs_cg_skin_other_spec" -> Array("olives"),
      "hs_cg_skin_other_month" -> Array("2"),
      "hs_cg_skin_other_year" -> Array("2020"),
      "hs_cg_skin_other_surg" -> Array("3"),
      "hs_cg_skin_other_fu" -> Array("0")
    )
    val exampleSkinDisorderRecord = RawRecord(id = 1, multipleCongenitalSkinDisorder)
    val output = HealthTransformations.mapHealthConditions(exampleSkinDisorderRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Skin.value,
        hsCondition = Some(HealthCondition.DermoidCysts.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Skin.value,
        hsCondition = Some(HealthCondition.OtherSkin.value),
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map skin disease values when values are defined" in {
    val multipleEarDisease = Map[String, Array[String]](
      "hs_dx_skin_yn" -> Array("1"),
      "hs_dx_skin_alo" -> Array("1"),
      "hs_dx_skin_alo_month" -> Array("2"),
      "hs_dx_skin_alo_year" -> Array("2020"),
      "hs_dx_skin_alo_surg" -> Array("3"),
      "hs_dx_skin_alo_fu" -> Array("1"),
      "hs_dx_skin_cd" -> Array("1"),
      "hs_dx_skin_cd_month" -> Array("2"),
      "hs_dx_skin_cd_year" -> Array("2020"),
      "hs_dx_skin_cd_surg" -> Array("3"),
      "hs_dx_skin_cd_fu" -> Array("1"),
      "hs_dx_skin_other" -> Array("1"),
      "hs_dx_skin_other_spec" -> Array("ohno"),
      "hs_dx_skin_other_month" -> Array("2"),
      "hs_dx_skin_other_year" -> Array("2020"),
      "hs_dx_skin_other_surg" -> Array("3"),
      "hs_dx_skin_other_fu" -> Array("1")
    )
    val exampleEarDiseaseRecord = RawRecord(id = 1, multipleEarDisease)
    val output = HealthTransformations.mapHealthConditions(exampleEarDiseaseRecord)
    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Skin.value,
        hsCondition = Some(HealthCondition.Alopecia.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Skin.value,
        hsCondition = Some(HealthCondition.ContactDermatitis.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Skin.value,
        hsCondition = Some(HealthCondition.OtherSkin.value),
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs truth
  }

  it should "correctly map congenital GI disorders when values are defined" in {
    val multipleCongenitalGIDisorder = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_gastro_disorders_yn" -> Array("1"),
      "hs_cg_gastro_megaeso" -> Array("1"),
      "hs_cg_gastro_megaeso_month" -> Array("2"),
      "hs_cg_gastro_megaeso_year" -> Array("2020"),
      "hs_cg_gastro_megaeso_surg" -> Array("3"),
      "hs_cg_gastro_megaeso_fu" -> Array("1"),
      "hs_cg_gastro_other" -> Array("1"),
      "hs_cg_gastro_other_spec" -> Array("olives"),
      "hs_cg_gastro_other_month" -> Array("2"),
      "hs_cg_gastro_other_year" -> Array("2020"),
      "hs_cg_gastro_other_surg" -> Array("3"),
      "hs_cg_gastro_other_fu" -> Array("0")
    )
    val exampleGIDisorderRecord = RawRecord(id = 1, multipleCongenitalGIDisorder)
    val output = HealthTransformations.mapHealthConditions(exampleGIDisorderRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Gastrointestinal.value,
        hsCondition = Some(HealthCondition.Megaesophagus.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Gastrointestinal.value,
        hsCondition = Some(HealthCondition.OtherGI.value),
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map GI disease values when values are defined" in {
    val multipleGIDisease = Map[String, Array[String]](
      "hs_dx_gi_yn" -> Array("1"),
      "hs_dx_gi_meg" -> Array("1"),
      "hs_dx_gi_meg_month" -> Array("2"),
      "hs_dx_gi_meg_year" -> Array("2020"),
      "hs_dx_gi_meg_surg" -> Array("3"),
      "hs_dx_gi_meg_fu" -> Array("1"),
      "hs_dx_gi_hge" -> Array("1"),
      "hs_dx_gi_hge_month" -> Array("2"),
      "hs_dx_gi_hge_year" -> Array("2020"),
      "hs_dx_gi_hge_surg" -> Array("3"),
      "hs_dx_gi_hge_fu" -> Array("1"),
      "hs_dx_gi_other" -> Array("1"),
      "hs_dx_gi_other_spec" -> Array("ohno"),
      "hs_dx_gi_other_month" -> Array("2"),
      "hs_dx_gi_other_year" -> Array("2020"),
      "hs_dx_gi_other_surg" -> Array("3"),
      "hs_dx_gi_other_fu" -> Array("1")
    )
    val exampleGIDiseaseRecord = RawRecord(id = 1, multipleGIDisease)
    val output = HealthTransformations.mapHealthConditions(exampleGIDiseaseRecord)
    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Gastrointestinal.value,
        hsCondition = Some(HealthCondition.Megaesophagus.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Gastrointestinal.value,
        hsCondition = Some(HealthCondition.HGE.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Gastrointestinal.value,
        hsCondition = Some(HealthCondition.OtherGI.value),
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs truth
  }

  it should "correctly map congenital liver disorders when values are defined" in {
    val multipleCongenitalSkinDisorder = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_liver_disorders_yn" -> Array("1"),
      "hs_cg_liver_ps" -> Array("1"),
      "hs_cg_liver_ps_month" -> Array("2"),
      "hs_cg_liver_ps_year" -> Array("2020"),
      "hs_cg_liver_ps_surg" -> Array("3"),
      "hs_cg_liver_ps_fu" -> Array("1"),
      "hs_cg_liver_other" -> Array("1"),
      "hs_cg_liver_other_spec" -> Array("olives"),
      "hs_cg_liver_other_month" -> Array("2"),
      "hs_cg_liver_other_year" -> Array("2020"),
      "hs_cg_liver_other_surg" -> Array("3"),
      "hs_cg_liver_other_fu" -> Array("0")
    )
    val exampleSkinDisorderRecord = RawRecord(id = 1, multipleCongenitalSkinDisorder)
    val output = HealthTransformations.mapHealthConditions(exampleSkinDisorderRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Liver.value,
        hsCondition = Some(HealthCondition.LiverPS.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Liver.value,
        hsCondition = Some(HealthCondition.OtherLiver.value),
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map liver disease values when values are defined" in {
    val multipleEarDisease = Map[String, Array[String]](
      "hs_dx_liver_yn" -> Array("1"),
      "hs_dx_liver_ps" -> Array("1"),
      "hs_dx_liver_ps_month" -> Array("2"),
      "hs_dx_liver_ps_year" -> Array("2020"),
      "hs_dx_liver_ps_surg" -> Array("3"),
      "hs_dx_liver_ps_fu" -> Array("1"),
      "hs_dx_liver_gbm" -> Array("1"),
      "hs_dx_liver_gbm_month" -> Array("2"),
      "hs_dx_liver_gbm_year" -> Array("2020"),
      "hs_dx_liver_gbm_surg" -> Array("3"),
      "hs_dx_liver_gbm_fu" -> Array("1"),
      "hs_dx_liver_other" -> Array("1"),
      "hs_dx_liver_other_spec" -> Array("ohno"),
      "hs_dx_liver_other_month" -> Array("2"),
      "hs_dx_liver_other_year" -> Array("2020"),
      "hs_dx_liver_other_surg" -> Array("3"),
      "hs_dx_liver_other_fu" -> Array("1")
    )
    val exampleEarDiseaseRecord = RawRecord(id = 1, multipleEarDisease)
    val output = HealthTransformations.mapHealthConditions(exampleEarDiseaseRecord)
    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Liver.value,
        hsCondition = Some(HealthCondition.LiverPS.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Liver.value,
        hsCondition = Some(HealthCondition.GBM.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Liver.value,
        hsCondition = Some(HealthCondition.OtherLiver.value),
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs truth
  }

  it should "correctly map congenital respiratory disorders and non-congenital diseases when values are defined" in {
    val multiple = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_resp_disorders_yn" -> Array("1"),
      "hs_cg_resp_st_nares" -> Array("1"),
      "hs_cg_resp_st_nares_month" -> Array("2"),
      "hs_cg_resp_st_nares_year" -> Array("2020"),
      "hs_cg_resp_st_nares_surg" -> Array("3"),
      "hs_cg_resp_st_nares_fu" -> Array("1"),
      "hs_cg_resp_other" -> Array("1"),
      "hs_cg_resp_other_spec" -> Array("olives"),
      "hs_cg_resp_other_month" -> Array("2"),
      "hs_cg_resp_other_year" -> Array("2020"),
      "hs_cg_resp_other_surg" -> Array("3"),
      "hs_cg_resp_other_fu" -> Array("0"),
      "hs_dx_respire_yn" -> Array("1"),
      "hs_dx_respire_ards" -> Array("1"),
      "hs_dx_respire_ards_month" -> Array("2"),
      "hs_dx_respire_ards_year" -> Array("2020"),
      "hs_dx_respire_ards_surg" -> Array("3"),
      "hs_dx_respire_ards_fu" -> Array("1"),
      "hs_dx_respire_other" -> Array("1"),
      "hs_dx_respire_other_spec" -> Array("ohno"),
      "hs_dx_respire_other_month" -> Array("2"),
      "hs_dx_respire_other_year" -> Array("2020"),
      "hs_dx_respire_other_surg" -> Array("3"),
      "hs_dx_respire_other_fu" -> Array("1")
    )
    val exampleRecord = RawRecord(id = 1, multiple)
    val output = HealthTransformations.mapHealthConditions(exampleRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Respiratory.value,
        hsCondition = Some(HealthCondition.SNN.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Respiratory.value,
        hsCondition = Some(HealthCondition.OtherRespiratory.value),
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Respiratory.value,
        hsCondition = Some(HealthCondition.ARDS.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Respiratory.value,
        hsCondition = Some(HealthCondition.OtherRespiratory.value),
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map congenital cardiac disorders when values are defined" in {
    val multipleCongenitalCardiacDisorder = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_heart_disorders_yn" -> Array("1"),
      "hs_cg_heart_murmur" -> Array("1"),
      "hs_cg_heart_murmur_month" -> Array("7"),
      "hs_cg_heart_murmur_year" -> Array("2020"),
      "hs_cg_heart_murmur_surg" -> Array("2"),
      "hs_cg_heart_murmur_fu" -> Array("0"),
      "hs_cg_heart_other" -> Array("1"),
      "hs_cg_heart_other_spec" -> Array("waffles"),
      "hs_cg_heart_other_month" -> Array("3"),
      "hs_cg_heart_other_year" -> Array("1998"),
      "hs_cg_heart_other_surg" -> Array("3"),
      "hs_cg_heart_other_fu" -> Array("1")
    )
    val exampleCardiacDisorderRecord = RawRecord(id = 1, multipleCongenitalCardiacDisorder)
    val output = HealthTransformations.mapHealthConditions(exampleCardiacDisorderRecord)
    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Cardiac.value,
        hsCondition = Some(HealthCondition.Murmur.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(7),
        hsRequiredSurgeryOrHospitalization = Some(2),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Cardiac.value,
        hsCondition = Some(HealthCondition.OtherCardiac.value),
        hsConditionOtherDescription = Some("waffles"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(1998),
        hsDiagnosisMonth = Some(3),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs truth
  }

  it should "correctly map cardiac disease values when values are defined" in {
    val multipleCardiacDisease = Map[String, Array[String]](
      "hs_dx_cardiac_yn" -> Array("1"),
      "hs_dx_cardiac_mur" -> Array("1"),
      "hs_dx_cardiac_mur_month" -> Array("1"),
      "hs_dx_cardiac_mur_year" -> Array("2019"),
      "hs_dx_cardiac_mur_surg" -> Array("1"),
      "hs_dx_cardiac_mur_fu" -> Array("1"),
      "hs_dx_cardiac_vd" -> Array("1"),
      "hs_dx_cardiac_vd_month" -> Array("2"),
      "hs_dx_cardiac_vd_year" -> Array("2019"),
      "hs_dx_cardiac_vd_surg" -> Array("2"),
      "hs_dx_cardiac_vd_fu" -> Array("1"),
      "hs_dx_cardiac_valve" -> Array("pesto"),
      "hs_dx_cardiac_other" -> Array("1"),
      "hs_dx_cardiac_other_spec" -> Array("zucchini"),
      "hs_dx_cardiac_other_month" -> Array("3"),
      "hs_dx_cardiac_other_year" -> Array("2019"),
      "hs_dx_cardiac_other_surg" -> Array("3"),
      "hs_dx_cardiac_other_fu" -> Array("0")
    )
    val exampleCardiacDiseaseRecord = RawRecord(id = 1, multipleCardiacDisease)
    val output = HealthTransformations.mapHealthConditions(exampleCardiacDiseaseRecord)
    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Cardiac.value,
        hsCondition = Some(HealthCondition.Murmur.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2019),
        hsDiagnosisMonth = Some(1),
        hsRequiredSurgeryOrHospitalization = Some(1),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Cardiac.value,
        hsCondition = Some(HealthCondition.ValveDisease.value),
        hsConditionOtherDescription = Some("pesto"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2019),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(2),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Cardiac.value,
        hsCondition = Some(HealthCondition.OtherCardiac.value),
        hsConditionOtherDescription = Some("zucchini"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2019),
        hsDiagnosisMonth = Some(3),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      )
    )

    output should contain theSameElementsAs truth
  }

  it should "correctly map congenital kidney disorders and non-congenital diseases when values are defined" in {
    val multiple = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_kidney_disorders_yn" -> Array("1"),
      "hs_cg_kidney_eu" -> Array("1"),
      "hs_cg_kidney_eu_month" -> Array("2"),
      "hs_cg_kidney_eu_year" -> Array("2020"),
      "hs_cg_kidney_eu_surg" -> Array("3"),
      "hs_cg_kidney_eu_fu" -> Array("1"),
      "hs_cg_kidney_other" -> Array("1"),
      "hs_cg_kidney_other_spec" -> Array("olives"),
      "hs_cg_kidney_other_month" -> Array("2"),
      "hs_cg_kidney_other_year" -> Array("2020"),
      "hs_cg_kidney_other_surg" -> Array("3"),
      "hs_cg_kidney_other_fu" -> Array("0"),
      "hs_dx_kidney_yn" -> Array("1"),
      "hs_dx_kidney_akf" -> Array("1"),
      "hs_dx_kidney_akf_month" -> Array("2"),
      "hs_dx_kidney_akf_year" -> Array("2020"),
      "hs_dx_kidney_akf_surg" -> Array("3"),
      "hs_dx_kidney_akf_fu" -> Array("1"),
      "hs_dx_kidney_other" -> Array("1"),
      "hs_dx_kidney_other_spec" -> Array("ohno"),
      "hs_dx_kidney_other_month" -> Array("2"),
      "hs_dx_kidney_other_year" -> Array("2020"),
      "hs_dx_kidney_other_surg" -> Array("3"),
      "hs_dx_kidney_other_fu" -> Array("1")
    )
    val exampleRecord = RawRecord(id = 1, multiple)
    val output = HealthTransformations.mapHealthConditions(exampleRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Kidney.value,
        hsCondition = Some(HealthCondition.EctopicUreter.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Kidney.value,
        hsCondition = Some(HealthCondition.OtherKidney.value),
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Kidney.value,
        hsCondition = Some(HealthCondition.AKF.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Kidney.value,
        hsCondition = Some(HealthCondition.OtherKidney.value),
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map the kidney disease of urinary incontinence when the cause is not known" in {
    val specialCase = Map[String, Array[String]](
      "hs_dx_kidney_yn" -> Array("1"),
      "hs_dx_kidney_ui" -> Array("1"),
      "hs_dx_kidney_ui_month" -> Array("2"),
      "hs_dx_kidney_ui_year" -> Array("2020"),
      "hs_dx_kidney_ui_surg" -> Array("3"),
      "hs_dx_kidney_ui_fu" -> Array("1"),
      "hs_dx_kidney_ui_fu_cause" -> Array("0")
    )
    val exampleRecord = RawRecord(id = 1, specialCase)
    val output = HealthTransformations.mapHealthConditions(exampleRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Kidney.value,
        hsCondition = Some(HealthCondition.UrinaryIncontinence.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )
    output should contain theSameElementsAs (truth)
  }

  it should "correctly map the kidney disease of urinary incontinence when the cause is known" in {
    val specialCase = Map[String, Array[String]](
      "hs_dx_kidney_yn" -> Array("1"),
      "hs_dx_kidney_ui" -> Array("1"),
      "hs_dx_kidney_ui_month" -> Array("2"),
      "hs_dx_kidney_ui_year" -> Array("2020"),
      "hs_dx_kidney_ui_surg" -> Array("3"),
      "hs_dx_kidney_ui_fu" -> Array("1"),
      "hs_dx_kidney_ui_fu_cause" -> Array("1"),
      "hs_dx_kidney_ui_fu_why" -> Array("funfunfun")
    )
    val exampleRecord = RawRecord(id = 1, specialCase)
    val output = HealthTransformations.mapHealthConditions(exampleRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Kidney.value,
        hsCondition = Some(HealthCondition.UrinaryIncontinence.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = Some("funfunfun"),
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )
    output should contain theSameElementsAs (truth)
  }

  it should "correctly map congenital hematopoietic disorders and non-congenital diseases when values are defined" in {
    // cg: cd + other, dx: ane + other
    val multiple = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_blood_disorders_yn" -> Array("1"),
      "hs_cg_blood_cd" -> Array("1"),
      "hs_cg_blood_cd_month" -> Array("2"),
      "hs_cg_blood_cd_year" -> Array("2020"),
      "hs_cg_blood_cd_surg" -> Array("3"),
      "hs_cg_blood_cd_fu" -> Array("1"),
      "hs_cg_blood_other" -> Array("1"),
      "hs_cg_blood_other_spec" -> Array("potatoes"),
      "hs_cg_blood_other_month" -> Array("2"),
      "hs_cg_blood_other_year" -> Array("2020"),
      "hs_cg_blood_other_surg" -> Array("3"),
      "hs_cg_blood_other_fu" -> Array("0"),
      "hs_dx_hema_yn" -> Array("1"),
      "hs_dx_hema_ane" -> Array("1"),
      "hs_dx_hema_ane_month" -> Array("2"),
      "hs_dx_hema_ane_year" -> Array("2020"),
      "hs_dx_hema_ane_surg" -> Array("3"),
      "hs_dx_hema_ane_fu" -> Array("1"),
      "hs_dx_hema_other" -> Array("1"),
      "hs_dx_hema_other_spec" -> Array("yams"),
      "hs_dx_hema_other_month" -> Array("2"),
      "hs_dx_hema_other_year" -> Array("2020"),
      "hs_dx_hema_other_surg" -> Array("3"),
      "hs_dx_hema_other_fu" -> Array("1")
    )
    val exampleRecord = RawRecord(id = 1, multiple)
    val output = HealthTransformations.mapHealthConditions(exampleRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Hematopoietic.value,
        hsCondition = Some(HealthCondition.Dyserythropoiesis.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Hematopoietic.value,
        hsCondition = Some(HealthCondition.OtherHematopoietic.value),
        hsConditionOtherDescription = Some("potatoes"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Hematopoietic.value,
        hsCondition = Some(HealthCondition.Anemia.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Hematopoietic.value,
        hsCondition = Some(HealthCondition.OtherHematopoietic.value),
        hsConditionOtherDescription = Some("yams"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map congenital reproductive disorders and non-congenital diseases when values are defined" in {
    val multiple = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_repro_disorders_yn" -> Array("1"),
      "hs_cg_repro_crypto" -> Array("1"),
      "hs_cg_repro_crypto_month" -> Array("2"),
      "hs_cg_repro_crypto_year" -> Array("2020"),
      "hs_cg_repro_crypto_surg" -> Array("3"),
      "hs_cg_repro_crypto_fu" -> Array("1"),
      "hs_cg_repro_other" -> Array("1"),
      "hs_cg_repro_other_spec" -> Array("olives"),
      "hs_cg_repro_other_month" -> Array("2"),
      "hs_cg_repro_other_year" -> Array("2020"),
      "hs_cg_repro_other_surg" -> Array("3"),
      "hs_cg_repro_other_fu" -> Array("0"),
      "hs_dx_repro_yn" -> Array("1"),
      "hs_dx_repro_bph" -> Array("1"),
      "hs_dx_repro_bph_month" -> Array("2"),
      "hs_dx_repro_bph_year" -> Array("2020"),
      "hs_dx_repro_bph_surg" -> Array("3"),
      "hs_dx_repro_bph_fu" -> Array("1"),
      "hs_dx_repro_other" -> Array("1"),
      "hs_dx_repro_other_spec" -> Array("ohno"),
      "hs_dx_repro_other_month" -> Array("2"),
      "hs_dx_repro_other_year" -> Array("2020"),
      "hs_dx_repro_other_surg" -> Array("3"),
      "hs_dx_repro_other_fu" -> Array("1")
    )
    val exampleRecord = RawRecord(id = 1, multiple)
    val output = HealthTransformations.mapHealthConditions(exampleRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Reproductive.value,
        hsCondition = Some(HealthCondition.Cryptorchid.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Reproductive.value,
        hsCondition = Some(HealthCondition.OtherReproductive.value),
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Reproductive.value,
        hsCondition = Some(HealthCondition.BPH.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Reproductive.value,
        hsCondition = Some(HealthCondition.OtherReproductive.value),
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map toxin consumption when values are defined" in {
    val multiple = Map[String, Array[String]](
      "hs_dx_tox_yn" -> Array("1"),
      "hs_dx_tox_rx_human" -> Array("1"),
      "hs_dx_tox_rx_human_spec" -> Array("mentos"),
      "hs_dx_tox_rx_human_month" -> Array("2"),
      "hs_dx_tox_rx_human_year" -> Array("2020"),
      "hs_dx_tox_rx_human_surg" -> Array("3"),
      "hs_dx_tox_rx_human_fu" -> Array("1"),
      "hs_dx_tox_rec" -> Array("1"),
      "hs_dx_tox_rec_spec" -> Array("oregano"),
      "hs_dx_tox_rec_month" -> Array("2"),
      "hs_dx_tox_rec_year" -> Array("2020"),
      "hs_dx_tox_rec_surg" -> Array("3"),
      "hs_dx_tox_rec_fu" -> Array("1"),
      "hs_dx_tox_other" -> Array("1"),
      "hs_dx_tox_other_spec" -> Array("other tasty looking things"),
      "hs_dx_tox_other_month" -> Array("2"),
      "hs_dx_tox_other_year" -> Array("2020"),
      "hs_dx_tox_other_surg" -> Array("3"),
      "hs_dx_tox_other_fu" -> Array("1")
    )
    val exampleRecord = RawRecord(id = 1, multiple)
    val output = HealthTransformations.mapHealthConditions(exampleRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.ToxinConsumption.value,
        hsCondition = Some(HealthCondition.HumanMedications.value),
        hsConditionOtherDescription = Some("mentos"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.ToxinConsumption.value,
        hsCondition = Some(HealthCondition.RecreationalDrugs.value),
        hsConditionOtherDescription = Some("oregano"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.ToxinConsumption.value,
        hsCondition = Some(HealthCondition.OtherToxin.value),
        hsConditionOtherDescription = Some("other tasty looking things"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map congenital orthopedic disorders and non-congenital diseases when values are defined" in {
    val multiple = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_bones_disorders_yn" -> Array("1"),
      "hs_cg_bones_limb" -> Array("1"),
      "hs_cg_bones_limb_month" -> Array("2"),
      "hs_cg_bones_limb_year" -> Array("2020"),
      "hs_cg_bones_limb_surg" -> Array("3"),
      "hs_cg_bones_limb_fu" -> Array("1"),
      "hs_cg_bones_other" -> Array("1"),
      "hs_cg_bones_other_spec" -> Array("olives"),
      "hs_cg_bones_other_month" -> Array("2"),
      "hs_cg_bones_other_year" -> Array("2020"),
      "hs_cg_bones_other_surg" -> Array("3"),
      "hs_cg_bones_other_fu" -> Array("0"),
      "hs_dx_ortho_yn" -> Array("1"),
      "hs_dx_ortho_css" -> Array("1"),
      "hs_dx_ortho_css_month" -> Array("2"),
      "hs_dx_ortho_css_year" -> Array("2020"),
      "hs_dx_ortho_css_surg" -> Array("3"),
      "hs_dx_ortho_css_fu" -> Array("1"),
      "hs_dx_ortho_other" -> Array("1"),
      "hs_dx_ortho_other_spec" -> Array("ohno"),
      "hs_dx_ortho_other_month" -> Array("2"),
      "hs_dx_ortho_other_year" -> Array("2020"),
      "hs_dx_ortho_other_surg" -> Array("3"),
      "hs_dx_ortho_other_fu" -> Array("1")
    )
    val exampleRecord = RawRecord(id = 1, multiple)
    val output = HealthTransformations.mapHealthConditions(exampleRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Orthopedic.value,
        hsCondition = Some(HealthCondition.MissingLimb.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Orthopedic.value,
        hsCondition = Some(HealthCondition.OtherOrthopedic.value),
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Orthopedic.value,
        hsCondition = Some(HealthCondition.CSS.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Orthopedic.value,
        hsCondition = Some(HealthCondition.OtherOrthopedic.value),
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map congenital neurologic disorders and non-congenital diseases when values are defined" in {
    val multiple = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_brain_disorders_yn" -> Array("1"),
      "hs_cg_brain_cereb" -> Array("1"),
      "hs_cg_brain_cereb_month" -> Array("2"),
      "hs_cg_brain_cereb_year" -> Array("2020"),
      "hs_cg_brain_cereb_surg" -> Array("3"),
      "hs_cg_brain_cereb_fu" -> Array("1"),
      "hs_cg_brain_other" -> Array("1"),
      "hs_cg_brain_other_spec" -> Array("olives"),
      "hs_cg_brain_other_month" -> Array("2"),
      "hs_cg_brain_other_year" -> Array("2020"),
      "hs_cg_brain_other_surg" -> Array("3"),
      "hs_cg_brain_other_fu" -> Array("0"),
      "hs_dx_neuro_yn" -> Array("1"),
      "hs_dx_neuro_vd" -> Array("1"),
      "hs_dx_neuro_vd_month" -> Array("2"),
      "hs_dx_neuro_vd_year" -> Array("2020"),
      "hs_dx_neuro_vd_surg" -> Array("3"),
      "hs_dx_neuro_vd_fu" -> Array("1"),
      "hs_dx_neuro_vd_type" -> Array("2"),
      "hs_dx_neuro_other" -> Array("1"),
      "hs_dx_neuro_other_spec" -> Array("ohno"),
      "hs_dx_neuro_other_month" -> Array("2"),
      "hs_dx_neuro_other_year" -> Array("2020"),
      "hs_dx_neuro_other_surg" -> Array("3"),
      "hs_dx_neuro_other_fu" -> Array("1")
    )
    val exampleRecord = RawRecord(id = 1, multiple)
    val output = HealthTransformations.mapHealthConditions(exampleRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Neurologic.value,
        hsCondition = Some(HealthCondition.CerebellarHypoplasia.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Neurologic.value,
        hsCondition = Some(HealthCondition.OtherNeurologic.value),
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Neurologic.value,
        hsCondition = Some(HealthCondition.VestibularDisease.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = Some(2),
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Neurologic.value,
        hsCondition = Some(HealthCondition.OtherNeurologic.value),
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map congenital endocrine disorders and non-congenital diseases when values are defined" in {
    val multiple = Map[String, Array[String]](
      "hs_congenital_yn" -> Array("1"),
      "hs_cg_endocr_disorders_yn" -> Array("1"),
      "hs_cg_endocr_ch" -> Array("1"),
      "hs_cg_endocr_ch_month" -> Array("2"),
      "hs_cg_endocr_ch_year" -> Array("2020"),
      "hs_cg_endocr_ch_surg" -> Array("3"),
      "hs_cg_endocr_ch_fu" -> Array("1"),
      "hs_cg_endocr_other" -> Array("1"),
      "hs_cg_endocr_other_spec" -> Array("olives"),
      "hs_cg_endocr_other_month" -> Array("2"),
      "hs_cg_endocr_other_year" -> Array("2020"),
      "hs_cg_endocr_other_surg" -> Array("3"),
      "hs_cg_endocr_other_fu" -> Array("0"),
      "hs_dx_endo_yn" -> Array("1"),
      "hs_dx_endo_ad" -> Array("1"),
      "hs_dx_endo_ad_month" -> Array("2"),
      "hs_dx_endo_ad_year" -> Array("2020"),
      "hs_dx_endo_ad_surg" -> Array("3"),
      "hs_dx_endo_ad_fu" -> Array("1"),
      "hs_dx_endo_other" -> Array("1"),
      "hs_dx_endo_other_spec" -> Array("ohno"),
      "hs_dx_endo_other_month" -> Array("2"),
      "hs_dx_endo_other_year" -> Array("2020"),
      "hs_dx_endo_other_surg" -> Array("3"),
      "hs_dx_endo_other_fu" -> Array("1")
    )
    val exampleRecord = RawRecord(id = 1, multiple)
    val output = HealthTransformations.mapHealthConditions(exampleRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Endocrine.value,
        hsCondition = Some(HealthCondition.CongenitalHypothyroidism.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Endocrine.value,
        hsCondition = Some(HealthCondition.OtherEndocrine.value),
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Endocrine.value,
        hsCondition = Some(HealthCondition.AddisonsDisease.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Endocrine.value,
        hsCondition = Some(HealthCondition.OtherEndocrine.value),
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs (truth)
  }

  it should "correctly map immune diseases when values are defined" in {
    val multiple = Map[String, Array[String]](
      "hs_dx_immune_yn" -> Array("1"),
      "hs_dx_immune_at" -> Array("1"),
      "hs_dx_immune_at_month" -> Array("2"),
      "hs_dx_immune_at_year" -> Array("2020"),
      "hs_dx_immune_at_surg" -> Array("3"),
      "hs_dx_immune_at_fu" -> Array("1"),
      "hs_dx_immune_other" -> Array("1"),
      "hs_dx_immune_other_spec" -> Array("sriracha"),
      "hs_dx_immune_other_month" -> Array("2"),
      "hs_dx_immune_other_year" -> Array("2020"),
      "hs_dx_immune_other_surg" -> Array("3"),
      "hs_dx_immune_other_fu" -> Array("1")
    )
    val exampleRecord = RawRecord(id = 1, multiple)
    val output = HealthTransformations.mapHealthConditions(exampleRecord)

    val truth = List(
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Immune.value,
        hsCondition = Some(HealthCondition.AutoimmuneThyroiditis.value),
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Immune.value,
        hsCondition = Some(HealthCondition.OtherImmune.value),
        hsConditionOtherDescription = Some("sriracha"),
        hsConditionIsCongenital = false,
        hsEyeConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsNeurologicalConditionVestibularDiseaseType = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      )
    )

    output should contain theSameElementsAs truth
  }

  it should "correctly map health status data when fields are null" in {
    val emptyRecord = RawRecord(1, Map.empty)

    HealthTransformations.mapHealthConditions(emptyRecord) shouldBe List.empty
  }
}
