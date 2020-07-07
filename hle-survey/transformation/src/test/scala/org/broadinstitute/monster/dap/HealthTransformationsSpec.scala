package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dap.healthcondition.{HealthCondition, HealthConditionType}
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesHealthCondition
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HealthTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "HealthTransformations"

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
        dogId = 1L,
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
        dogId = 1L,
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
        dogId = 1L,
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
        dogId = 1L,
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
        dogId = 1L,
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
        hsCondition = HealthCondition.Deafness.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Ear.value,
        hsCondition = HealthCondition.OtherEar.value,
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
        hsCondition = HealthCondition.Deafness.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Ear.value,
        hsCondition = HealthCondition.EarMites.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Ear.value,
        hsCondition = HealthCondition.OtherEar.value,
        hsConditionOtherDescription = Some("ohno"),
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

  it should "correctly map trauma values when values are defined" in {
    val multipleTrauma = Map[String, Array[String]](
      "hs_dx_trauma_yn" -> Array("1"),
      "hs_dx_trauma_dogbite" -> Array("1"),
      "hs_dx_trauma_dogbite_month" -> Array("2"),
      "hs_dx_trauma_dogbite_year" -> Array("2020"),
      "hs_dx_trauma_dogbite_surg" -> Array("3"),
      "hs_dx_trauma_dogbite_fu" -> Array("1"),
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
        hsCondition = HealthCondition.DogBite.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Trauma.value,
        hsCondition = HealthCondition.OtherTrauma.value,
        hsConditionOtherDescription = Some("ohno"),
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
        hsCondition = HealthCondition.DermoidCysts.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Skin.value,
        hsCondition = HealthCondition.OtherSkin.value,
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
        hsCondition = HealthCondition.Alopecia.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Skin.value,
        hsCondition = HealthCondition.ContactDermatitis.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Skin.value,
        hsCondition = HealthCondition.OtherSkin.value,
        hsConditionOtherDescription = Some("ohno"),
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
        hsCondition = HealthCondition.Megaesophagus.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Gastrointestinal.value,
        hsCondition = HealthCondition.OtherGI.value,
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
        hsCondition = HealthCondition.Megaesophagus.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Gastrointestinal.value,
        hsCondition = HealthCondition.HGE.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Gastrointestinal.value,
        hsCondition = HealthCondition.OtherGI.value,
        hsConditionOtherDescription = Some("ohno"),
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
        hsCondition = HealthCondition.LiverPS.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Liver.value,
        hsCondition = HealthCondition.OtherLiver.value,
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
        hsCondition = HealthCondition.LiverPS.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Liver.value,
        hsCondition = HealthCondition.GBM.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Liver.value,
        hsCondition = HealthCondition.OtherLiver.value,
        hsConditionOtherDescription = Some("ohno"),
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
        hsCondition = HealthCondition.SNN.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Respiratory.value,
        hsCondition = HealthCondition.OtherRespiratory.value,
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Respiratory.value,
        hsCondition = HealthCondition.ARDS.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Respiratory.value,
        hsCondition = HealthCondition.OtherRespiratory.value,
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
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
        hsCondition = HealthCondition.Murmur.value,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = true,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(7),
        hsRequiredSurgeryOrHospitalization = Some(2),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Cardiac.value,
        hsCondition = HealthCondition.OtherCardiac.value,
        hsConditionOtherDescription = Some("waffles"),
        hsConditionIsCongenital = true,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
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
        hsCondition = HealthCondition.Murmur.value,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2019),
        hsDiagnosisMonth = Some(1),
        hsRequiredSurgeryOrHospitalization = Some(1),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Cardiac.value,
        hsCondition = HealthCondition.ValveDisease.value,
        hsConditionOtherDescription = Some("pesto"),
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2019),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(2),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Cardiac.value,
        hsCondition = HealthCondition.OtherCardiac.value,
        hsConditionOtherDescription = Some("zucchini"),
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
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
        hsCondition = HealthCondition.EctopicUreter.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Kidney.value,
        hsCondition = HealthCondition.OtherKidney.value,
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Kidney.value,
        hsCondition = HealthCondition.AKF.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Kidney.value,
        hsCondition = HealthCondition.OtherKidney.value,
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
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
        hsCondition = HealthCondition.UrinaryIncontinence.value,
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
        hsCondition = HealthCondition.UrinaryIncontinence.value,
        hsConditionOtherDescription = None,
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = Some("funfunfun"),
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
        hsCondition = HealthCondition.Cryptorchid.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Reproductive.value,
        hsCondition = HealthCondition.OtherReproductive.value,
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Reproductive.value,
        hsCondition = HealthCondition.BPH.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Reproductive.value,
        hsCondition = HealthCondition.OtherReproductive.value,
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
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
        hsCondition = HealthCondition.HumanMedications.value,
        hsConditionOtherDescription = Some("mentos"),
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.ToxinConsumption.value,
        hsCondition = HealthCondition.RecreationalDrugs.value,
        hsConditionOtherDescription = Some("oregano"),
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(true)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.ToxinConsumption.value,
        hsCondition = HealthCondition.OtherToxin.value,
        hsConditionOtherDescription = Some("other tasty looking things"),
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
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
        hsCondition = HealthCondition.MissingLimb.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Orthopedic.value,
        hsCondition = HealthCondition.OtherOrthopedic.value,
        hsConditionOtherDescription = Some("olives"),
        hsConditionIsCongenital = true,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
        hsDiagnosisYear = Some(2020),
        hsDiagnosisMonth = Some(2),
        hsRequiredSurgeryOrHospitalization = Some(3),
        hsFollowUpOngoing = Some(false)
      ),
      HlesHealthCondition(
        dogId = 1L,
        hsConditionType = HealthConditionType.Orthopedic.value,
        hsCondition = HealthCondition.CSS.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Orthopedic.value,
        hsCondition = HealthCondition.OtherOrthopedic.value,
        hsConditionOtherDescription = Some("ohno"),
        hsConditionIsCongenital = false,
        hsConditionCause = None,
        hsConditionCauseOtherDescription = None,
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
        hsCondition = HealthCondition.AutoimmuneThyroiditis.value,
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
        dogId = 1L,
        hsConditionType = HealthConditionType.Immune.value,
        hsCondition = HealthCondition.OtherImmune.value,
        hsConditionOtherDescription = Some("sriracha"),
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

  it should "correctly map health status data when fields are null" in {
    val emptyRecord = RawRecord(1, Map.empty)

    HealthTransformations.mapHealthConditions(emptyRecord) shouldBe List.empty
  }
}
