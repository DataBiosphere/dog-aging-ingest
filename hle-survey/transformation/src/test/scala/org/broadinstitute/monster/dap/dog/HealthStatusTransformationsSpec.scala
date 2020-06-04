package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HealthStatusTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {

  behavior of "HealthStatusTransformations"

  it should "map summary-level health info" in {
    val example = Map(
      "ss_dog_vet_freq" -> Array("1"),
      "hs_general" -> Array("2"),
      "hs_new_cond_yn" -> Array("1"),
      "hs_new_cond_month" -> Array("0"),
      "hs_cond_chron" -> Array("1"),
      "hs_cond_chron_change" -> Array("0"),
      "hs_congenital_yn" -> Array("0"),
      "hs_hosp_yn" -> Array("1"),
      "hs_hosp_why" -> Array("1", "3", "99"),
      "hs_hosp_why_other" -> Array("D'oh"),
      "hs_other_med_info" -> Array("Dog is a zombie")
    )

    val out = HealthStatusTransformations.mapHealthSummary(
      RawRecord(1, example),
      HlesDog.init(1, 1)
    )

    out.ssVetFrequency.value shouldBe 1L
    out.hsGeneralHealth.value shouldBe 2L
    out.hsNewConditionDiagnosedRecently.value shouldBe true
    out.hsNewConditionDiagnosedLastMonth.value shouldBe false
    out.hsChronicConditionPresent.value shouldBe true
    out.hsChronicConditionRecentlyChangedOrTreated.value shouldBe false
    out.hsCongenitalConditionPresent.value shouldBe false
    out.hsRecentHospitalization.value shouldBe true
    out.hsRecentHospitalizationReasonSpayOrNeuter.value shouldBe true
    out.hsRecentHospitalizationReasonDentistry.value shouldBe false
    out.hsRecentHospitalizationReasonBoarding.value shouldBe true
    out.hsRecentHospitalizationReasonOther.value shouldBe true
    out.hsRecentHospitalizationReasonOtherDescription.value shouldBe "D'oh"
    out.hsOtherMedicalInfo.value shouldBe "Dog is a zombie"
  }

  it should "map statuses of condition types" in {
    val example = Map(
      "hs_cg_eye_disorders_yn" -> Array("1"),
      "hs_dx_eye_yn" -> Array("0"),
      "hs_cg_ear_disorders_yn" -> Array("0"),
      "hs_dx_ear_yn" -> Array("1"),
      "hs_cg_oral_disorders_yn" -> Array("0"),
      "hs_dx_oral_yn" -> Array("0"),
      "hs_cg_skin_disorders_yn" -> Array("1"),
      "hs_dx_skin_yn" -> Array("1"),
      "hs_cg_heart_disorders_yn" -> Array("0"),
      "hs_dx_cardiac_yn" -> Array("0"),
      "hs_cg_resp_disorders_yn" -> Array("1"),
      "hs_dx_respire_yn" -> Array("1"),
      "hs_cg_gastro_disorders_yn" -> Array("1"),
      "hs_dx_gi_yn" -> Array("0"),
      "hs_cg_liver_disorders_yn" -> Array("0"),
      "hs_dx_liver_yn" -> Array("1"),
      "hs_cg_kidney_disorders_yn" -> Array("0"),
      "hs_dx_kidney_yn" -> Array("0"),
      "hs_cg_repro_disorders_yn" -> Array("0"),
      "hs_dx_repro_yn" -> Array("0"),
      "hs_cg_bones_disorders_yn" -> Array("0"),
      "hs_dx_ortho_yn" -> Array("0"),
      "hs_cg_brain_disorders_yn" -> Array("1"),
      "hs_dx_neuro_yn" -> Array("1"),
      "hs_cg_endocr_disorders_yn" -> Array("1"),
      "hs_dx_endo_yn" -> Array("0"),
      "hs_cg_blood_disorders_yn" -> Array("0"),
      "hs_dx_hema_yn" -> Array("1"),
      "hs_dx_immune_yn" -> Array("0"),
      "hs_dx_infectious_yn" -> Array("1"),
      "hs_dx_tox_yn" -> Array("0"),
      "hs_dx_trauma_yn" -> Array("0"),
      "hs_dx_cancer_yn" -> Array("1"),
      "hs_cg_other_yn" -> Array("1")
    )

    val out = HealthStatusTransformations.mapConditions(
      RawRecord(1, example),
      HlesDog.init(1, 1)
    )

    out.hsHealthConditionsEye.value shouldBe 1L
    out.hsHealthConditionsEar.value shouldBe 2L
    out.hsHealthConditionsOral.value shouldBe 0L
    out.hsHealthConditionsSkin.value shouldBe 3L
    out.hsHealthConditionsCardiac.value shouldBe 0L
    out.hsHealthConditionsRespiratory.value shouldBe 3L
    out.hsHealthConditionsGastrointestinal.value shouldBe 1L
    out.hsHealthConditionsLiver.value shouldBe 2L
    out.hsHealthConditionsKidney.value shouldBe 0L
    out.hsHealthConditionsReproductive.value shouldBe 0L
    out.hsHealthConditionsOrthopedic.value shouldBe 0L
    out.hsHealthConditionsNeurological.value shouldBe 3L
    out.hsHealthConditionsEndocrine.value shouldBe 1L
    out.hsHealthConditionsHematologic.value shouldBe 2L
    out.hsHealthConditionsImmune.value shouldBe 0L
    out.hsHealthConditionsInfectiousDisease.value shouldBe 2L
    out.hsHealthConditionsToxinConsumption.value shouldBe 0L
    out.hsHealthConditionsTrauma.value shouldBe 0L
    out.hsHealthConditionsCancer.value shouldBe 2L
    out.hsHealthConditionsOther.value shouldBe 1L
  }

  it should "map flags for alternative care methods" in {
    val example = Map(
      "hs_other_health_care" -> Array("1", "3", "5", "7", "98"),
      "hs_other_health_care_other" -> Array("Voodoo")
    )

    val out = HealthStatusTransformations.mapAltCare(
      RawRecord(1, example),
      HlesDog.init(1, 1)
    )

    out.hsAlternativeCareAcupuncture.value shouldBe true
    out.hsAlternativeCareHerbalMedicine.value shouldBe false
    out.hsAlternativeCareHomeopathy.value shouldBe true
    out.hsAlternativeCareChiropractic.value shouldBe false
    out.hsAlternativeCareMassage.value shouldBe true
    out.hsAlternativeCareRehabilitationTherapy.value shouldBe false
    out.hsAlternativeCareReiki.value shouldBe true
    out.hsAlternativeCareTraditionalChineseMedicine.value shouldBe false
    out.hsAlternativeCareOther.value shouldBe true
    out.hsAlternativeCareOtherDescription.value shouldBe "Voodoo"
  }
}
