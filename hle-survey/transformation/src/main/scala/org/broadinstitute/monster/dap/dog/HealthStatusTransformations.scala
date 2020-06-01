package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog

object HealthStatusTransformations {

  /** TODO */
  def mapHealthStatus(rawRecord: RawRecord, dog: HlesDog): HlesDog = {
    val transformations = List(
      mapHealthSummary _,
      mapConditions _,
      mapAltCare _
    )

    transformations.foldLeft(dog)((acc, f) => f(rawRecord, acc))
  }

  /** TODO */
  def mapHealthSummary(rawRecord: RawRecord, dog: HlesDog): HlesDog = {
    val recentHospitalization = rawRecord.getOptionalBoolean("hs_hosp_yn")
    val hospitalizationReason = recentHospitalization.flatMap {
      if (_) rawRecord.getOptionalNumber("hs_hosp_why") else None
    }

    dog.copy(
      ssVetFrequency = rawRecord.getOptionalNumber("ss_dog_vet_freq"),
      hsGeneralHealth = rawRecord.getOptionalNumber("hs_general"),
      hsNewConditionDiagnosedRecently = rawRecord.getOptionalBoolean("hs_new_cond_yn"),
      hsNewConditionDiagnosedLastMonth = rawRecord.getOptionalBoolean("hs_new_cond_month"),
      hsChronicConditionPresent = rawRecord.getOptionalBoolean("hs_cond_chron"),
      hsChronicConditionRecentlyChangedOrTreated =
        rawRecord.getOptionalBoolean("hs_cond_chron_change"),
      hsCongenitalConditionPresent = rawRecord.getOptionalBoolean("hs_congenital_yn"),
      hsRecentHospitalization = recentHospitalization,
      hsRecentHospitalizationReason = hospitalizationReason,
      hsRecentHospitalizationReasonOtherDescription = if (hospitalizationReason.contains(98L)) {
        rawRecord.getOptional("hs_hosp_why_other")
      } else {
        None
      },
      hsOtherMedicalInfo = rawRecord.getOptional("hs_other_med_info")
    )
  }

  /** TODO */
  def mapConditions(rawRecord: RawRecord, dog: HlesDog): HlesDog = {

    def code(congenitalField: Option[String], diagnosedField: Option[String]): Option[Long] = {
      val congenital = congenitalField.flatMap(rawRecord.getOptionalBoolean)
      val diagnosed = diagnosedField.flatMap(rawRecord.getOptionalBoolean)

      (congenital, diagnosed) match {
        case (None, None)             => None
        case (Some(true), Some(true)) => Some(3L)
        case (Some(true), _)          => Some(1L)
        case (_, Some(true))          => Some(2L)
        case _                        => Some(0L)
      }
    }

    dog.copy(
      hsHealthConditionsEye = code(Some("hs_cg_eye_disorders_yn"), Some("hs_dx_eye_yn")),
      hsHealthConditionsEar = code(Some("hs_cg_ear_disorders_yn"), Some("hs_dx_ear_yn")),
      hsHealthConditionsOral = code(Some("hs_cg_oral_disorders_yn"), Some("hs_dx_oral_yn")),
      hsHealthConditionsSkin = code(Some("hs_cg_skin_disorders_yn"), Some("hs_dx_skin_yn")),
      hsHealthConditionsCardiac = code(Some("hs_cg_heart_disorders_yn"), Some("hs_dx_cardiac_yn")),
      hsHealthConditionsRespiratory =
        code(Some("hs_cg_resp_disorders_yn"), Some("hs_dx_respire_yn")),
      hsHealthConditionsGastrointestinal =
        code(Some("hs_cg_gastro_disorders_yn"), Some("hs_dx_gi_yn")),
      hsHealthConditionsLiver = code(Some("hs_cg_liver_disorders_yn"), Some("hs_dx_liver_yn")),
      hsHealthConditionsKidney = code(Some("hs_cg_kidney_disorders_yn"), Some("hs_dx_kidney_yn")),
      hsHealthConditionsReproductive =
        code(Some("hs_cg_repro_disorders_yn"), Some("hs_dx_repro_yn")),
      hsHealthConditionsOrthopedic = code(Some("hs_cg_bones_disorders_yn"), Some("hs_dx_ortho_yn")),
      hsHealthConditionsNeurological =
        code(Some("hs_cg_brain_disorders_yn"), Some("hs_dx_neuro_yn")),
      hsHealthConditionsEndocrine = code(Some("hs_cg_endocr_disorders_yn"), Some("hs_dx_endo_yn")),
      hsHealthConditionsHematologic = code(Some("hs_cg_blood_disorders_yn"), Some("hs_dx_hema_yn")),
      hsHealthConditionsImmune = code(None, Some("hs_dx_immune_yn")),
      hsHealthConditionsInfectiousDisease = code(None, Some("hs_dx_infectious_yn")),
      hsHealthConditionsToxinConsumption = code(None, Some("hs_dx_tox_yn")),
      hsHealthConditionsTrauma = code(None, Some("hs_dx_trauma_yn")),
      hsHealthConditionsCancer = code(None, Some("hs_dx_cancer_yn")),
      hsHealthConditionsOther = code(Some("hs_cg_other_yn"), None)
    )
  }

  /** TODO */
  def mapAltCare(rawRecord: RawRecord, dog: HlesDog): HlesDog = {
    val altCareMethods = rawRecord.get("hs_other_health_care")
    val otherAltCare = altCareMethods.map(_.contains("98"))

    dog.copy(
      hsAlternativeCareAcupuncture = altCareMethods.map(_.contains("1")),
      hsAlternativeCareHerbalMedicine = altCareMethods.map(_.contains("2")),
      hsAlternativeCareHomeopathy = altCareMethods.map(_.contains("3")),
      hsAlternativeCareChiropractic = altCareMethods.map(_.contains("4")),
      hsAlternativeCareMassage = altCareMethods.map(_.contains("5")),
      hsAlternativeCareRehabilitationTherapy = altCareMethods.map(_.contains("6")),
      hsAlternativeCareReiki = altCareMethods.map(_.contains("7")),
      hsAlternativeCareTraditionalChineseMedicine = altCareMethods.map(_.contains("8")),
      hsAlternativeCareOther = otherAltCare,
      hsAlternativeCareOtherDescription = otherAltCare.flatMap {
        if (_) rawRecord.getOptional("hs_other_health_care_other") else None
      }
    )
  }
}
