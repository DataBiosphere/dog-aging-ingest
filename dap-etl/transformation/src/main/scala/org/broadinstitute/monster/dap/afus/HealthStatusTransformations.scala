package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.AfusDogHealthStatus

object HealthStatusTransformations {

  def mapHealthStatus(rawRecord: RawRecord): AfusDogHealthStatus = {
    val init = AfusDogHealthStatus.init()

    val transformations = List(
      mapHighLevelFields _,
      mapOngoingConditions _,
      mapNewConditions _,
      mapAltCare _
    )

    transformations.foldLeft(init)((acc, f) => f(rawRecord, acc))
  }

  def mapHighLevelFields(rawRecord: RawRecord, dog: AfusDogHealthStatus): AfusDogHealthStatus = {
    val recentHospitalization = rawRecord.getOptionalBoolean("fu_hs_hosp_yn")
    val hospitalizationReasons = recentHospitalization.flatMap {
      if (_) rawRecord.get("fu_hs_hosp_why") else None
    }
    val otherHospitalization = hospitalizationReasons.map(_.contains("99"))

    dog.copy(
      afusHsGeneralHealth = rawRecord.getOptionalNumber("fu_hs_general"),
      afusHsRecentHospitalizationYn = rawRecord.getOptionalBoolean("fu_hs_hosp_yn"),
      afusHsRecentHospitalizationWhySpayNeuter = hospitalizationReasons.map(_.contains("1")),
      afusHsRecentHospitalizationWhyDentistry = hospitalizationReasons.map(_.contains("2")),
      afusHsRecentHospitalizationWhyBoarding = hospitalizationReasons.map(_.contains("3")),
      afusHsRecentHospitalizationWhyOther = otherHospitalization,
      afusHsRecentHospitalizationWhyOtherDescription =
        if (otherHospitalization.contains(true))
          rawRecord.getOptionalStripped("fu_hs_hosp_why_other")
        else None,
      afusHsOtherMedicalInfo = rawRecord.getOptional("fu_hs_other_med_info")
    )
  }

  def mapOngoingConditions(rawRecord: RawRecord, dog: AfusDogHealthStatus): AfusDogHealthStatus = {
    val oldConditionTreatment = rawRecord.getOptionalBoolean("fu_hs_old_cond_treatment_yn")
    val conditionTypes =
      if (oldConditionTreatment.contains(true)) rawRecord.get("fu_hs_old_cond_type") else None
    dog.copy(
      afusHsOngoingConditionTreatmentYn = oldConditionTreatment,
      afusHsOngoingConditionInfectiousDisease = conditionTypes.map(_.contains("1")),
      afusHsOngoingConditionToxinConsumption = conditionTypes.map(_.contains("2")),
      afusHsOngoingConditionTrauma = conditionTypes.map(_.contains("3")),
      afusHsOngoingConditionCancer = conditionTypes.map(_.contains("4")),
      afusHsOngoingConditionEye = conditionTypes.map(_.contains("5")),
      afusHsOngoingConditionEar = conditionTypes.map(_.contains("6")),
      afusHsOngoingConditionOral = conditionTypes.map(_.contains("7")),
      afusHsOngoingConditionSkin = conditionTypes.map(_.contains("8")),
      afusHsOngoingConditionCardiac = conditionTypes.map(_.contains("9")),
      afusHsOngoingConditionRespiratory = conditionTypes.map(_.contains("10")),
      afusHsOngoingConditionGastrointestinal = conditionTypes.map(_.contains("11")),
      afusHsOngoingConditionLiver = conditionTypes.map(_.contains("12")),
      afusHsOngoingConditionKidney = conditionTypes.map(_.contains("13")),
      afusHsOngoingConditionReproductive = conditionTypes.map(_.contains("14")),
      afusHsOngoingConditionOrthopedic = conditionTypes.map(_.contains("15")),
      afusHsOngoingConditionNeurologic = conditionTypes.map(_.contains("16")),
      afusHsOngoingConditionEndocrine = conditionTypes.map(_.contains("17")),
      afusHsOngoingConditionHematologic = conditionTypes.map(_.contains("18")),
      afusHsOngoingConditionImmune = conditionTypes.map(_.contains("19")),
      afusHsOngoingConditionOther = conditionTypes.map(_.contains("98")),
      afusHsOngoingConditionOtherDescription =
        rawRecord.getOptionalStripped("fu_hs_old_cond_type_spec"),
      afusHsNewConditionDiagnosedLastYear = rawRecord.getOptionalBoolean("fu_hs_new_cond_year_yn"),
      afusHsNewConditionDiagnosedLastThreeMonths =
        rawRecord.getOptionalBoolean("fu_hs_new_condition_3months"),
      afusHsChronicConditionDiagnosedLastYear =
        rawRecord.getOptionalBoolean("fu_hs_cond_chron_year"),
      afusHsChronicConditionChangedTreatedLastThreeMonths =
        rawRecord.getOptionalBoolean("fu_hs_cond_chron_change_3months")
    )
  }

  def mapNewConditions(rawRecord: RawRecord, dog: AfusDogHealthStatus): AfusDogHealthStatus = {
    def code(congenitalField: Option[String], diagnosedField: Option[String]): Option[Long] = {
      val congenital = congenitalField.flatMap(rawRecord.getOptionalBoolean)
      val diagnosed = diagnosedField.flatMap(rawRecord.getOptionalBoolean)
      // todo: redo this logic with the correct output:
      // 0 if neither, 1 if only congenital, 2 if only diagnosed, 3 if both
      (congenital, diagnosed) match {
        case (None, None)             => None
        case (Some(true), Some(true)) => Some(HealthConditionSummary.Both.value)
        case (Some(true), _)          => Some(HealthConditionSummary.Congenital.value)
        case (_, Some(true))          => Some(HealthConditionSummary.Diagnosed.value)
        case _                        => Some(HealthConditionSummary.None.value)
      }
    }

    dog.copy(
      afusHsNewConditionEye = code(Some("fu_hs_cg_eye_disorders_yn"), Some("fu_hs_dx_eye_yn")),
      afusHsNewConditionEar = code(Some("fu_hs_cg_ear_disorders_yn"), Some("fu_hs_dx_ear_yn")),
      afusHsNewConditionOral = code(Some("fu_hs_cg_oral_disorders_yn"), Some("fu_hs_dx_oral_yn")),
      afusHsNewConditionSkin = code(Some("fu_hs_cg_skin_disorders_yn"), Some("fu_hs_dx_skin_yn")),
      afusHsNewConditionCardiac =
        code(Some("fu_hs_cg_heart_disorders_yn"), Some("fu_hs_dx_cardiac_yn")),
      afusHsNewConditionRespiratory =
        code(Some("fu_hs_cg_resp_disorders_yn"), Some("fu_hs_dx_respire_yn")),
      afusHsNewConditionGastrointestinal =
        code(Some("fu_hs_cg_gastro_disorders_yn"), Some("fu_hs_dx_gi_yn")),
      afusHsNewConditionLiver =
        code(Some("fu_hs_cg_liver_disorders_yn"), Some("fu_hs_dx_liver_yn")),
      afusHsNewConditionKidney =
        code(Some("fu_hs_cg_kidney_disorders_yn"), Some("fu_hs_dx_kidney_yn")),
      afusHsNewConditionReproductive =
        code(Some("fu_hs_cg_repro_disorders_yn"), Some("fu_hs_dx_repro_yn")),
      afusHsNewConditionOrthopedic =
        code(Some("fu_hs_cg_bones_disorders_yn"), Some("fu_hs_dx_ortho_yn")),
      afusHsNewConditionNeurological =
        code(Some("fu_hs_cg_brain_disorders_yn"), Some("fu_hs_dx_neuro_yn")),
      afusHsNewConditionEndocrine =
        code(Some("fu_hs_cg_endocr_disorders_yn"), Some("fu_hs_dx_endo_yn")),
      afusHsNewConditionHematologic =
        code(Some("fu_hs_cg_blood_disorders_yn"), Some("fu_hs_dx_hema_yn")),
      afusHsNewConditionImmune = code(None, Some("fu_hs_dx_immune_yn")),
      afusHsNewConditionInfectiousDisease = code(None, Some("fu_hs_dx_infectious_yn")),
      afusHsNewConditionToxinConsumption = code(None, Some("fu_hs_dx_tox_yn")),
      afusHsNewConditionTrauma = code(None, Some("fu_hs_dx_trauma_yn")),
      afusHsNewConditionCancer = code(None, Some("fu_hs_dx_cancer_yn")),
      afusHsNewConditionOther = code(Some("fu_hs_cg_other_yn"), None)
    )
  }

  def mapAltCare(rawRecord: RawRecord, dog: AfusDogHealthStatus): AfusDogHealthStatus = {
    val altCareMethods = rawRecord.get("fu_hs_other_health_care")
    val otherAltCare = altCareMethods.map(_.contains("98"))
    dog.copy(
      afusHsAlternativeCareAcupuncture = altCareMethods.map(_.contains("1")),
      afusHsAlternativeCareHerbalMedicine = altCareMethods.map(_.contains("2")),
      afusHsAlternativeCareHomeopathy = altCareMethods.map(_.contains("3")),
      afusHsAlternativeCareChiropractic = altCareMethods.map(_.contains("4")),
      afusHsAlternativeCareMassage = altCareMethods.map(_.contains("5")),
      afusHsAlternativeCareRehabilitationTherapy = altCareMethods.map(_.contains("6")),
      afusHsAlternativeCareReiki = altCareMethods.map(_.contains("7")),
      afusHsAlternativeCareTraditionalChineseMedicine = altCareMethods.map(_.contains("8")),
      afusHsAlternativeCareNone = altCareMethods.map(_.contains("99")),
      afusHsAlternativeCareOther = otherAltCare,
      afusHsAlternativeCareOtherDescription = otherAltCare.flatMap {
        if (_) rawRecord.getOptionalStripped("fu_hs_other_health_care_other") else None
      }
    )
  }
}
