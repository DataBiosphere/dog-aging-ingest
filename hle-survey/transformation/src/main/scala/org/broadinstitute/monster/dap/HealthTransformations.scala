package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesHealthCondition

object HealthTransformations {

  /** Parse all health-condition-related fields out of a raw RedCap record. */
  def mapHealthConditions(rawRecord: RawRecord): Iterable[HlesHealthCondition] =
    Iterable.concat(mapInfectiousDisease(rawRecord), mapEyeDisease(rawRecord))

  /** Generic helper method for creating Hles Health Condition rows. */
  def createHealthConditionRow(
    rawRecord: RawRecord,
    conditionName: String,
    conditionType: Long,
    condition: Long,
    isCongenital: Boolean = false,
    conditionOtherDescription: Option[String] = None,
    conditionCause: Option[Long] = None,
    conditionCauseOtherDescription: Option[String] = None
  ): Option[HlesHealthCondition] =
    if (rawRecord.getBoolean(s"hs_${conditionName}")) {
      Some(
        HlesHealthCondition(
          dogId = rawRecord.getRequired("study_id").toLong,
          hsConditionType = conditionType,
          hsCondition = condition,
          hsConditionOtherDescription = conditionOtherDescription,
          hsConditionIsCongenital = isCongenital,
          hsConditionCause = conditionCause,
          hsConditionCauseOtherDescription = conditionCauseOtherDescription,
          hsDiagnosisYear = rawRecord.getOptionalNumber(s"hs_${conditionName}_year"),
          hsDiagnosisMonth = rawRecord.getOptionalNumber(s"hs_${conditionName}_month"),
          hsRequiredSurgeryOrHospitalization =
            rawRecord.getOptionalNumber(s"hs_${conditionName}_surg"),
          hsFollowUpOngoing = rawRecord.getOptionalBoolean(s"hs_${conditionName}_fu")
        )
      )
    } else None

  /** Parse all infectious disease related fields out of a raw RedCap record. */
  def mapInfectiousDisease(rawRecord: RawRecord): Iterable[HlesHealthCondition] =
    if (rawRecord.getBoolean("hs_dx_infectious_yn")) {
      // iterate over all infectious diseases
      infectious_diseases.flatMap {
        // "other" case
        case ("infect_other", categorical) =>
          createHealthConditionRow(
            rawRecord,
            "dx_infect_other",
            infectiousDiseaseCondition,
            categorical,
            conditionOtherDescription = rawRecord.getOptional("hs_dx_infect_other_spec")
          )
        // generic case for infectious disease
        case (disease, categorical) =>
          createHealthConditionRow(
            rawRecord,
            s"dx_${disease}",
            infectiousDiseaseCondition,
            categorical
          )
      }
    } else None

  def mapEyeDisease(rawRecord: RawRecord): Iterable[HlesHealthCondition] =
    if (rawRecord.getBoolean("hs_dx_eye_yn")) {
      eye_diseases.flatMap {
        // "other" case
        case ("eye_other", categorical) =>
          createHealthConditionRow(
            rawRecord,
            "dx_eye_other",
            eyeDiseaseCondition,
            categorical,
            conditionOtherDescription = rawRecord.getOptional("hs_dx_eye_other_spec")
          )
        case ("blind", categorical) => {
          val isCauseKnown = rawRecord.getBoolean("hs_dx_eye_cause_yn")
          val conditionCause =
            if (isCauseKnown) rawRecord.getOptionalNumber("hs_dx_eye_cause") else None
          createHealthConditionRow(
            rawRecord,
            "dx_blind",
            eyeDiseaseCondition,
            categorical,
            conditionCause = conditionCause,
            conditionCauseOtherDescription =
              if (isCauseKnown && (conditionCause.getOrElse(None) == 98))
                rawRecord.getOptional("hs_dx_eye_cause_other")
              else None
          )
        }
        case (disease, categorical) =>
          createHealthConditionRow(rawRecord, s"dx_${disease}", eyeDiseaseCondition, categorical)
      }
    } else None

  // list conditions and assign categorical Longs
  val eyeDiseaseCondition = 1L
  val infectiousDiseaseCondition = 50L

  // specific condition type maps
  val infectious_diseases: Map[String, Long] = Map(
    "anaplasmosis" -> 0,
    "asperg" -> 1,
    "babesio" -> 2,
    "blastomy" -> 3,
    "bordetella" -> 4,
    "brucellosis" -> 5,
    "campylo" -> 6,
    "chagas" -> 7,
    "ccdia" -> 8,
    "ccdio" -> 9,
    "crypto" -> 10,
    "dermato" -> 11,
    "dstmp" -> 12,
    "ehrlich" -> 13,
    "fever" -> 14,
    "gp" -> 15,
    "giar" -> 16,
    "granu" -> 17,
    "hrtworm" -> 18,
    "histo" -> 19,
    "hepato" -> 20,
    "hkworm" -> 21,
    "influ" -> 22,
    "isosp" -> 23,
    "leish" -> 24,
    "lepto" -> 25,
    "lyme" -> 26,
    "mrsa" -> 27,
    "mycob" -> 28,
    "parvo" -> 29,
    "plague" -> 30,
    "pythium" -> 31,
    "rmsf" -> 32,
    "rndworm" -> 33,
    "slmosis" -> 34,
    "slmpois" -> 35,
    "tpworm" -> 36,
    "toxop" -> 37,
    "tular" -> 38,
    "whpworm" -> 39,
    "infect_other" -> 98
  )

  val eye_diseases: Map[String, Long] = Map(
    "cat" -> 0,
    "blind" -> 1,
    "ce" -> 2,
    "conj" -> 3,
    "cu" -> 4,
    "dist" -> 5,
    "kcs" -> 6,
    "ectrop" -> 7,
    "entrop" -> 8,
    "glauc" -> 9,
    "ilp" -> 10,
    "ic" -> 11,
    "jcat" -> 12,
    "ns" -> 13,
    "pu" -> 14,
    "pra" -> 15,
    "rd" -> 16,
    "uvei" -> 17,
    "eye_other" -> 98
  )
}
