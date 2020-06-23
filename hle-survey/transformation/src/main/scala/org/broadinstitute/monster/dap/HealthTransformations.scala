package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesHealthCondition

object HealthTransformations {

  /** Parse all health-condition-related fields out of a raw RedCap record. */
  def mapHealthConditions(rawRecord: RawRecord): Iterable[HlesHealthCondition] =
    healthConditions.flatMap(processHealthCondition(rawRecord, _))

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

  def processHealthCondition(
    rawRecord: RawRecord,
    healthCondition: HealthCondition
  ): Iterable[HlesHealthCondition] = {
    val congenital: Option[HlesHealthCondition] = healthCondition.congenital.flatMap {
      case (target, dependent) =>
        // if congenital, then check hs_congenital_yn && healthCondition.congenital._2
        if (rawRecord.getBoolean("hs_congenital_yn") &&
            rawRecord.getBoolean(s"hs_${dependent}_yn"))
          // congenital other
          if (healthCondition.isOther)
            createHealthConditionRow(
              rawRecord,
              healthCondition.congenital.get._1,
              conditionTypes.apply(healthCondition.conditionType),
              conditions.apply(healthCondition.condition),
              isCongenital = true,
              conditionOtherDescription = rawRecord.getOptional(s"hs_${target}_spec")
            )
          // congenital general
          else if (rawRecord.getBoolean(s"hs_${target}"))
            createHealthConditionRow(
              rawRecord,
              target,
              conditionTypes.apply(healthCondition.conditionType),
              conditions.apply(healthCondition.condition),
              isCongenital = true
            )
          else None
        else None
    }
    val nonCongenital: Option[HlesHealthCondition] = healthCondition.nonCongenital.flatMap {
      case (target, dependent) =>
        if (rawRecord.getBoolean(s"hs_${dependent}_yn"))
          // non-congenital other
          if (healthCondition.isOther)
            createHealthConditionRow(
              rawRecord,
              target,
              conditionTypes.apply(healthCondition.conditionType),
              conditions.apply(healthCondition.condition),
              conditionOtherDescription = rawRecord.getOptional(s"hs_${target}_spec")
            )
          // specific non-congenital blindness case
          else if (healthCondition.condition == "blind") {
            val isCauseKnown = rawRecord.getBoolean("hs_dx_eye_cause_yn")
            val conditionCause =
              if (isCauseKnown) rawRecord.getOptionalNumber("hs_dx_eye_cause") else None
            createHealthConditionRow(
              rawRecord,
              target,
              conditionTypes.apply(healthCondition.conditionType),
              conditions.apply(healthCondition.condition),
              conditionCause = conditionCause,
              conditionCauseOtherDescription =
                if (isCauseKnown && (conditionCause.getOrElse(false) == 98))
                  rawRecord.getOptional("hs_dx_eye_cause_other")
                else None
            )
          }
          // non congenital general case
          else if (rawRecord.getBoolean(s"hs_${target}"))
            createHealthConditionRow(
              rawRecord,
              target,
              conditionTypes.apply(healthCondition.conditionType),
              conditions.apply(healthCondition.condition)
            )
          else None
        else None
    }
    Iterable.concat(congenital, nonCongenital)
  }

  val healthConditions: List[HealthCondition] = List(
    // eye conditions
    HealthCondition(
      "eye",
      "blind",
      Some(("cg_eye_blind", "cg_eye_disorders")),
      Some(("dx_blind", "dx_eye"))
    ),
    HealthCondition(
      "eye",
      "cat",
      Some(("cg_eye_cat", "cg_eye_disorders")),
      Some(("dx_cat", "dx_eye"))
    ),
    HealthCondition(
      "eye",
      "glauc",
      Some(("cg_eye_glauc", "cg_eye_disorders")),
      Some(("dx_glauc", "dx_eye"))
    ),
    HealthCondition(
      "eye",
      "kcs",
      Some(("cg_eye_kcs", "cg_eye_disorders")),
      Some(("dx_kcs", "dx_eye"))
    ),
    HealthCondition("eye", "ppm", Some(("cg_eye_ppm", "cg_eye_disorders")), None),
    HealthCondition("eye", "miss", Some(("cg_eye_miss", "cg_eye_disorders")), None),
    HealthCondition(
      "eye",
      "eye_other",
      Some(("cg_eye_other", "cg_eye_disorders")),
      Some(("dx_eye_other", "dx_eye")),
      isOther = true
    ),
    HealthCondition("eye", "ce", None, Some(("dx_ce", "dx_eye"))),
    HealthCondition("eye", "conj", None, Some(("dx_conj", "dx_eye"))),
    HealthCondition("eye", "cu", None, Some(("dx_cu", "dx_eye"))),
    HealthCondition("eye", "dist", None, Some(("dx_dist", "dx_eye"))),
    HealthCondition("eye", "ectrop", None, Some(("dx_ectrop", "dx_eye"))),
    HealthCondition("eye", "entrop", None, Some(("dx_entrop", "dx_eye"))),
    HealthCondition("eye", "ilp", None, Some(("dx_ilp", "dx_eye"))),
    HealthCondition("eye", "ic", None, Some(("dx_ic", "dx_eye"))),
    HealthCondition("eye", "jcat", None, Some(("dx_jcat", "dx_eye"))),
    HealthCondition("eye", "ns", None, Some(("dx_ns", "dx_eye"))),
    HealthCondition("eye", "pu", None, Some(("dx_pu", "dx_eye"))),
    HealthCondition("eye", "pra", None, Some(("dx_pra", "dx_eye"))),
    HealthCondition("eye", "rd", None, Some(("dx_rd", "dx_eye"))),
    HealthCondition("eye", "uvei", None, Some(("dx_uvei", "dx_eye"))),
    // infectious conditions
    HealthCondition("infectious", "anaplasmosis", None, Some(("dx_anaplasmosis", "dx_infectious"))),
    HealthCondition("infectious", "asperg", None, Some(("dx_asperg", "dx_infectious"))),
    HealthCondition("infectious", "babesio", None, Some(("dx_babesio", "dx_infectious"))),
    HealthCondition("infectious", "blastomy", None, Some(("dx_blastomy", "dx_infectious"))),
    HealthCondition("infectious", "bordetella", None, Some(("dx_bordetella", "dx_infectious"))),
    HealthCondition("infectious", "brucellosis", None, Some(("dx_brucellosis", "dx_infectious"))),
    HealthCondition("infectious", "campylo", None, Some(("dx_campylo", "dx_infectious"))),
    HealthCondition("infectious", "chagas", None, Some(("dx_chagas", "dx_infectious"))),
    HealthCondition("infectious", "ccdia", None, Some(("dx_ccdia", "dx_infectious"))),
    HealthCondition("infectious", "ccdio", None, Some(("dx_ccdio", "dx_infectious"))),
    HealthCondition("infectious", "crypto", None, Some(("dx_crypto", "dx_infectious"))),
    HealthCondition("infectious", "dermato", None, Some(("dx_dermato", "dx_infectious"))),
    HealthCondition("infectious", "dstmp", None, Some(("dx_dstmp", "dx_infectious"))),
    HealthCondition("infectious", "ehrlich", None, Some(("dx_ehrlich", "dx_infectious"))),
    HealthCondition("infectious", "fever", None, Some(("dx_fever", "dx_infectious"))),
    HealthCondition("infectious", "gp", None, Some(("dx_gp", "dx_infectious"))),
    HealthCondition("infectious", "giar", None, Some(("dx_giar", "dx_infectious"))),
    HealthCondition("infectious", "granu", None, Some(("dx_granu", "dx_infectious"))),
    HealthCondition("infectious", "hrtworm", None, Some(("dx_hrtworm", "dx_infectious"))),
    HealthCondition("infectious", "histo", None, Some(("dx_histo", "dx_infectious"))),
    HealthCondition("infectious", "hepato", None, Some(("dx_hepato", "dx_infectious"))),
    HealthCondition("infectious", "hkworm", None, Some(("dx_hkworm", "dx_infectious"))),
    HealthCondition("infectious", "influ", None, Some(("dx_influ", "dx_infectious"))),
    HealthCondition("infectious", "isosp", None, Some(("dx_isosp", "dx_infectious"))),
    HealthCondition("infectious", "leish", None, Some(("dx_leish", "dx_infectious"))),
    HealthCondition("infectious", "lepto", None, Some(("dx_lepto", "dx_infectious"))),
    HealthCondition("infectious", "lyme", None, Some(("dx_lyme", "dx_infectious"))),
    HealthCondition("infectious", "mrsa", None, Some(("dx_mrsa", "dx_infectious"))),
    HealthCondition("infectious", "mycob", None, Some(("dx_mycob", "dx_infectious"))),
    HealthCondition("infectious", "parvo", None, Some(("dx_parvo", "dx_infectious"))),
    HealthCondition("infectious", "plague", None, Some(("dx_plague", "dx_infectious"))),
    HealthCondition("infectious", "pythium", None, Some(("dx_pythium", "dx_infectious"))),
    HealthCondition("infectious", "rmsf", None, Some(("dx_rmsf", "dx_infectious"))),
    HealthCondition("infectious", "rndworm", None, Some(("dx_rndworm", "dx_infectious"))),
    HealthCondition("infectious", "slmosis", None, Some(("dx_slmosis", "dx_infectious"))),
    HealthCondition("infectious", "slmpois", None, Some(("dx_slmpois", "dx_infectious"))),
    HealthCondition("infectious", "tpworm", None, Some(("dx_tpworm", "dx_infectious"))),
    HealthCondition("infectious", "toxop", None, Some(("dx_toxop", "dx_infectious"))),
    HealthCondition("infectious", "tular", None, Some(("dx_tular", "dx_infectious"))),
    HealthCondition("infectious", "whpworm", None, Some(("dx_whpworm", "dx_infectious"))),
    HealthCondition(
      "infectious",
      "infect_other",
      None,
      Some(("dx_infect_other", "dx_infectious")),
      isOther = true
    )
  )

  val conditionTypes: Map[String, Long] = Map(
    "eye" -> 0,
    "infectious" -> 1 // prolly need to make this way higher to fit in order correctly
  )

  val conditions: Map[String, Long] = Map(
    // eye disorders and diseases
    "blind" -> 0,
    "cat" -> 1,
    "glauc" -> 2,
    "kcs" -> 3,
    "ppm" -> 4,
    "miss" -> 5,
    "ce" -> 6,
    "conj" -> 7,
    "cu" -> 8,
    "dist" -> 9,
    "ectrop" -> 10,
    "entrop" -> 11,
    "ilp" -> 12,
    "ic" -> 13,
    "jcat" -> 14,
    "ns" -> 15,
    "pu" -> 16,
    "pra" -> 17,
    "rd" -> 18,
    "uvei" -> 19,
    "eye_other" -> 20,
    // infectious diseases
    "anaplasmosis" -> 21,
    "asperg" -> 22,
    "babesio" -> 23,
    "blastomy" -> 24,
    "bordetella" -> 25,
    "brucellosis" -> 26,
    "campylo" -> 27,
    "chagas" -> 28,
    "ccdia" -> 29,
    "ccdio" -> 30,
    "crypto" -> 31,
    "dermato" -> 32,
    "dstmp" -> 33,
    "ehrlich" -> 34,
    "fever" -> 35,
    "gp" -> 36,
    "giar" -> 37,
    "granu" -> 38,
    "hrtworm" -> 39,
    "histo" -> 40,
    "hepato" -> 41,
    "hkworm" -> 42,
    "influ" -> 43,
    "isosp" -> 44,
    "leish" -> 45,
    "lepto" -> 46,
    "lyme" -> 47,
    "mrsa" -> 48,
    "mycob" -> 49,
    "parvo" -> 50,
    "plague" -> 51,
    "pythium" -> 52,
    "rmsf" -> 53,
    "rndworm" -> 54,
    "slmosis" -> 55,
    "slmpois" -> 56,
    "tpworm" -> 57,
    "toxop" -> 58,
    "tular" -> 59,
    "whpworm" -> 60,
    "infect_other" -> 61
  )
}
