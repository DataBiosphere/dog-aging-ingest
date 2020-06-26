package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dap.healthcondition.HealthCondition
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesHealthCondition

object HealthTransformations {

  /** Parse all health-condition-related fields out of a raw RedCap record. */
  def mapHealthConditions(rawRecord: RawRecord): Iterable[HlesHealthCondition] =
    HealthCondition.values.flatMap { healthCondition =>
      val cg = healthCondition.conditionType.cgKey.flatMap { cgKey =>
        if (rawRecord.getBoolean("hs_congenital_yn") &&
            rawRecord.getBoolean(cgKey.gate) &&
            healthCondition.hasCg) {

          val prefix = healthCondition.prefixOverride.getOrElse(
            s"hs_cg_${cgKey.abbreviation}_${healthCondition.abbreviation}"
          )

          val conditionGate = healthCondition.computeGate(prefix)
          if (rawRecord.getBoolean(conditionGate)) {
            val base = createHealthConditionRow(
              rawRecord,
              prefix,
              healthCondition.conditionType.value,
              healthCondition.value,
              isCongenital = true
            )
            Some {
              if (healthCondition.isOther) {
                base.copy(hsConditionOtherDescription = rawRecord.getOptional(s"${prefix}_spec"))
              } else {
                base
              }
            }
          } else {
            None
          }
        } else {
          None
        }
      }

      val dx = healthCondition.conditionType.dxKey.flatMap { dxKey =>
        if (rawRecord.getBoolean(dxKey.gate)) {
          val infix =
            if (dxKey.typePrefixed || healthCondition.isOther) s"_${dxKey.abbreviation}" else ""
          val prefix = s"hs_dx${infix}_${healthCondition.abbreviation}"
          if (rawRecord.getBoolean(prefix)) {
            val base = createHealthConditionRow(
              rawRecord,
              prefix,
              healthCondition.conditionType.value,
              healthCondition.value,
              isCongenital = false
            )
            Some {
              if (healthCondition == HealthCondition.Blindness) {
                val isCauseKnown = rawRecord.getBoolean("hs_dx_eye_cause_yn")
                val conditionCause =
                  if (isCauseKnown) rawRecord.getOptionalNumber("hs_dx_eye_cause") else None
                base.copy(
                  hsConditionCause = conditionCause,
                  hsConditionCauseOtherDescription = if (conditionCause.contains(98)) {
                    rawRecord.getOptional("hs_dx_eye_cause_other")
                  } else {
                    None
                  }
                )
              } else if (healthCondition.isOther) {
                base.copy(hsConditionOtherDescription = rawRecord.getOptional(s"${prefix}_spec"))
              } else {
                base
              }
            }
          } else {
            None
          }
        } else {
          None
        }
      }

      Iterable.concat(cg, dx)
    }

  /** Generic helper method for creating Hles Health Condition rows. */
  def createHealthConditionRow(
    rawRecord: RawRecord,
    fieldPrefix: String,
    conditionType: Long,
    condition: Long,
    isCongenital: Boolean
  ): HlesHealthCondition =
    HlesHealthCondition(
      dogId = rawRecord.getRequired("study_id").toLong,
      hsConditionType = conditionType,
      hsCondition = condition,
      hsConditionOtherDescription = None,
      hsConditionIsCongenital = isCongenital,
      hsConditionCause = None,
      hsConditionCauseOtherDescription = None,
      hsDiagnosisYear = rawRecord.getOptionalNumber(s"${fieldPrefix}_year"),
      hsDiagnosisMonth = rawRecord.getOptionalNumber(s"${fieldPrefix}_month"),
      hsRequiredSurgeryOrHospitalization = rawRecord.getOptionalNumber(s"${fieldPrefix}_surg"),
      hsFollowUpOngoing = rawRecord.getOptionalBoolean(s"${fieldPrefix}_fu")
    )
  /*
  val healthConditions: List[HealthCondition] = List(
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
  )*/
}
