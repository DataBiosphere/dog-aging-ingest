package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dap.healthcondition.HealthCondition
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesHealthCondition

object HealthTransformations {

  /** Parse all health-condition-related fields out of a raw RedCap record. */
  def mapHealthConditions(rawRecord: RawRecord): Iterable[HlesHealthCondition] =
    HealthCondition.values.flatMap { healthCondition =>
      val cg = for {
        cgKey <- healthCondition.conditionType.cgKey
        if healthCondition.hasCg &&
          rawRecord.getBoolean("hs_congenital_yn") &&
          rawRecord.getBoolean(cgKey.gate)
        infix = healthCondition.cgPrefixOverride.getOrElse(
          s"${cgKey.abbreviation}_${healthCondition.abbreviation}"
        )
        prefix = s"hs_cg_$infix"
        conditionGate = healthCondition.computeGate(prefix)
        if rawRecord.getBoolean(conditionGate)
      } yield {
        val base = createHealthConditionRow(
          rawRecord,
          prefix,
          healthCondition.conditionType.value,
          healthCondition.value,
          isCongenital = true
        )
        if (healthCondition.isOther) {
          base.copy(hsConditionOtherDescription = rawRecord.getOptional(s"${prefix}_spec"))
        } else {
          base
        }
      }

      val dx = for {
        dxKey <- healthCondition.conditionType.dxKey
        if healthCondition.hasDx && rawRecord.getBoolean(dxKey.gate)
        infix = if (dxKey.typePrefixed) s"_${dxKey.abbreviation}" else ""
        prefix = s"hs_dx${infix}_${healthCondition.abbreviation}"
        if rawRecord.getBoolean(prefix)
      } yield {
        val base = createHealthConditionRow(
          rawRecord,
          prefix,
          healthCondition.conditionType.value,
          healthCondition.value,
          isCongenital = false
        )
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
}
