package org.broadinstitute.monster.dap.common

import org.broadinstitute.monster.dap.healthcondition.{
  HealthCondition,
  HealthConditionKey,
  HealthConditionType
}
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesHealthCondition

object HealthTransformations {

  /** Parse all health-condition-related fields out of a raw RedCap record. */
  def mapHealthConditions(rawRecord: RawRecord): Iterable[HlesHealthCondition] = {
    val cgs = if (rawRecord.getBoolean("hs_congenital_yn")) {
      HealthCondition.cgValues.flatMap { healthCondition =>
        for {
          cgKey <- healthCondition.conditionType.cgKey
          if rawRecord.getBoolean(cgKey.categoryGate)
          prefix <- healthCondition.conditionType match {
            case HealthConditionType.OtherCongenital => Some(cgKey.dataPrefix)
            case _ =>
              healthCondition.both.orElse(healthCondition.cg).map { abbrev =>
                s"${cgKey.dataPrefix}_$abbrev"
              }
          }
          conditionGate = healthCondition.conditionType match {
            case HealthConditionType.OtherCongenital => s"${prefix}_yn"
            case _                                   => prefix
          }
          if rawRecord.getBoolean(conditionGate)
        } yield {
          val base = createHealthConditionRow(
            rawRecord,
            prefix,
            healthCondition.conditionType.value,
            Some(healthCondition.value),
            isCongenital = true
          )
          if (healthCondition.isOther) {
            val descriptionFieldName = healthCondition.descriptionSuffixOverride
              .map(suffix => s"${cgKey.dataPrefix}_$suffix")
              .getOrElse(s"${prefix}_spec")
            base.copy(hsConditionOtherDescription =
              rawRecord.getOptionalStripped(descriptionFieldName)
            )
          } else {
            base
          }
        }
      }
    } else {
      Iterable.empty
    }

    val dxs = HealthCondition.dxValues.flatMap { healthCondition =>
      for {
        dxKey <- healthCondition.conditionType.dxKey
        if rawRecord.getBoolean(dxKey.categoryGate)
        prefix <- healthCondition.both.orElse(healthCondition.dx).map { abbrev =>
          s"${dxKey.dataPrefix}_$abbrev"
        }
        if rawRecord.getBoolean(prefix) && healthCondition.subcategory
          .map(subcategoryId =>
            // if a condition has a subcategory, only include it if that subcategory was selected in the
            // supplemental "check all that apply" question
            rawRecord.getArray(s"${prefix}_spec").contains(subcategoryId.toString)
          )
          .getOrElse(true)
      } yield {
        val base = createHealthConditionRow(
          rawRecord,
          prefix,
          healthCondition.conditionType.value,
          Some(healthCondition.value),
          isCongenital = false
        )
        // If [hs_dx_eye_cause_yn]=Yes, define [hs_eye_condition_cause] using the options in [hs_dx_eye_cause];
        // If [hs_dx_eye_cause_yn]=No,  define [hs_eye_condition_cause] as 99 for TDR_Raw_value
        if (healthCondition == HealthCondition.Blindness) {
          val isCauseKnown = rawRecord.getBoolean("hs_dx_eye_cause_yn")
          val conditionCause =
            if (isCauseKnown) rawRecord.getOptionalNumber("hs_dx_eye_cause") else Some(99L)
          base.copy(
            hsEyeConditionCause = conditionCause,
            hsConditionCauseOtherDescription = if (conditionCause.contains(98)) {
              rawRecord.getOptionalStripped("hs_dx_eye_cause_other")
            } else {
              None
            }
          )
        } else if (healthCondition == HealthCondition.UrinaryIncontinence) {
          val isCauseKnown = rawRecord.getBoolean("hs_dx_kidney_ui_fu_cause")
          base.copy(
            hsConditionCauseOtherDescription =
              if (isCauseKnown) rawRecord.getOptionalStripped("hs_dx_kidney_ui_fu_why") else None
          )
        } else if (healthCondition == HealthCondition.VestibularDisease) {
          base.copy(hsNeurologicalConditionVestibularDiseaseType =
            rawRecord.getOptionalNumber("hs_dx_neuro_vd_type")
          )
        } else if (healthCondition.isOther) {
          val descriptionFieldName = healthCondition.descriptionSuffixOverride
            .map(suffix => s"${dxKey.dataPrefix}_$suffix")
            .getOrElse(s"${prefix}_spec")
          base.copy(hsConditionOtherDescription = rawRecord.getOptionalStripped(descriptionFieldName))
        } else {
          base
        }
      }
    }

    // Fill in with dummy health condition records for instances where the top level condition question
    // answered "yes" but no specific health conditions were filled in
    val cgsFillIn =
      createFillInConditionRecords(
        cgs,
        rawRecord,
        isCongenital = true,
        (h: HealthConditionType) => h.cgKey
      )
    val dxsFillIn =
      createFillInConditionRecords(
        dxs,
        rawRecord,
        isCongenital = false,
        (h: HealthConditionType) => h.dxKey
      )

    Iterable.concat(cgs, dxs, cgsFillIn, dxsFillIn)
  }

  def createFillInConditionRecords(
    base: Iterable[HlesHealthCondition],
    rawRecord: RawRecord,
    isCongenital: Boolean,
    keyGetter: (HealthConditionType) => Option[HealthConditionKey]
  ): Iterable[HlesHealthCondition] = {
    HealthConditionType.values
      .filterNot(h => base.exists(c => c.hsConditionType == h.value) || keyGetter(h).isEmpty)
      .filter(h => rawRecord.getBoolean(keyGetter(h).get.categoryGate))
      .map { hc =>
        createHealthConditionRow(
          rawRecord,
          keyGetter(hc).get.dataPrefix,
          hc.value,
          None,
          isCongenital = isCongenital
        )
      }
  }

  /** Generic helper method for creating Hles Health Condition rows. */
  def createHealthConditionRow(
    rawRecord: RawRecord,
    fieldPrefix: String,
    conditionType: Long,
    condition: Option[Long],
    isCongenital: Boolean
  ): HlesHealthCondition =
    HlesHealthCondition(
      dogId = rawRecord.id,
      hsConditionType = conditionType,
      hsCondition = condition,
      hsConditionOtherDescription = None,
      hsConditionIsCongenital = isCongenital,
      hsEyeConditionCause = None,
      hsConditionCauseOtherDescription = None,
      hsNeurologicalConditionVestibularDiseaseType = None,
      hsDiagnosisYear = rawRecord.getOptionalNumber(s"${fieldPrefix}_year"),
      hsDiagnosisMonth = rawRecord.getOptionalNumber(s"${fieldPrefix}_month"),
      hsRequiredSurgeryOrHospitalization = rawRecord.getOptionalNumber(s"${fieldPrefix}_surg"),
      hsFollowUpOngoing = rawRecord.getOptionalBoolean(s"${fieldPrefix}_fu")
    )
}
