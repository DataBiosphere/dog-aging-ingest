package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dap.healthcondition.{
  HealthCondition,
  HealthConditionKey,
  HealthConditionType
}
import org.broadinstitute.monster.dogaging.jadeschema.table.AfusHealthCondition

object AfusHealthTransformations {

  //todo: ensure "fu_" prefix is applied to health conditions (currently only prefixed with "hs_"
  /** Parse all health-condition-related fields out of a raw RedCap record. */
  def mapHealthConditions(rawRecord: RawRecord): Iterable[AfusHealthCondition] = {
    val cgs = if (rawRecord.getBoolean("fu_hs_congenital_yn")) {
      HealthCondition.cgValues.flatMap { healthCondition =>
        for {
          cgKey <- healthCondition.conditionType.cgKey
          if rawRecord.getBoolean(s"fu_${cgKey.categoryGate}")
          prefix <- healthCondition.conditionType match {
            case HealthConditionType.OtherCongenital => Some(s"fu_${cgKey.dataPrefix}")
            case _ =>
              healthCondition.both.orElse(healthCondition.cg).map { abbrev =>
                s"fu_${cgKey.dataPrefix}_$abbrev"
              }
          }
          conditionGate = healthCondition.conditionType match {
            case HealthConditionType.OtherCongenital => s"fu_${prefix}_yn"
            case _                                   => s"fu_${prefix}"
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
              .map(suffix => s"fu_${cgKey.dataPrefix}_$suffix")
              .getOrElse(s"fu_${prefix}_spec")
            base.copy(afusHsNewConditionOtherDescription =
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
        if rawRecord.getBoolean(s"fu_${dxKey.categoryGate}")
        prefix <- healthCondition.both.orElse(healthCondition.dx).map { abbrev =>
          s"fu_${dxKey.dataPrefix}_$abbrev"
        }
        if rawRecord.getBoolean(prefix) && healthCondition.subcategory
          .map(subcategoryId =>
            // if a condition has a subcategory, only include it if that subcategory was selected in the
            // supplemental "check all that apply" question
            rawRecord.getArray(s"fu_${prefix}_spec").contains(subcategoryId.toString)
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
        // If [fu_hs_dx_eye_cause_yn]=Yes, define [fu_hs_eye_condition_cause] using the options in [fu_hs_dx_eye_cause];
        // If [fu_hs_dx_eye_cause_yn]=No,  define [fu_hs_eye_condition_cause] as 99 for TDR_Raw_value
        if (healthCondition == HealthCondition.Blindness) {
          val isCauseKnown = rawRecord.getBoolean("fu_hs_dx_eye_cause_yn")
          val conditionCause =
            if (isCauseKnown) rawRecord.getOptionalNumber("fu_hs_dx_eye_cause") else Some(99L)
          base.copy(
            afusHsNewEyeConditionCause = conditionCause,
            afusHsNewConditionCauseOtherDescription = if (conditionCause.contains(98)) {
              rawRecord.getOptionalStripped("fu_hs_dx_eye_cause_other")
            } else {
              None
            }
          )
        } else if (healthCondition == HealthCondition.UrinaryIncontinence) {
          val isCauseKnown = rawRecord.getBoolean("fu_hs_dx_kidney_ui_fu_cause")
          base.copy(
            afusHsNewConditionCauseOtherDescription =
              if (isCauseKnown) rawRecord.getOptionalStripped("fu_hs_dx_kidney_ui_fu_why") else None
          )
        } else if (healthCondition == HealthCondition.VestibularDisease) {
          base.copy(afusHsNewNeurologicalConditionVestibularDiseaseType =
            rawRecord.getOptional("fu_hs_dx_neuro_vd_type")
          )
        } else if (healthCondition.isOther) {
          val descriptionFieldName = healthCondition.descriptionSuffixOverride
            .map(suffix => s"fu_${dxKey.dataPrefix}_$suffix")
            .getOrElse(s"fu_${prefix}_spec")
          base.copy(afusHsNewConditionOtherDescription =
            rawRecord.getOptionalStripped(descriptionFieldName)
          )
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
    base: Iterable[AfusHealthCondition],
    rawRecord: RawRecord,
    isCongenital: Boolean,
    keyGetter: (HealthConditionType) => Option[HealthConditionKey]
  ): Iterable[AfusHealthCondition] = {
    HealthConditionType.values
      .filterNot(h => base.exists(c => c.afusHsNewConditionType == h.value) || keyGetter(h).isEmpty)
      .filter(h => rawRecord.getBoolean(s"fu_${keyGetter(h).get.categoryGate}"))
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
  ): AfusHealthCondition =
    AfusHealthCondition(
      dogId = rawRecord.id,
      afusHsNewConditionType =
        conditionType, //rawRecord.getOptionalNumber("fu_hs_cg_*_yn OR fu_hs_dx_*_yn OR fu_hs_*_other"),
      afusHsNewCondition =
        condition, //rawRecord.getOptionalNumber("fu_hs_cg_*_yn OR fu_hs_dx_*_yn OR fu_hs_*_other"),
      afusHsNewConditionIsCongenital =
        isCongenital, //rawRecord.getOptionalBoolean("fu_hs_cg_*_yn OR fu_hs_dx_*_yn"),
      afusHsNewConditionDiagnosisYear = rawRecord.getOptionalNumber(
        s"${fieldPrefix}_year"
      ), //rawRecord.getOptionalNumber("fu_hs_cg_*_year OR fu_hs_dx_*_year"),
      afusHsNewConditionDiagnosisMonth = rawRecord.getOptionalNumber(
        s"${fieldPrefix}_month"
      ), //rawRecord.getOptionalNumber("fu_hs_cg_*_month OR fu_hs_dx_*_month"),
      afusHsNewConditionRequiredSurgeryOrHospitalization = rawRecord.getOptionalNumber(
        s"${fieldPrefix}_surg"
      ), //rawRecord.getOptionalNumber("fu_hs_cg_*_surg OR fu_hs_dx_*_surg"),
      afusHsNewConditionFollowUpOngoing = rawRecord.getOptionalBoolean(
        s"${fieldPrefix}_fu"
      ), //rawRecord.getOptionalBoolean("fu_hs_cg_*_fu OR fu_hs_dx_*_fu"),
      afusHsNewConditionOtherDescription =
        None, //rawRecord.getOptional("fu_hs_*_spec OR fu_hs_*_other_spec"),
      afusHsNewEyeConditionCause =
        None, //rawRecord.getOptionalNumber("fu_hs_dx_eye_cause_yn, fu_hs_dx_eye_cause"),
      afusHsNewConditionCauseOtherDescription =
        None, //rawRecord.getOptional("fu_hs_dx_eye_cause_other OR fu_hs_dx_kidney_ui_fu_why"),
      afusHsNewNeurologicalConditionVestibularDiseaseType =
        None //rawRecord.getOptional("fu_hs_dx_neuro_vd_type")
    )
}
