package org.broadinstitute.monster.dap

import io.circe.syntax._
import io.circe.JsonObject

import org.broadinstitute.monster.dap.healthcondition.{HealthCondition, HealthConditionType}
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesHealthCondition

object HealthTransformations {

  def jsonTsvTransform(json: JsonObject): JsonObject = {
    val congenitalBool = json.apply("hs_condition_is_congenital").get.asBoolean.getOrElse(false)
    val congenitalInt =
      if (congenitalBool) 1 else 0
    val primaryKey = Seq(json.apply("dog_id").get, json.apply("hs_condition").get, congenitalInt)
      .map(_.toString)
      .mkString("-")
    json.add("entity:hles_health_condition_id", primaryKey.asJson)
  }

  val tsvHeaders: Seq[String] =
    Seq("entity:hles_health_condition_id") ++ CaseClassInspector
      .snakeCaseHeaderList[HlesHealthCondition]

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
            healthCondition.value,
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
          base.copy(hsConditionCauseOtherDescription =
            rawRecord.getOptionalStripped("hs_dx_neuro_vd_type")
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

    Iterable.concat(cgs, dxs)
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
      dogId = rawRecord.id,
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
