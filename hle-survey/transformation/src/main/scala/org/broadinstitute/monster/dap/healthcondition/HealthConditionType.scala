package org.broadinstitute.monster.dap.healthcondition

import enumeratum.values.{LongEnum, LongEnumEntry}

sealed abstract class HealthConditionType(
  override val value: Long,
  val label: String,
  val cgKey: Option[HealthConditionType.CgKey],
  val dxKey: Option[HealthConditionType.DxKey]
) extends LongEnumEntry

object HealthConditionType extends LongEnum[HealthConditionType] {

  case class CgKey(abbreviation: String, disorder: Boolean = true) {
    def gate: String = s"hs_cg_$abbreviation${if (disorder) "_disorders" else ""}_yn"
  }

  case class DxKey(abbreviation: String, typePrefixed: Boolean = true) {
    def gate: String = s"hs_dx_${abbreviation}_yn"
  }

  override val values = findValues

  // scalafmt: { maxColumn = 140, newlines.topLevelStatements = [] }
  case object Eye extends HealthConditionType(1L, "Eye", Some(CgKey("eye")), Some(DxKey("eye", false)))
  case object Ear extends HealthConditionType(2L, "Ear", Some(CgKey("ear")), Some(DxKey("ear")))
  case object Oral extends HealthConditionType(3L, "Oral", Some(CgKey("oral")), Some(DxKey("oral")))
  case object Skin extends HealthConditionType(4L, "Skin", Some(CgKey("skin")), Some(DxKey("skin")))
  case object Cardiac extends HealthConditionType(5L, "Heart/Cardiac", Some(CgKey("heart")), Some(DxKey("cardiac")))
  case object Respiratory extends HealthConditionType(6L, "Respiratory", Some(CgKey("resp")), Some(DxKey("respire")))
  case object Gastrointestinal extends HealthConditionType(7L, "Gastrointestinal", Some(CgKey("gastro")), Some(DxKey("gi")))
  case object Liver extends HealthConditionType(8L, "Liver", Some(CgKey("liver")), Some(DxKey("liver")))
  case object Kidney extends HealthConditionType(9L, "Kidney", Some(CgKey("kidney")), Some(DxKey("kidney")))
  case object Reproductive extends HealthConditionType(10L, "Reproductive", Some(CgKey("repro")), Some(DxKey("repro")))
  case object Orthopedic extends HealthConditionType(11L, "Bone/Orthopedic", Some(CgKey("bones")), Some(DxKey("ortho")))
  case object Neurologic extends HealthConditionType(12L, "Brain/Neurologic", Some(CgKey("brain")), Some(DxKey("neuro")))
  case object Endocrine extends HealthConditionType(13L, "Endocrine", Some(CgKey("endocr")), Some(DxKey("endo")))
  case object Hematopoietic extends HealthConditionType(14L, "Hematopoietic", Some(CgKey("blood")), Some(DxKey("hema")))
  case object OtherCongenital extends HealthConditionType(15L, "Other congenital disorder", Some(CgKey("other", false)), None)
  case object Infection extends HealthConditionType(16L, "Infectious disease", None, Some(DxKey("infectious", false)))
  case object ToxinConsumption extends HealthConditionType(17L, "Toxin Consumption", None, Some(DxKey("dx_tox")))
  case object Trauma extends HealthConditionType(18L, "Trauma", None, Some(DxKey("trauma")))
  case object Immune extends HealthConditionType(19L, "Immune", None, Some(DxKey("immune")))
}
