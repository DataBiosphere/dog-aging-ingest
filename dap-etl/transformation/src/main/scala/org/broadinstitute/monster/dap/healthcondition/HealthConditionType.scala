package org.broadinstitute.monster.dap.healthcondition

import enumeratum.values.{LongEnum, LongEnumEntry}

/**
  * General class of health condition that a dog might experience.
  *
  * @param value raw value to store on a per-row basis in BQ
  * @param label string label to associate with the raw value in lookup tables
  * @param cgKey metadata about how to look up congenital conditions in this category
  * @param dxKey metadata about how to look up non-congenital conditions in this category
  */
sealed abstract class HealthConditionType(
  override val value: Long,
  val label: String,
  val cgKey: Option[HealthConditionType.CgKey],
  val dxKey: Option[HealthConditionType.DxKey]
) extends LongEnumEntry

object HealthConditionType extends LongEnum[HealthConditionType] {

  /**
    * Container for metadata determining how to look up congenital conditions
    * within a category.
    *
    * @param abbreviation short-hand used in RedCap to represent the congenital category
    * @param disorder flag for whether or not the RedCap data uses a '_disorders' suffix
    *                 in the Y/N flag for the category
    */
  case class CgKey(abbreviation: String, disorder: Boolean = true) {

    /** Name of the Y/N field marking if there is any data in this congential category. */
    def categoryGate: String = s"hs_cg_$abbreviation${if (disorder) "_disorders" else ""}_yn"

    /** Prefix used for individual condition fields under this congenital category. */
    def dataPrefix: String = s"hs_cg_$abbreviation"
  }

  /**
    * Container for metadata determining how to look up non-congenital conditions
    * within a category.
    *
    * @param abbreviation short-hand used in RedCap to represent the non-congenital category
    * @param typePrefixed flag for whether or not the RedCap data uses the category
    *                     abbreviation as a piece of field names for individual conditions
    */
  case class DxKey(abbreviation: String, typePrefixed: Boolean = true) {

    /** Name of the Y/N field marking if there is any data in this non-congential category. */
    def categoryGate: String = s"hs_dx_${abbreviation}_yn"

    /** Prefix used for individual condition fields under this non-congenital category. */
    def dataPrefix: String = s"hs_dx${if (typePrefixed) s"_$abbreviation" else ""}"
  }

  override val values = findValues

  // scalafmt: { maxColumn = 140, newlines.topLevelStatements = [] }
  case object Eye extends HealthConditionType(1L, "Eye", Some(CgKey("eye")), Some(DxKey("eye", false)))
  case object Ear extends HealthConditionType(2L, "Ear/Nose/Throat", Some(CgKey("ear")), Some(DxKey("ear")))
  case object Oral extends HealthConditionType(3L, "Mouth/Dental/Oral", Some(CgKey("oral")), Some(DxKey("oral")))
  case object Skin extends HealthConditionType(4L, "Skin", Some(CgKey("skin")), Some(DxKey("skin")))
  case object Cardiac extends HealthConditionType(5L, "Cardiac", Some(CgKey("heart")), Some(DxKey("cardiac")))
  case object Respiratory extends HealthConditionType(6L, "Respiratory", Some(CgKey("resp")), Some(DxKey("respire")))
  case object Gastrointestinal extends HealthConditionType(7L, "Gastrointestinal", Some(CgKey("gastro")), Some(DxKey("gi")))
  case object Liver extends HealthConditionType(8L, "Liver/Pancreas", Some(CgKey("liver")), Some(DxKey("liver")))
  case object Kidney extends HealthConditionType(9L, "Kidney/Urinary", Some(CgKey("kidney")), Some(DxKey("kidney")))
  case object Reproductive extends HealthConditionType(10L, "Reproductive", Some(CgKey("repro")), Some(DxKey("repro")))
  case object Orthopedic extends HealthConditionType(11L, "Bone/Orthopedic", Some(CgKey("bones")), Some(DxKey("ortho")))
  case object Neurologic extends HealthConditionType(12L, "Brain/Neurologic", Some(CgKey("brain")), Some(DxKey("neuro")))
  case object Endocrine extends HealthConditionType(13L, "Endocrine", Some(CgKey("endocr")), Some(DxKey("endo")))
  case object Hematopoietic extends HealthConditionType(14L, "Hematopoietic", Some(CgKey("blood")), Some(DxKey("hema")))
  case object OtherCongenital extends HealthConditionType(15L, "Other Congenital Disorder", Some(CgKey("other", false)), None)
  case object Infection extends HealthConditionType(16L, "Infection/Parasites", None, Some(DxKey("infectious", false)))
  case object ToxinConsumption extends HealthConditionType(17L, "Toxin Consumption", None, Some(DxKey("tox")))
  case object Trauma extends HealthConditionType(18L, "Trauma", None, Some(DxKey("trauma")))
  case object Immune extends HealthConditionType(19L, "Immune-mediated", None, Some(DxKey("immune")))
}
