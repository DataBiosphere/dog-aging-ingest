package org.broadinstitute.monster.dap.healthcondition

import enumeratum.values.{IntEnum, IntEnumEntry}

sealed abstract class HealthConditionType(override val value: Int, val label: String)
    extends IntEnumEntry

object HealthConditionType extends IntEnum[HealthConditionType] {
  override val values = findValues

  case object Eye extends HealthConditionType(1, "Eye")
  case object Ear extends HealthConditionType(2, "Ear")
  case object Oral extends HealthConditionType(3, "Oral")
  case object Skin extends HealthConditionType(4, "Skin")
  case object Cardiac extends HealthConditionType(5, "Heart/Cardiac")
  case object Respiratory extends HealthConditionType(6, "Respiratory")
  case object Gastrointestinal extends HealthConditionType(7, "Gastrointestinal")
  case object Liver extends HealthConditionType(8, "Liver")
  case object Kidney extends HealthConditionType(9, "Kidney")
  case object Reproductive extends HealthConditionType(10, "Reproductive")
  case object Orthopedic extends HealthConditionType(11, "Bone/Orthopedic")
  case object Neurologic extends HealthConditionType(12, "Brain/Neurologic")
  case object Endocrine extends HealthConditionType(13, "Endocrine")
  case object Hematologic extends HealthConditionType(14, "Blood/Hematologic")
  case object OtherCongenital extends HealthConditionType(15, "Other Congenital")
  case object InfectiousDisease extends HealthConditionType(16, "Infectious Disease")
  case object ToxinConsumption extends HealthConditionType(17, "Toxin/Controlled Substance Consumption")
  case object Trauma extends HealthConditionType(18, "Trauma")
  case object Immune extends HealthConditionType(19, "Immune")
}
