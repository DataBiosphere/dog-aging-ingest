package org.broadinstitute.monster.dap.dog

import enumeratum.values.{LongEnum, LongEnumEntry}

/**
  * High-level description of the types of health conditions a dog has.
  *
  * @param value raw value to store on a per-row basis in BQ
  * @param label string label to associate with the raw value in lookup tables
  */
sealed abstract class HealthConditionSummary(override val value: Long, val label: String)
    extends LongEnumEntry

object HealthConditionSummary extends LongEnum[HealthConditionSummary] {
  override val values = findValues

  // scalafmt: { maxColumn = 140, newlines.topLevelStatements = [] }
  case object None extends HealthConditionSummary(0L, "No disorder(s)")
  case object Congenital extends HealthConditionSummary(1L, "Only congenital disorder(s)")
  case object Diagnosed extends HealthConditionSummary(2L, "Only non-congenital disorder(s)")
  case object Both extends HealthConditionSummary(3L, "Both congenital and non-congenital disorder(s)")
}
