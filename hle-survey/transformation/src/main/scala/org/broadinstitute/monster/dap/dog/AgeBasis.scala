package org.broadinstitute.monster.dap.dog

import enumeratum.values.{LongEnum, LongEnumEntry}

/**
  * Description of how a dog's age was determined.
  *
  * @param value raw value to store on a per-row basis in BQ
  * @param label string label to associate with the raw value in lookup tables
  */
sealed abstract class AgeBasis(override val value: Long, val label: String) extends LongEnumEntry

object AgeBasis extends LongEnum[AgeBasis] {
  override val values = findValues

  // scalafmt: { maxColumn = 140, newlines.topLevelStatements = [] }
  case object Calculated extends AgeBasis(1L, "Calculated from birth year and month")
  case object EstimatedFromYear extends AgeBasis(2L, "Estimated from birth year")
  case object EstimatedByOwner extends AgeBasis(3L, "Estimated by owner")
}
