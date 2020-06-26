package org.broadinstitute.monster.dap.healthcondition

import enumeratum.values.{LongEnum, LongEnumEntry}

sealed abstract class HealthCondition(
  override val value: Long,
  val label: String,
  val conditionType: HealthConditionType,
  val abbreviation: String,
  val hasCg: Boolean,
  val hasDx: Boolean,
  val isOther: Boolean = false,
  val prefixOverride: Option[String] = None,
  val computeGate: String => String = identity
) extends LongEnumEntry

object HealthCondition extends LongEnum[HealthCondition] {
  override val values = findValues

  import HealthConditionType.{findValues => _, _}

  // Eye conditions.
  case object Blindness extends HealthCondition(101L, "Blindness", Eye, "blind", true, true)
  case object Cataracts extends HealthCondition(102L, "Cataracts", Eye, "cat", true, true)
  case object Glaucoma extends HealthCondition(103L, "Glaucoma", Eye, "glauc", true, true)

  case object KCS
      extends HealthCondition(104L, "Keratoconjunctivitis sicca (KCS)", Eye, "kcs", true, true)

  case object PPM
      extends HealthCondition(105L, "Persistent pupillary membrane (PPM)", Eye, "ppm", true, false)

  case object MissingEye
      extends HealthCondition(106L, "Missing one or both eyes", Eye, "miss", true, false)

  case object CherryEye
      extends HealthCondition(108L, "Third eyelid prolapse (cherry eye)", Eye, "ce", false, true)

  case object Conjunctivitis
      extends HealthCondition(109L, "Conjunctivitis", Eye, "conj", false, true)
  case object CornealUlcer extends HealthCondition(110L, "Corneal ulcer", Eye, "cu", false, true)
  case object Distichia extends HealthCondition(111L, "Distichia", Eye, "dist", false, true)

  case object Ectropion
      extends HealthCondition(112L, "Ectropion (eyelid rolled out)", Eye, "ectrop", false, true)

  case object Entropion
      extends HealthCondition(113L, "Entropion (eyelid rolled in)", Eye, "entrop", false, true)

  case object ILP
      extends HealthCondition(114L, "Imperforate lacrimal punctum", Eye, "ilp", false, true)
  case object IrisCyst extends HealthCondition(115L, "Iris cyst", Eye, "ic", false, true)

  case object JuvenileCataracts
      extends HealthCondition(116L, "Juvenile cataracts", Eye, "jcat", false, true)
  case object NS extends HealthCondition(117L, "Nuclear sclerosis", Eye, "ns", false, true)
  case object PU extends HealthCondition(118L, "Pigmentary uveitis", Eye, "pu", false, true)

  case object PRA
      extends HealthCondition(119L, "Progressive retinal atrophy", Eye, "pra", false, true)
  case object RD extends HealthCondition(120L, "Retinal detachment", Eye, "rd", false, true)
  case object Uveitis extends HealthCondition(121L, "Uveitis", Eye, "uvei", false, true)

  case object OtherEye
      extends HealthCondition(198L, "Other eye condition", Eye, "other", true, true, isOther = true)

  // Other congenital conditions.
  case object OtherCG
      extends HealthCondition(
        1598L,
        "Other Congenital",
        OtherCongenital,
        "other",
        true,
        false,
        isOther = true,
        prefixOverride = Some("hs_cg_other"),
        computeGate = prefix => s"${prefix}_yn"
      )

  // Infections diseases.

}
