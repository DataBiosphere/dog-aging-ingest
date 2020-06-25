package org.broadinstitute.monster.dap.healthcondition

import enumeratum.values.{IntEnum, IntEnumEntry}

sealed abstract class HealthCondition(
  override val value: Int,
  val label: String,
  val conditionType: HealthConditionType,
  val congenital: Option[(String, String)] = None,
  val nonCongenital: Option[(String, String)] = None,
  val isOther: Boolean = false
) extends IntEnumEntry

object HealthCondition extends IntEnum[HealthCondition] {
  override val values = findValues

  // Eye conditions.

  case object Blindness
      extends HealthCondition(
        value = 101,
        label = "Blindness",
        conditionType = HealthConditionType.Eye,
        congenital = Some("cg_eye_blind" -> "cg_eye_disorders"),
        nonCongenital = Some("dx_blind" -> "dx_eye")
      )
  case object Cataracts
      extends HealthCondition(
        value = 102,
        label = "Cataracts",
        conditionType = HealthConditionType.Eye,
        congenital = Some("cg_eye_cat" -> "cg_eye_disorders"),
        nonCongenital = Some("dx_cat" -> "dx_eye")
      )
  case object Glaucoma
      extends HealthCondition(
        value = 103,
        label = "Glaucoma",
        conditionType = HealthConditionType.Eye,
        congenital = Some("cg_eye_glauc" -> "cg_eye_disorders"),
        nonCongenital = Some("dx_glauc" -> "dx_eye")
      )
  case object KCS
      extends HealthCondition(
        value = 104,
        label = "Keratoconjunctivitis sicca (KCS)",
        conditionType = HealthConditionType.Eye,
        congenital = Some("cg_eye_kcs" -> "cg_eye_disorders"),
        nonCongenital = Some("dx_kcs" -> "dx_eye")
      )
  case object PPM
      extends HealthCondition(
        value = 105,
        label = "Persistent pupillary membrane (PPM)",
        conditionType = HealthConditionType.Eye,
        congenital = Some("cg_eye_ppm" -> "cg_eye_disorders")
      )
  case object MissingEye
      extends HealthCondition(
        value = 106,
        label = "Missing one or both eyes",
        conditionType = HealthConditionType.Eye,
        congenital = Some("cg_eye_miss" -> "cg_eye_disorders")
      )
  case object CherryEye
      extends HealthCondition(
        value = 108,
        label = "Third eyelid prolapse (cherry eye)",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_ce" -> "dx_eye")
      )
  case object Conjunctivitis
      extends HealthCondition(
        value = 109,
        label = "Conjunctivitis",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_conj" -> "dx_eye")
      )
  case object CornealUlcer
      extends HealthCondition(
        value = 110,
        label = "Corneal ulcer",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_cu" -> "dx_eye")
      )
  case object Distichia
      extends HealthCondition(
        value = 111,
        label = "Distichia",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_dist" -> "dx_eye")
      )
  case object Ectropion
      extends HealthCondition(
        value = 112,
        label = "Ectropion (eyelid rolled out)",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_ectrop" -> "dx_eye")
      )
  case object Entropion
      extends HealthCondition(
        value = 113,
        label = "Entropion (eyelid rolled in)",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_entrop" -> "dx_eye")
      )
  case object ILP
      extends HealthCondition(
        value = 114,
        label = "Imperforate lacrimal punctum",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_ilp" -> "dx_eye")
      )
  case object IrisCyst
      extends HealthCondition(
        value = 115,
        label = "Iris cyst",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_ic" -> "dx_eye")
      )
  case object JuvenileCataracts
      extends HealthCondition(
        value = 116,
        label = "Juvenile cataracts",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_jcat", "dx_eye")
      )
  case object NS
      extends HealthCondition(
        value = 117,
        label = "Nuclear sclerosis (whitening of the eye)",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_ns" -> "dx_eye")
      )
  case object PU
      extends HealthCondition(
        value = 118,
        label = "Pigmentary uveitis",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_pu", "dx_eye")
      )
  case object PRA
      extends HealthCondition(
        value = 119,
        label = "Progressive retinal atrophy or degeneration",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_pra" -> "dx_eye")
      )
  case object RD
      extends HealthCondition(
        value = 120,
        label = "Retinal detachment",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_rd" -> "dx_eye")
      )
  case object Uveitis
      extends HealthCondition(
        value = 121,
        label = "Uveitis",
        conditionType = HealthConditionType.Eye,
        nonCongenital = Some("dx_uvei" -> "dx_eye")
      )
  case object OtherEye
      extends HealthCondition(
        value = 198,
        label = "Other Eye Condition",
        conditionType = HealthConditionType.Eye,
        congenital = Some("cg_eye_other" -> "cg_eye_disorders"),
        nonCongenital = Some("dx_eye_other" -> "dx_eye"),
        isOther = true
      )

  // Infections diseases.
}
