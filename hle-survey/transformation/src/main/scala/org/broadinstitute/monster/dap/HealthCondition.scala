package org.broadinstitute.monster.dap

/**
  *
  * @param conditionType a name to categorize the condition type by, like "eye", "ear", "cardiac", etc.
  * @param condition     a specific condition within a condition type, like "blind", "cat", "glauc", "other", etc.
  * @param congenital    a tuple where the first element is the piece of the target field name that fits in
  *                      "hs_*_yn", like "cg_eye_blind", and where the second element is the higher level boolean
  *                      flag name that fits in "hs_*_yn", like "cg_eye_disorders", that the first depends on
  * @param nonCongenital a tuple where the first element is the piece of the target field name that fits in "hs_*",
  *                      like "dx_blind", and where the second element is the higher level boolean flag name that fits
  *                      in "hs_*_yn", like "dx_eye", that the first depends on
  * @param isOther       a boolean flag that indicates this condition is an "other" condition, helpful for the business
  *                      logic to correctly transform data
  */
case class HealthCondition(
  conditionType: String,
  condition: String,
  congenital: Option[(String, String)] = None,
  nonCongenital: Option[(String, String)] = None,
  isOther: Boolean = false
)
