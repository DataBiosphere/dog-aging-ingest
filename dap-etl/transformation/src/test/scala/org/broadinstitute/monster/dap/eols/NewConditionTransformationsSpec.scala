package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class NewConditionTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {

  behavior of "NewConditionTransformations"

  it should "map new condition metadata variables where complete" in {
    val newConditionMetadata = Map(
      "eol_dx" -> Array("0", "1", "10", "13", "15", "20"),
      "eol_new_condition_infectious_disease" -> Array("true"),
      "eol_new_condition_respiratory" -> Array("true"),
      "eol_new_condition_kidney" -> Array("true"),
      "eol_new_condition_orthopedic" -> Array("true"),
      "eol_new_condition_other" -> Array("true"),
      "eol_dx_month1" -> Array("1"),
      "eol_dx_year1" -> Array("2012"),
      "eol_dx_month10" -> Array("2"),
      "eol_dx_year10" -> Array("2018"),
      "eol_dx_specify10" -> Array("hard breathing"),
      "eol_dx_month13" -> Array("8"),
      "eol_dx_year13" -> Array("2019"),
      "eol_dx_specify13" -> Array("kidney"),
      "eol_dx_month15" -> Array("11"),
      "eol_dx_year15" -> Array("2020"),
      "eol_dx_specify15" -> Array("mouth"),
      "eol_dx_month20" -> Array("10"),
      "eol_dx_year20" -> Array("2020")
    )

    val newConditionDataMapped = NewConditionTransformations.mapNewConditionMetadata(
      RawRecord(1, newConditionMetadata)
    )

    newConditionDataMapped.eolNewConditionInfectiousDisease.value shouldBe (true)
    newConditionDataMapped.eolNewConditionRespiratory.value shouldBe (true)
    newConditionDataMapped.eolNewConditionKidney.value shouldBe (true)
    newConditionDataMapped.eolNewConditionOrthopedic.value shouldBe (true)
    newConditionDataMapped.eolNewConditionOther.value shouldBe (true)
    newConditionDataMapped.eolNewConditionEndocrine.value shouldBe (false)
    newConditionDataMapped.eolNewConditionHematologic.value shouldBe (false)

    newConditionDataMapped.eolNewConditionInfectiousDiseaseMonth.value shouldBe 1L
    newConditionDataMapped.eolNewConditionInfectiousDiseaseYear.value shouldBe 2012L
    newConditionDataMapped.eolNewConditionRespiratoryMonth.value shouldBe 2L
    newConditionDataMapped.eolNewConditionRespiratoryYear.value shouldBe 2018L
    newConditionDataMapped.eolNewConditionRespiratorySpecify.value shouldBe "hard breathing"
    newConditionDataMapped.eolNewConditionKidneyMonth.value shouldBe 8L
    newConditionDataMapped.eolNewConditionKidneyYear.value shouldBe 2019L
    newConditionDataMapped.eolNewConditionKidneySpecify.value shouldBe "kidney"
    newConditionDataMapped.eolNewConditionOrthopedicMonth.value shouldBe 11L
    newConditionDataMapped.eolNewConditionOrthopedicYear.value shouldBe 2020L
    newConditionDataMapped.eolNewConditionOrthopedicSpecify.value shouldBe "mouth"
    newConditionDataMapped.eolNewConditionOtherMonth.value shouldBe 10L
    newConditionDataMapped.eolNewConditionOtherYear.value shouldBe 2020L
  }
}
