package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dap.environment.PollutantTransformations
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PollutantTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {

  behavior of "PollutantTransformations"

  it should "map pollutant variables where complete" in {
    val pollutantData = Map(
      "pv_data_year" -> Array("1"),
      "pv_co" -> Array("0.281453079"),
      "pv_no2" -> Array("9.261036847"),
      "pv_o3" -> Array("38.25333854"),
      "pv_pm10" -> Array("13.63006549"),
      "pv_pm25" -> Array("6.196086383"),
      "pv_so2" -> Array("0.70759654"),
      "pollutant_variables_complete" -> Array("2")
    )

    val pollutantDataMapped = PollutantTransformations.mapPollutantVariables(
      RawRecord(1, pollutantData)
    )

    // output of the example record's pollutant transformations
    pollutantDataMapped.pvDataYear.value shouldBe 1L
    pollutantDataMapped.pvCo shouldBe Some(0.281453079)
    pollutantDataMapped.pvNo2 shouldBe Some(9.261036847)
    pollutantDataMapped.pvO3 shouldBe Some(38.25333854)
    pollutantDataMapped.pvPm10 shouldBe Some(13.63006549)
    pollutantDataMapped.pvPm25 shouldBe Some(6.196086383)
    pollutantDataMapped.pvSo2 shouldBe Some(0.70759654)
    pollutantDataMapped.pvComplete.value shouldBe 2L
  }
}
