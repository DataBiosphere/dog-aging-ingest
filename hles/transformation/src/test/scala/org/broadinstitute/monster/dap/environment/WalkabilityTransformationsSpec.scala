package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dap.environment.WalkabilityTransformations
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WalkabilityTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {

  behavior of "WalkabilityTransformations"

  it should "map walkability variables where complete" in {
    val walkabilityData = Map(
      "wv_walkscore" -> Array("76"),
      "wv_walkscore_descrip" -> Array("2"),
      "wv_walkscore_date" -> Array("2020-07-14 19:56:29.131646"), //todo:This is clearly gonna fail
      "wv_housing_units" -> Array("1532"),
      "wv_res_density" -> Array("2556.02391460212"),
      "wv_density_data_year" -> Array("1")
    )

    val walkabilityDataMapped = WalkabilityTransformations.mapWalkabilityVariables(
      RawRecord(1, walkabilityData)
    )

    // output of the example record's walkability transformations
    walkabilityDataMapped.wvWalkscore.value shouldBe Some("76")
    walkabilityDataMapped.wvWalkscoreDescrip.value shouldBe 2L
    walkabilityDataMapped.wvWalkscoreDate.value shouldBe Some(
      "2020-07-14 19:56:29.131646"
    ) //todo:This is clearly gonna fail
    walkabilityDataMapped.wvHousingUnits.value shouldBe Some(1532)
    walkabilityDataMapped.wvResDensity.value shouldBe Some(2556.02391460212)
    walkabilityDataMapped.wvDensityDataYear.value shouldBe 1L
  }
}
