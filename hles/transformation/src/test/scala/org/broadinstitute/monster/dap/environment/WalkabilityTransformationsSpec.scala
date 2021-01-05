package org.broadinstitute.monster.dap.dog

import java.time.LocalDate

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
      "wv_walkscore_date" -> Array("2020-07-14 19:56:29.131646"),
      "wv_housing_units" -> Array("1532"),
      "wv_res_density" -> Array("2556.02391460212"),
      "wv_density_data_year" -> Array("1")
    )

    val walkabilityDataMapped = WalkabilityTransformations.mapWalkabilityVariables(
      RawRecord(1, walkabilityData)
    )

    // output of the example record's walkability transformations
    walkabilityDataMapped.wvWalkscore shouldBe Some(76.0)
    walkabilityDataMapped.wvWalkscoreDescrip.value shouldBe 2L
    walkabilityDataMapped.wvWalkscoreDate shouldBe Some(LocalDate.parse("2020-07-14"))
    walkabilityDataMapped.wvHousingUnits shouldBe Some(1532.0)
    walkabilityDataMapped.wvResDensity shouldBe Some(2556.02391460212)
    walkabilityDataMapped.wvDensityDataYear.value shouldBe 1L
  }

  it should "map be able to handle 'NA' values in wv_walkscore_date" in {
    // Case 1: Array("NA")
    val walkabilityData = Map(
      "wv_walkscore" -> Array("76"),
      "wv_walkscore_descrip" -> Array("2"),
      "wv_walkscore_date" -> Array("NA"),
      "wv_housing_units" -> Array("1532"),
      "wv_res_density" -> Array("2556.02391460212"),
      "wv_density_data_year" -> Array("1")
    )

    val walkabilityDataMapped = WalkabilityTransformations.mapWalkabilityVariables(
      RawRecord(1, walkabilityData)
    )

    // output of the example record's walkability transformations
    walkabilityDataMapped.wvWalkscoreDate shouldBe None

    // Case 3: Array("NA", "[DateTime]")
    val walkabilityData2 = Map(
      "wv_walkscore" -> Array("76"),
      "wv_walkscore_descrip" -> Array("2"),
      "wv_walkscore_date" -> Array("NA", "2020-07-14 19:56:29.131646"),
      "wv_housing_units" -> Array("1532"),
      "wv_res_density" -> Array("2556.02391460212"),
      "wv_density_data_year" -> Array("1")
    )

    val walkabilityDataMapped2 = WalkabilityTransformations.mapWalkabilityVariables(
      RawRecord(1, walkabilityData2)
    )

    // output of the example record's walkability transformations
    walkabilityDataMapped2.wvWalkscoreDate shouldBe Some(LocalDate.parse("2020-07-14"))

    // Case 3: Array("[DateTime]", "NA")
    val walkabilityData3 = Map(
      "wv_walkscore" -> Array("76"),
      "wv_walkscore_descrip" -> Array("2"),
      "wv_walkscore_date" -> Array("2020-07-14 19:56:29.131646", "NA"),
      "wv_housing_units" -> Array("1532"),
      "wv_res_density" -> Array("2556.02391460212"),
      "wv_density_data_year" -> Array("1")
    )

    val walkabilityDataMapped3 = WalkabilityTransformations.mapWalkabilityVariables(
      RawRecord(1, walkabilityData3)
    )

    // output of the example record's walkability transformations
    walkabilityDataMapped3.wvWalkscoreDate shouldBe Some(LocalDate.parse("2020-07-14"))
  }
}
