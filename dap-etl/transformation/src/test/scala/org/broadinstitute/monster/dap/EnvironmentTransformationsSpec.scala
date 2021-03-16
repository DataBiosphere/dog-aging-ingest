package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.fragment._
import org.broadinstitute.monster.dogaging.jadeschema.table.Environment
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues

class EnvironmentTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "EnvironmentTransformations"

  it should "map required environmental fields when complete" in {
    val mapped = EnvironmentTransformations.mapEnvironment(
      RawRecord(
        1,
        Map(
          "redcap_event_name" -> Array("sept2020_secondary_arm_1"),
          "baseline_complete" -> Array("2")
        )
      )
    )
    mapped shouldBe Some(
      Environment(
        dogId = 1L,
        address1Or2 = 2L,
        addressMonth = 9L,
        addressYear = 2020L,
        environmentGeocoding = Some(EnvironmentGeocoding.init()),
        environmentCensus = Some(EnvironmentCensus.init()),
        environmentPollutants = Some(EnvironmentPollutants.init()),
        environmentTemperaturePrecipitation = Some(EnvironmentTemperaturePrecipitation.init()),
        environmentWalkability = Some(EnvironmentWalkability.init())
      )
    )
  }

  // Testing the deduping behavior of getRequired
  it should "collapse duplicate rows when encountered" in {
    val mapped = EnvironmentTransformations.mapEnvironment(
      RawRecord(
        1,
        Map(
          "redcap_event_name" -> Array("dec2019_arm_1", "dec2019_arm_1"),
          "baseline_complete" -> Array("2")
        )
      )
    )
    mapped shouldBe Some(
      Environment(
        dogId = 1L,
        address1Or2 = 1L,
        addressMonth = 12L,
        addressYear = 2019L,
        environmentGeocoding = Some(EnvironmentGeocoding.init()),
        environmentCensus = Some(EnvironmentCensus.init()),
        environmentPollutants = Some(EnvironmentPollutants.init()),
        environmentTemperaturePrecipitation = Some(EnvironmentTemperaturePrecipitation.init()),
        environmentWalkability = Some(EnvironmentWalkability.init())
      )
    )
  }
}
