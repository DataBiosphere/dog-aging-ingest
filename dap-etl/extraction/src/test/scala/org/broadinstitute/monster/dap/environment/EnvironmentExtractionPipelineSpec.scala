package org.broadinstitute.monster.dap.environment

import java.time.{LocalDate, LocalTime, OffsetDateTime, ZoneOffset}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EnvironmentExtractionPipelineSpec extends AnyFlatSpec with Matchers {
  it should "generate the correct arms when provided startTime and endTime arguments" in {
    val startTime =
      OffsetDateTime.of(2020, 2, 1, 1, 0, 0, 0, ZoneOffset.ofHours(-5))
    val endTime =
      OffsetDateTime.of(2021, 1, 1, 1, 0, 0, 0, ZoneOffset.ofHours(-5))
    val envArms =
      EnvironmentExtractionPipeline.extractionArmsGenerator(Some(startTime), Some(endTime))

    envArms shouldBe List(
      "annual_feb2020_arm_1",
      "annual_feb2020_secondary_arm_1",
      "annual_mar2020_arm_1",
      "annual_mar2020_secondary_arm_1",
      "annual_apr2020_arm_1",
      "annual_apr2020_secondary_arm_1",
      "annual_may2020_arm_1",
      "annual_may2020_secondary_arm_1",
      "annual_jun2020_arm_1",
      "annual_jun2020_secondary_arm_1",
      "annual_jul2020_arm_1",
      "annual_jul2020_secondary_arm_1",
      "annual_aug2020_arm_1",
      "annual_aug2020_secondary_arm_1",
      "annual_sep2020_arm_1",
      "annual_sep2020_secondary_arm_1",
      "annual_oct2020_arm_1",
      "annual_oct2020_secondary_arm_1",
      "annual_nov2020_arm_1",
      "annual_nov2020_secondary_arm_1",
      "annual_dec2020_arm_1",
      "annual_dec2020_secondary_arm_1",
      "annual_jan2021_arm_1",
      "annual_jan2021_secondary_arm_1"
    )
  }

  it should "use EnvEpoch as startTime when none is provided" in {
    val endTime =
      OffsetDateTime.of(LocalDate.of(2020, 3, 2), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val envArms = EnvironmentExtractionPipeline.extractionArmsGenerator(None, Some(endTime))

    envArms shouldBe List(
      "annual_jan2020_arm_1",
      "annual_jan2020_secondary_arm_1",
      "annual_feb2020_arm_1",
      "annual_feb2020_secondary_arm_1",
      "annual_mar2020_arm_1",
      "annual_mar2020_secondary_arm_1"
    )
  }

  it should "create a list of arms inclusive of the last date in the range (endTime)" in {
    val startTime =
      OffsetDateTime.of(2020, 3, 2, 0, 0, 0, 0, ZoneOffset.ofHours(-5))
    val endTime =
      OffsetDateTime.of(2020, 4, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

    val envArms =
      EnvironmentExtractionPipeline.extractionArmsGenerator(Some(startTime), Some(endTime))

    envArms shouldBe List(
      "annual_mar2020_arm_1",
      "annual_mar2020_secondary_arm_1",
      "annual_apr2020_arm_1",
      "annual_apr2020_secondary_arm_1"
    )
  }

  it should "fail when startTime is after endTime" in {
    val startTime =
      OffsetDateTime.of(2022, 3, 1, 1, 0, 0, 0, ZoneOffset.ofHours(-5))
    val endTime =
      OffsetDateTime.of(2020, 3, 1, 1, 0, 0, 0, ZoneOffset.ofHours(-5))

    assertThrows[EnvironmentExtractionFailException] {
      EnvironmentExtractionPipeline.extractionArmsGenerator(Some(startTime), Some(endTime))
    }
  }
}
