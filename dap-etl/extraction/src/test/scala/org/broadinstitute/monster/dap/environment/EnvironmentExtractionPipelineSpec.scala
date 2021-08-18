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
      "feb2020_arm_1",
      "feb2020_secondary_arm_1",
      "mar2020_arm_1",
      "mar2020_secondary_arm_1",
      "apr2020_arm_1",
      "apr2020_secondary_arm_1",
      "may2020_arm_1",
      "may2020_secondary_arm_1",
      "june2020_arm_1",
      "june2020_secondary_arm_1",
      "july2020_arm_1",
      "july2020_secondary_arm_1",
      "aug2020_arm_1",
      "aug2020_secondary_arm_1",
      "sept2020_arm_1",
      "sept2020_secondary_arm_1",
      "oct2020_arm_1",
      "oct2020_secondary_arm_1",
      "nov2020_arm_1",
      "nov2020_secondary_arm_1",
      "dec2020_arm_1",
      "dec2020_secondary_arm_1",
      "jan2021_arm_1",
      "jan2021_secondary_arm_1"
    )
  }

  it should "use EnvEpoch as startTime when none is provided" in {
    val endTime =
      OffsetDateTime.of(LocalDate.of(2020, 3, 2), LocalTime.MIDNIGHT, ZoneOffset.UTC)

    val envArms = EnvironmentExtractionPipeline.extractionArmsGenerator(None, Some(endTime))

    envArms shouldBe List(
      "jan2020_arm_1",
      "jan2020_secondary_arm_1",
      "feb2020_arm_1",
      "feb2020_secondary_arm_1",
      "mar2020_arm_1",
      "mar2020_secondary_arm_1"
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
      "mar2020_arm_1",
      "mar2020_secondary_arm_1",
      "apr2020_arm_1",
      "apr2020_secondary_arm_1"
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
