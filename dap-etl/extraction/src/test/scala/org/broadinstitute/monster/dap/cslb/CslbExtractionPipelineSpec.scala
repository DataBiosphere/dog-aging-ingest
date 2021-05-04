package org.broadinstitute.monster.dap.cslb

import java.time.{OffsetDateTime, ZoneOffset}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CslbExtractionPipelineSpec extends AnyFlatSpec with Matchers {
  it should "generate the correct arms when provided startTime and endTime arguments" in {
    val startTime =
      OffsetDateTime.of(2020, 2, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))
    val endTime =
      OffsetDateTime.of(2022, 3, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

    val cslbArms = CslbExtractionPipeline.extractionArmsGenerator(Some(startTime), Some(endTime))

    cslbArms shouldBe List("annual_2020_arm_1", "annual_2021_arm_1", "annual_2022_arm_1")
  }

  it should "use CSLBEpoch as startTime when none is provided" in {
    val endTime =
      OffsetDateTime.of(2022, 3, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))
    val cslbArms = CslbExtractionPipeline.extractionArmsGenerator(None, Some(endTime))

    cslbArms shouldBe List("annual_2020_arm_1", "annual_2021_arm_1", "annual_2022_arm_1")
  }

  it should "use current date as the endTime when none is provided" in {
    val startTime =
      OffsetDateTime.of(2020, 3, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

    val cslbArms = CslbExtractionPipeline.extractionArmsGenerator(Some(startTime), None)

    cslbArms shouldBe List("annual_2020_arm_1", "annual_2021_arm_1")
  }

  it should "create a list of arms inclusive of the last date in the range (endTime)" in {
    val startTime =
      OffsetDateTime.of(2020, 3, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))
    val endTime =
      OffsetDateTime.of(2022, 1, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

    val cslbArms = CslbExtractionPipeline.extractionArmsGenerator(Some(startTime), Some(endTime))

    cslbArms shouldBe List("annual_2020_arm_1", "annual_2021_arm_1", "annual_2022_arm_1")
  }

  it should "fail when startTime is after endTime" in {
    val startTime =
      OffsetDateTime.of(2022, 3, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))
    val endTime =
      OffsetDateTime.of(2020, 3, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

    assertThrows[CslbExtractionFailException] {
      CslbExtractionPipeline.extractionArmsGenerator(Some(startTime), Some(endTime))
    }
  }
}
