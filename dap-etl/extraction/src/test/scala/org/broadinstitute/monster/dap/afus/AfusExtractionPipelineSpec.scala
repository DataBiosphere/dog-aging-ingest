package org.broadinstitute.monster.dap.afus

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{OffsetDateTime, ZoneOffset}

class AfusExtractionPipelineSpec extends AnyFlatSpec with Matchers {
  it should "generate the correct arms when provided startTime and endTime arguments" in {
    val startTime =
      OffsetDateTime.of(2021, 10, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))
    val endTime =
      OffsetDateTime.of(2023, 10, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

    val afusArms = AfusExtractionPipeline.extractionArmsGenerator(Some(startTime), Some(endTime))

    // todo: update when extraction arm generator logic is updated
    afusArms shouldBe List("fup_1_arm_1", "baseline_arm_1")
    //afusArms shouldBe List("fup_1_arm_1", "fup_2_arm_1", "fup_3_arm_1", "baseline_arm_1")
  }

  it should "use AFUSEpoch as startTime when none is provided" in {
    val endTime =
      OffsetDateTime.of(2022, 12, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))
    val afusArms = AfusExtractionPipeline.extractionArmsGenerator(None, Some(endTime))

    // todo: update when extraction arm generator logic is updated
    afusArms shouldBe List("fup_1_arm_1", "baseline_arm_1")
    //afusArms shouldBe List("fup_1_arm_1", "fup_2_arm_1", "baseline_arm_1")
  }

  it should "fail when startTime is after endTime" in {
    val startTime =
      OffsetDateTime.of(2022, 3, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))
    val endTime =
      OffsetDateTime.of(2020, 3, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

    assertThrows[AfusExtractionFailException] {
      AfusExtractionPipeline.extractionArmsGenerator(Some(startTime), Some(endTime))
    }
  }
}
