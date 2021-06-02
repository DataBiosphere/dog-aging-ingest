package org.broadinstitute.monster.dap.sample

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec
import org.broadinstitute.monster.dap.common
import org.broadinstitute.monster.dap.common.{
  Args,
  GetRecords,
  MockOkWrapper,
  RedcapMsgGenerator,
  RedcapRequestGeneratorParams
}

import java.time.{LocalDate, LocalTime, OffsetDateTime, ZoneOffset}

class SampleExtractionPipelineBuilderIntegrationSpec extends PipelineBuilderSpec[Args] {
  val outputDir = File.newTemporaryDirectory()
  val sampleOutputDir = File(outputDir, SampleExtractionPipeline.subdir)
  override def afterAll(): Unit = outputDir.delete()

  val start =
    OffsetDateTime.of(LocalDate.of(2020, 11, 15), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  val end =
    OffsetDateTime.of(LocalDate.of(2020, 11, 16), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  override val testArgs =
    Args(
      "token-foken-lo-token",
      Some(start),
      Some(end),
      outputDir.pathAsString,
      pullDataDictionaries = false
    )

  val expectedStudyIds = List("some_study_id", "some_other_id")

  val studyIdsRequest = RedcapRequestGeneratorParams(
    testArgs.apiToken,
    List(SampleExtractionPipeline.arm),
    GetRecords(
      fields = List("study_id"),
      filters = SampleExtractionPipeline.extractionFiltersGenerator(testArgs)
    )
  )

  val followUpRecords = common.RedcapRequestGeneratorParams(
    testArgs.apiToken,
    List(SampleExtractionPipeline.arm),
    GetRecords(
      ids = expectedStudyIds,
      fields = SampleExtractionPipeline.fieldList,
      forms = SampleExtractionPipeline.forms
    )
  )

  val mockWrapper = new MockOkWrapper(
    Map(
      studyIdsRequest -> RedcapMsgGenerator.toRedcapFormat(expectedStudyIds.map { i =>
        Map(
          "study_id" -> i.toString
        )
      }),
      followUpRecords -> RedcapMsgGenerator.toRedcapFormat(expectedStudyIds.map { _ =>
        Map(
          "ce_enroll_stat" -> "2"
        )
      })
    )
  )

  override val builder = SampleExtractionPipeline.buildPipelineWithWrapper(mockWrapper)

  behavior of "SampleExtractionPipelineBuilder"

  it should "successfully download records from RedCap" in {
    readMsgs(sampleOutputDir, "records/*.json") shouldNot be(empty)
  }
}
