package org.broadinstitute.monster.dap.environment

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec
import org.broadinstitute.monster.dap.common._
import org.broadinstitute.monster.dap.common

import java.time.{LocalDate, LocalTime, OffsetDateTime, ZoneOffset}

class EnvironmentExtractionPipelineBuilderIntegrationSpec extends PipelineBuilderSpec[Args] {
  val outputDir = File.newTemporaryDirectory()
  val envOutputDir = File(outputDir, EnvironmentExtractionPipeline.subdir)
  override def afterAll(): Unit = outputDir.delete()

  val start =
    OffsetDateTime.of(LocalDate.of(2020, 11, 15), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  val end =
    OffsetDateTime.of(LocalDate.of(2020, 11, 16), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  override val testArgs =
    Args(
      "token-token-bo-boken",
      Some(start),
      Some(end),
      outputDir.pathAsString,
      pullDataDictionaries = false
    )

  val expectedStudyIds = List("some_study_id", "some_other_id")

  val arms =
    EnvironmentExtractionPipeline.extractionArmsGenerator(testArgs.startTime, testArgs.endTime)

  val studyIdsRequest = RedcapRequestGeneratorParams(
    testArgs.apiToken,
    arms,
    GetRecords(
      fields = List("study_id"),
      filters = EnvironmentExtractionPipeline.extractionFiltersGenerator(testArgs)
    )
  )

  val followUpRecords = common.RedcapRequestGeneratorParams(
    testArgs.apiToken,
    arms,
    GetRecords(
      ids = expectedStudyIds,
      fields = EnvironmentExtractionPipeline.fieldList,
      forms = EnvironmentExtractionPipeline.forms
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
          "baseline_complete" -> "2"
        )
      })
    )
  )

  override val builder = EnvironmentExtractionPipeline.buildPipelineWithWrapper(mockWrapper)

  behavior of "EnvironmentSurveyExtractionPipelineBuilder"

  it should "successfully download records from RedCap" in {
    readMsgs(envOutputDir, "records/*.json") shouldNot be(empty)
  }
}
