package org.broadinstitute.monster.dap

import java.time.{LocalDate, LocalTime, OffsetDateTime, ZoneOffset}

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec

class CslbExtractionPipelineBuilderIntegrationSpec extends PipelineBuilderSpec[Args] {
  val outputDir = File.newTemporaryDirectory()
  val cslbOutputDir = File(outputDir, CslbExtractionPipeline.subdir)
  override def afterAll(): Unit = outputDir.delete()

  // We know that some records were updated in this time range, so it should
  // be fine to pull consistently without worrying about changes in data size.
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

  val studyIdsRequest = RedcapRequestGeneratorParams(
    testArgs.apiToken,
    CslbExtractionPipeline.arm,
    GetRecords(
      fields = List("study_id"),
      start = Some(start),
      end = Some(end),
      filters = CslbExtractionPipeline.extractionFilters
    )
  )

  val followUpRecords = RedcapRequestGeneratorParams(
    testArgs.apiToken,
    CslbExtractionPipeline.arm,
    GetRecords(
      ids = expectedStudyIds,
      fields = CslbExtractionPipeline.fieldList,
      forms = CslbExtractionPipeline.forms
    )
  )

  val mockWrapper = new MockOkWrapper(
    Map(
      studyIdsRequest -> RedcapMsgGenerator.redcapifyRecords(expectedStudyIds.map { i =>
        Map(
          "study_id" -> i.toString
        )
      }),
      followUpRecords -> RedcapMsgGenerator.redcapifyRecords(expectedStudyIds.map { _ =>
        Map(
          "canine_social_and_learned_behavior_complete" -> "2"
        )
      })
    )
  )

  override val builder = CslbExtractionPipeline.buildPipelineWithWrapper(mockWrapper)

  behavior of "CslbSurveyExtractionPipelineBuilder"

  it should "successfully download records from RedCap" in {
    readMsgs(cslbOutputDir, "records/*.json") shouldNot be(empty)
  }
}
