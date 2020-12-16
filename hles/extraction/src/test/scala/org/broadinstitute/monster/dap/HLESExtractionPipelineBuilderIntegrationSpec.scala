package org.broadinstitute.monster.dap

import java.time.{LocalDate, LocalTime, OffsetDateTime, ZoneOffset}

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec

class HLESExtractionPipelineBuilderIntegrationSpec extends PipelineBuilderSpec[Args] {
  val outputDir = File.newTemporaryDirectory()
  val hlesOutputDir = File(outputDir, HLESurveyExtractionPipeline.subdir)
  override def afterAll(): Unit = outputDir.delete()

  val start =
    OffsetDateTime.of(LocalDate.of(2020, 2, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  val end =
    OffsetDateTime.of(LocalDate.of(2020, 2, 2), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  override val testArgs =
    Args(
      "banana-fana-fo-foken",
      Some(start),
      Some(end),
      outputDir.pathAsString,
      pullDataDictionaries = false
    )

  val expectedStudyIds = List("id_one", "id_two")

  val studyIdsRequest = RedcapRequestGeneratorParams(
    testArgs.apiToken,
    HLESurveyExtractionPipeline.arm,
    GetRecords(
      fields = List("study_id"),
      start = Some(start),
      end = Some(end),
      filters = HLESurveyExtractionPipeline.extractionFilters
    )
  )

  val followUpRecords = RedcapRequestGeneratorParams(
    testArgs.apiToken,
    HLESurveyExtractionPipeline.arm,
    GetRecords(
      ids = expectedStudyIds,
      fields = HLESurveyExtractionPipeline.fieldList,
      forms = HLESurveyExtractionPipeline.forms
    )
  )

  val mockWrapper = new MockOkWrapper(
    Map(
      studyIdsRequest -> RedcapMsgGenerator.redcapifyRecords(expectedStudyIds.map { i =>
        Map("study_id" -> i.toString)
      }),
      followUpRecords -> RedcapMsgGenerator.redcapifyRecords(expectedStudyIds.map { _ =>
        Map(
          "st_dap_pack_count" -> "2",
          "co_consent" -> "1",
          "st_dap_pack_date" -> "2019-04-04"
        )
      })
    )
  )

  override val builder =
    HLESurveyExtractionPipeline.buildPipelineWithWrapper(mockWrapper)

  behavior of "HLESurveyExtractionPipelineBuilder"

  it should "successfully download records from RedCap" in {
    readMsgs(hlesOutputDir, "records/*.json") shouldNot be(empty)
  }
}
