package org.broadinstitute.monster.dap.cslb

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec
import org.broadinstitute.monster.dap.common._
import org.broadinstitute.monster.dap.{CslbExtractionPipeline, common}

import java.time.{LocalDate, LocalTime, OffsetDateTime, ZoneOffset}

class CslbExtractionPipelineBuilderIntegrationSpec extends PipelineBuilderSpec[Args] {
  val outputDir = File.newTemporaryDirectory()
  val cslbOutputDir = File(outputDir, CslbExtractionPipeline.subdir)
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

  val studyIdsRequest = RedcapRequestGeneratorParams(
    testArgs.apiToken,
    CslbExtractionPipeline.arm,
    GetRecords(
      fields = List("study_id"),
      filters = CslbExtractionPipeline.extractionFiltersGenerator(testArgs)
    )
  )

  val followUpRecords = common.RedcapRequestGeneratorParams(
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
      studyIdsRequest -> RedcapMsgGenerator.toRedcapFormat(expectedStudyIds.map { i =>
        Map(
          "study_id" -> i.toString
        )
      }),
      followUpRecords -> RedcapMsgGenerator.toRedcapFormat(expectedStudyIds.map { _ =>
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
