package org.broadinstitute.monster.dap.afus

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec
import org.broadinstitute.monster.dap.common
import org.broadinstitute.monster.dap.common._

import java.time.{LocalDate, LocalTime, OffsetDateTime, ZoneOffset}

class AfusExtractionPipelineBuilderIntegrationSpec extends PipelineBuilderSpec[Args] {
  val outputDir = File.newTemporaryDirectory()
  val afusOutputDir = File(outputDir, AfusExtractionPipeline.subdir)
  override def afterAll(): Unit = outputDir.delete()

  val start =
    OffsetDateTime.of(LocalDate.of(2021, 11, 15), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  val end =
    OffsetDateTime.of(LocalDate.of(2022, 5, 16), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  override val testArgs =
    Args(
      "token-token-bo-boken",
      Some(start),
      Some(end),
      outputDir.pathAsString,
      pullDataDictionaries = false
    )

  val expectedStudyIds = List("some_owner_id", "some_other_id")

  val arms = AfusExtractionPipeline.extractionArmsGenerator(testArgs.startTime, testArgs.endTime)

  val studyIdsRequest = RedcapRequestGeneratorParams(
    testArgs.apiToken,
    arms,
    GetRecords(
      fields = List("st_owner_id"),
      filters = AfusExtractionPipeline.extractionFiltersGenerator(testArgs)
    )
  )

  val followUpRecords = common.RedcapRequestGeneratorParams(
    testArgs.apiToken,
    arms,
    GetRecords(
      ids = expectedStudyIds,
      fields = AfusExtractionPipeline.fieldList,
      forms = AfusExtractionPipeline.forms
    )
  )

  val mockWrapper = new MockOkWrapper(
    Map(
      studyIdsRequest -> RedcapMsgGenerator.toRedcapFormat(expectedStudyIds.map { i =>
        Map(
          "st_owner_id" -> i.toString
        )
      }),
      followUpRecords -> RedcapMsgGenerator.toRedcapFormat(expectedStudyIds.map { _ =>
        Map(
          "fu_is_completed" -> "1"
        )
      })
    )
  )

  override val builder = AfusExtractionPipeline.buildPipelineWithWrapper(mockWrapper)

  behavior of "AfusSurveyExtractionPipelineBuilder"

  it should "successfully download records from RedCap" in {
    readMsgs(afusOutputDir, "records/*.json") shouldNot be(empty)
  }
}
