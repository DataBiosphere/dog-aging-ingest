package org.broadinstitute.monster.dap.eols

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

class EolsExtractionPipelineBuilderIntegrationSpec extends PipelineBuilderSpec[Args] {
  val outputDir = File.newTemporaryDirectory()
  val eolsOutputDir = File(outputDir, EolsExtractionPipeline.subdir)
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
    List(EolsExtractionPipeline.arm),
    GetRecords(
      fields = List("study_id"),
      filters = EolsExtractionPipeline.extractionFiltersGenerator(testArgs)
    )
  )

  val followUpRecords = common.RedcapRequestGeneratorParams(
    testArgs.apiToken,
    List(EolsExtractionPipeline.arm),
    GetRecords(
      ids = expectedStudyIds,
      fields = EolsExtractionPipeline.fieldList,
      forms = EolsExtractionPipeline.forms
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
          "eol_willing_to_complete" -> "1"
        )
      })
    )
  )

  override val builder = EolsExtractionPipeline.buildPipelineWithWrapper(mockWrapper)

  behavior of "EOLSExtractionPipelineBuilder"

  it should "successfully download records from RedCap" in {
    readMsgs(eolsOutputDir, "records/*.json") shouldNot be(empty)
  }
}
