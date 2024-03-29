package org.broadinstitute.monster.dap.hles

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec
import org.broadinstitute.monster.dap.common._
import org.broadinstitute.monster.dap.common
import upack._

import java.time.{LocalDate, LocalTime, OffsetDateTime, ZoneOffset}

class HLESExtractionPipelineBuilderIntegrationSpec extends PipelineBuilderSpec[Args] {
  val outputDir = File.newTemporaryDirectory()
  val hlesOutputDir = File(outputDir, HLESExtractionPipeline.subdir)
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

  val fakeIds = 0 to 1
  val expectedStudyIds = fakeIds.map { id => s"id_${id}" }.toList

  val studyIdsRequest = RedcapRequestGeneratorParams(
    testArgs.apiToken,
    List(HLESExtractionPipeline.arm),
    GetRecords(
      fields = List("study_id"),
      filters = HLESExtractionPipeline.extractionFiltersGenerator(testArgs)
    )
  )

  val followUpRecords = common.RedcapRequestGeneratorParams(
    testArgs.apiToken,
    List(HLESExtractionPipeline.arm),
    GetRecords(
      ids = expectedStudyIds,
      fields = HLESExtractionPipeline.fieldList,
      forms = HLESExtractionPipeline.forms
    )
  )

  val mockWrapper = new MockOkWrapper(
    Map(
      studyIdsRequest -> RedcapMsgGenerator.toRedcapFormat(expectedStudyIds.map { i =>
        Map("study_id" -> i.toString)
      }),
      followUpRecords -> RedcapMsgGenerator.toRedcapFormat(expectedStudyIds.map { _ =>
        Map(
          "st_dap_pack_count" -> "2",
          "co_consent" -> "1",
          "st_dap_pack_date" -> "2019-04-04"
        )
      })
    )
  )

  override val builder =
    HLESExtractionPipeline.buildPipelineWithWrapper(mockWrapper)

  behavior of "HLESurveyExtractionPipelineBuilder"

  it should "successfully download records from RedCap" in {
    readMsgs(hlesOutputDir, "records/*.json") shouldNot be(empty)
  }

  val expectedPackDateRecord: Seq[Obj] = fakeIds.map { i =>
    Obj(
      Str("record") -> Str(i.toString),
      Str("field_name") -> Str("st_dap_pack_date"),
      Str("value") -> Str("2019-04-04")
    )
  }

  val expectedPackCountRecord: Seq[Obj] = fakeIds.map { i =>
    Obj(
      Str("record") -> Str(i.toString),
      Str("field_name") -> Str("st_dap_pack_count"),
      Str("value") -> Str("2")
    )
  }

  val expectedConsentRecord: Seq[Obj] = fakeIds.map { i =>
    Obj(
      Str("record") -> Str(i.toString),
      Str("field_name") -> Str("co_consent"),
      Str("value") -> Str("1")
    )
  }
  val expectedRecords = expectedPackCountRecord ++ expectedPackDateRecord ++ expectedConsentRecord

  it should "contain all expected fields" in {
    val allRecords = readMsgs(hlesOutputDir, "records/*.json")
    allRecords shouldBe expectedRecords.toSet
  }

}
