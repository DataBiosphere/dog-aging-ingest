package org.broadinstitute.monster.dap

import java.time.OffsetDateTime

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec
import upack._

import scala.collection.mutable

object HLESurveyExtractionPipelineBuilderSpec {
  import HLESurveyExtractionPipelineBuilder.ExtractedForms

  val token = "pls-let-me-in"
  val start = OffsetDateTime.now()
  val end = start.plusDays(3).plusHours(10).minusSeconds(100)

  val fakeIds = 1 to 7

  val initQuery = GetRecords(
    start = Some(start),
    end = Some(end),
    fields = List("study_id"),
    filters = Map("co_consent" -> "1")
  ): RedcapRequest

  val downloadRecords = fakeIds.map { i =>
    GetRecords(
      ids = List(i.toString),
      forms = ExtractedForms
    ): RedcapRequest
  }

  val expectedRecords = fakeIds.map { i =>
    Obj(
      Str("value") -> Str(i.toString),
      Str("some_attribute") -> Str(s"This is the ${i}th attribute"),
      Str("excitement_level") -> Str("a" * i + "!")
    )
  }

  val downloadDataDictionary =
    ExtractedForms.map(instrument => GetDataDictionary(instrument): RedcapRequest)

  val expectedDataDictionary = ExtractedForms.map(i => Obj(Str("value") -> Str(i)): Msg)

  val mockClient = new MockRedCapClient(
    token,
    downloadRecords.zip(expectedRecords.map(Arr(_))).toMap + (
      initQuery -> new Arr(
        fakeIds
          .map(i => Obj(Str("value") -> Str(i.toString)): Msg)
          .to[mutable.ArrayBuffer]
      )
    ) ++ downloadDataDictionary.zip(expectedDataDictionary.map(Arr(_))).toMap
  )
}

class HLESurveyExtractionPipelineBuilderSpec extends PipelineBuilderSpec[Args] {
  import HLESurveyExtractionPipelineBuilderSpec._

  val outputDir = File.newTemporaryDirectory()
  override def afterAll(): Unit = outputDir.delete()

  override val testArgs = Args(
    apiToken = token,
    startTime = Some(start),
    endTime = Some(end),
    outputPrefix = outputDir.pathAsString
  )

  override val builder =
    new HLESurveyExtractionPipelineBuilder(idBatchSize = 1, getClient = () => mockClient)

  behavior of "HLESurveyExtractionPipelineBuilder"

  it should "query RedCap for records correctly" in {
    mockClient.recordedRequests.toSet should contain allElementsOf (Set(initQuery)
      .union(downloadRecords.toSet))
  }

  it should "query RedCap for data dictionaries correctly" in {
    mockClient.recordedRequests.toSet should contain allElementsOf (downloadDataDictionary.toSet)
  }

  it should "write downloaded records to disk" in {
    readMsgs(outputDir, "records/*.json") shouldBe expectedRecords.toSet
  }

  it should "write downloaded data dictionaries to disk" in {
    readMsgs(outputDir, "data_dictionaries/*.json") shouldBe expectedDataDictionary.toSet
  }
}
