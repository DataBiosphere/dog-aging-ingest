package org.broadinstitute.monster.dap

import java.time.OffsetDateTime

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec
import upack._

import scala.collection.mutable

object ExtractionPipelineBuilderSpec {
  val token = "pls-let-me-in"
  val start = OffsetDateTime.now()
  val end = start.plusDays(3).plusHours(10).minusSeconds(100)
  val event = "fake_event_1"

  val fakeIds = 1 to 50
  val forms = List("fake_form_1", "fake_form_2")
  val filters = Map("foo" -> "Bar")

  val initQuery = GetRecords(
    start = Some(start),
    end = Some(end),
    fields = List("study_id"),
    filters = filters
  ): RedcapRequest

  val downloadRecords = fakeIds.map { i =>
    GetRecords(
      ids = List(i.toString),
      forms = forms,
      fields = List("co_consent")
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
    forms.map(instrument => GetDataDictionary(instrument): RedcapRequest)

  val expectedDataDictionary =
    forms.map(i => Obj(Str("value") -> Str(i)): Msg)

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

class ExtractionPipelineBuilderSpec extends PipelineBuilderSpec[Args] {
  import ExtractionPipelineBuilderSpec._

  val outputDir = File.newTemporaryDirectory()

  override def afterAll(): Unit = outputDir.delete()

  override val testArgs = Args(
    apiToken = token,
    startTime = Some(start),
    endTime = Some(end),
    outputPrefix = outputDir.pathAsString
  )

  override val builder =
    new ExtractionPipelineBuilder(
      forms,
      filters,
      event,
      "",
      idBatchSize = 1,
      getClient = _ => mockClient
    )

  behavior of "ExtractionPipelineBuilder"

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
