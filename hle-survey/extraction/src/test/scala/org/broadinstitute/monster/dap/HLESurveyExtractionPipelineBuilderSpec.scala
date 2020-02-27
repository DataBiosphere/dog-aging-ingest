package org.broadinstitute.monster.dap

import java.time.OffsetDateTime

import better.files.File
import org.broadinstitute.monster.common.PipelineBuilderSpec
import upack._

import scala.collection.mutable

object HLESurveyExtractionPipelineBuilderSpec {
  import MockRedCapClient.QueryParams

  val token = "pls-let-me-in"
  val start = OffsetDateTime.now()
  val end = start.plusDays(3).plusHours(10).minusSeconds(100)

  val fakeIds = 1 to 50

  val initQuery = QueryParams(
    start = Some(start),
    end = Some(end),
    fields = List("study_id"),
    filters = Map("co_consent" -> "1")
  )

  val downloadQueries = fakeIds.map { i =>
    QueryParams(
      ids = List(i.toString),
      forms = HLESurveyExtractionPipelineBuilder.ExtractedForms
    )
  }

  val expectedOut = fakeIds.map { i =>
    Obj(
      Str("study_id") -> Str(i.toString),
      Str("some_attribute") -> Str(s"This is the ${i}th attribute"),
      Str("excitement_level") -> Str("a" * i + "!")
    )
  }

  val mockClient = new MockRedCapClient(
    token,
    downloadQueries.zip(expectedOut.map(Arr(_))).toMap + (
      initQuery -> new Arr(
        fakeIds
          .map(i => Obj(Str("study_id") -> Str(i.toString)): Msg)
          .to[mutable.ArrayBuffer]
      )
    )
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

  it should "query RedCap in the expected way" in {
    mockClient.recordedRequests.toSet shouldBe Set(initQuery).union(downloadQueries.toSet)
  }

  it should "write downloaded outputs to disk" in {
    readMsgs(outputDir) shouldBe expectedOut.toSet
  }
}
