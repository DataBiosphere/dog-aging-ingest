package org.broadinstitute.monster.dap

import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.broadinstitute.monster.common.PipelineCoders
import org.broadinstitute.monster.common.msg.JsonParser
import org.broadinstitute.monster.dap.PostProcess.errCount
import org.scalatest
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import better.files.File
import org.apache.beam.sdk.metrics.MetricName

import scala.io.Source

class HLESurveyTransformationPipelineValidationSpec
    extends AnyFlatSpec
    with Matchers
    with PipelineSpec
    with PipelineCoders
    with BeforeAndAfterAll {
  behavior of "HLESurveyTransformationPipeline"

  val opts: PipelineOptions = PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create()

  // set up a pipeline output directory that we can then clean up
  val outputDir = File.newTemporaryDirectory()
  override def afterAll(): Unit = outputDir.delete()

  def pipelineTest(
    inputPath: String,
    expectedErrorMessage: String,
    count: Int = 1
  ): scalatest.Assertion = {
    val result = runWithRealContext(opts)(ctx =>
      HLESurveyTransformationPipelineBuilder.buildPipeline(
        ctx,
        Args(
          this.getClass.getResource(inputPath).toString,
          outputDir.pathAsString
        )
      )
    ).waitUntilFinish()
    val expectedErrorMessageJson = JsonParser.parseEncodedJson(expectedErrorMessage)

    // make sure logs are correct
    val src = Source.fromFile("../logs/errors.log")
    val lines = src.getLines.toList
    src.close()
    lines.map(err => JsonParser.parseEncodedJson(err)).takeRight(count) should contain(
      expectedErrorMessageJson
    )

    // make sure counter was incremented right number of times
    val errorCountMetric: MetricName = MetricName.named("main", errCount)
    assert(result.allCounters.isDefinedAt(errorCountMetric))
    result.allCounters.foreach {
      case (name, errorCount) =>
        if (name == errorCountMetric) errorCount.attempted shouldBe count
    }

    assertThrows[HLESurveyTransformationFailException](PostProcess.postProcess(result))
  }

  it should "log an error in each place where st_owner_id is None" in {
    val expected =
      """
        |{"errorType":"MissingOwnerIdError",
        |"message":"Record 87616 has less than 1 value for field st_owner_id"}
        |""".stripMargin
    pipelineTest("blank_st_owner_id", expected, 2)
  }
}
