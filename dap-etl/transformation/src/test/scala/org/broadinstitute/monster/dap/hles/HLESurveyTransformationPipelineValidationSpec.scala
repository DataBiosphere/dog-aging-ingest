package org.broadinstitute.monster.dap.hles

import better.files.File
import com.spotify.scio.ScioResult
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.metrics.MetricName
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.broadinstitute.monster.common.PipelineCoders
import org.broadinstitute.monster.common.msg.JsonParser
import org.broadinstitute.monster.dap.common.PostProcess.errCount
import org.broadinstitute.monster.dap.common.{
  Args,
  HLESurveyTransformationFailException,
  PostProcess
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import upack.Msg

import scala.io.Source

class HLESurveyTransformationPipelineValidationSpec
    extends AnyFlatSpec
    with Matchers
    with PipelineSpec
    with PipelineCoders
    with BeforeAndAfterAll {
  behavior of "HLESurveyTransformationPipeline"

  val opts: PipelineOptions = PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create()
  val errorCountMetric: MetricName = MetricName.named("main", errCount)

  // set up a pipeline output directory that we can then clean up
  val outputDir = File.newTemporaryDirectory()
  override def afterAll(): Unit = outputDir.delete()

  def pipelineTest(
    inputPath: String
  ): ScioResult = {
    runWithRealContext(opts)(ctx =>
      HLESurveyTransformationPipelineBuilder.buildPipeline(
        ctx,
        Args(
          this.getClass.getResource(inputPath).toString,
          outputDir.pathAsString
        )
      )
    ).waitUntilFinish()
  }

  def getParsedErrorJsonFromLog(
    logPath: String,
    expectedErrorCount: Int
  ): Iterable[Msg] = {
    val src = Source.fromFile(logPath)
    val lines = src.getLines.toList
    src.close()
    lines.map(err => JsonParser.parseEncodedJson(err)).takeRight(expectedErrorCount)
  }

  it should "produce one error for each missing st_owner_id" in {
    val result = pipelineTest("blank_st_owner_id")
    result.allCounters(errorCountMetric).attempted shouldBe 2
  }

  it should "produce the expected error message for a missing st_owner_id" in {
    val expectedRawJson =
      """
        |{"errorType":"MissingOwnerIdError",
        |"message":"Record 11111 has less than 1 value for field st_owner_id"}
        |""".stripMargin
    val expectedParsedJson = JsonParser.parseEncodedJson(expectedRawJson)

    pipelineTest("blank_st_owner_id")
    val loggedErrorJson = getParsedErrorJsonFromLog("../logs/errors.log", 2)

    loggedErrorJson should contain(expectedParsedJson)
  }

  it should "fail the pipeline when there is a missing st_owner_id" in {
    val result = pipelineTest("blank_st_owner_id")
    an[HLESurveyTransformationFailException] should be thrownBy PostProcess.postProcess(result)
  }
}
