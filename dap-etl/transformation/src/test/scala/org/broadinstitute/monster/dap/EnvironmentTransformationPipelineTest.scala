package org.broadinstitute.monster.dap

import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.broadinstitute.monster.dap.environment.EnvironmentTransformationPipeline
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class EnvironmentTransformationPipelineTest extends PipelineSpec with Matchers {

  it should "process a valid environment record and process data" in {
    val lines =
      Source.fromResource("environment_valid.json").getLines().toSeq
    val expected_environment = Source.fromResource("environment_valid_output.json").getLines().toSeq

    JobTest[EnvironmentTransformationPipeline.type]
      .args("--inputPrefix=in", "--outputPrefix=out")
      .input(TextIO("in/records/*.json"), lines)
      .output(TextIO("out/environment"))(col => col should containInAnyOrder(expected_environment))
      .run()
  }

  it should "not process a environment record without a valid redcap_event_name" in {
    val lines =
      Source.fromResource("environment_missing_event.json").getLines().toSeq
    val expected_environment = Source.fromResource("environment_valid_output.json").getLines().toSeq

    assertThrows[PipelineExecutionException] {
      JobTest[EnvironmentTransformationPipeline.type]
        .args("--inputPrefix=in", "--outputPrefix=out")
        .input(TextIO("in/records/*.json"), lines)
        .output(TextIO("out/environment"))(col => col should containInAnyOrder(expected_environment))
        .run()
    }
  }
}
