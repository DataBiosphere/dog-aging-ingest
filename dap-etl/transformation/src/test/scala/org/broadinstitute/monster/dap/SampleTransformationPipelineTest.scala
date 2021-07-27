package org.broadinstitute.monster.dap

import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import org.broadinstitute.monster.dap.sample.SampleTransformationPipeline
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class SampleTransformationPipelineTest extends PipelineSpec with Matchers {

  it should "process a valid sample record" in {
    val lines =
      Source.fromResource("sample_valid.json").getLines().toSeq
    val expected = Source.fromResource("sample_valid_output.json").getLines().toSeq
    JobTest[SampleTransformationPipeline.type]
      .args("--inputPrefix=in", "--outputPrefix=out")
      .input(TextIO("in/records/*.json"), lines)
      .output(TextIO("out/sample"))(col => col should containInAnyOrder(expected))
      .run()
  }

  it should "not process a sample record without k1_rtn_tracking_date" in {
    val lines = Source.fromResource("sample_missing_return_date.json").getLines().toSeq
    JobTest[SampleTransformationPipeline.type]
      .args("--inputPrefix=in", "--outputPrefix=out")
      .input(TextIO("in/records/*.json"), lines)
      .output(TextIO("out/sample"))(col => col should beEmpty)
      .run()
  }
}
