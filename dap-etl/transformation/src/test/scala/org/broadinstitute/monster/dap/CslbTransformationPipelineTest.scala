package org.broadinstitute.monster.dap

import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.monster.dap.cslb.CslbTransformationPipeline

import scala.io.Source

class CslbTransformationPipelineTest extends PipelineSpec with Matchers {

  it should "process a valid cslb record" in {
    val lines =
      Source.fromResource("cslb_valid.json").getLines().toSeq
    val expected = Source.fromResource("cslb_valid_output.json").getLines().toSeq
    JobTest[CslbTransformationPipeline.type]
      .args("--inputPrefix=in", "--outputPrefix=out")
      .input(TextIO("in/records/*.json"), lines)
      .output(TextIO("out/cslb"))(col => col should containInAnyOrder(expected))
      .run()
  }

  it should "not process a cslb record without cslb_date" in {
    val lines = Source.fromResource("cslb_missing_date.json").getLines().toSeq
    JobTest[CslbTransformationPipeline.type]
      .args("--inputPrefix=in", "--outputPrefix=out")
      .input(TextIO("in/records/*.json"), lines)
      .output(TextIO("out/cslb"))(col => col should beEmpty)
      .run()
  }
}
