package org.broadinstitute.monster.dap

import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import org.scalatest.matchers.should.Matchers
import org.broadinstitute.monster.dap.cslb.CslbTransformationPipeline

import scala.io.Source

class CslbTransformationPipelineTest extends PipelineSpec with Matchers {

  it should "do something" in {
    val lines = Source.fromResource("cslb.json").getLines().toSeq
    val expected = Seq("a: 1")
    JobTest[CslbTransformationPipeline.type]
      .args("--inputPrefix=in", "--outputPrefix=out")
      .input(TextIO("in/records/*.json"), lines)
      .output(TextIO("out/cslb"))(col => col should containInAnyOrder(expected))
      .run()
  }
}
