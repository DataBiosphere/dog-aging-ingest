package org.broadinstitute.monster.dap

import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import org.broadinstitute.monster.dap.eols.EolsTransformationPipeline
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class EolsTransformationPipelineTest extends PipelineSpec with Matchers {

  it should "process a valid eols record" in {
    val lines =
      Source.fromResource("eols_valid.json").getLines().toSeq
    val expected = Source.fromResource("eols_valid_output.json").getLines().toSeq
    JobTest[EolsTransformationPipeline.type]
      .args("--inputPrefix=in", "--outputPrefix=out")
      .input(TextIO("in/records/*.json"), lines)
      .output(TextIO("out/eols"))(col => col should containInAnyOrder(expected))
      .run()
  }

  it should "not process an eols record without consent" in {
    val lines = Source.fromResource("eols_not_willing.json").getLines().toSeq
    JobTest[EolsTransformationPipeline.type]
      .args("--inputPrefix=in", "--outputPrefix=out")
      .input(TextIO("in/records/*.json"), lines)
      .output(TextIO("out/eols"))(col => col should beEmpty)
      .run()
  }
}
