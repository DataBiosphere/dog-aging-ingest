package org.broadinstitute.monster.dap

import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import org.broadinstitute.monster.dap.afus.AfusTransformationPipeline
import org.broadinstitute.monster.dap.common.HLESurveyTransformationFailException
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class AfusTransformationPipelineTest extends PipelineSpec with Matchers {

  it should "process a valid AFUS record" in {
    // todo: provide valid test inputs and outputs for dog, health, and cancer tables
    val lines =
      Source.fromResource("afus_valid.json").getLines().toSeq
    val expected_afus_dog = Source.fromResource("afus_valid_dog.json").getLines().toSeq
    val expected_afus_owner = Source.fromResource("afus_valid_owner.json").getLines().toSeq
    val expected_afus_health = Source.fromResource("afus_valid_health.json").getLines().toSeq
    val expected_afus_cancer = Source.fromResource("afus_valid_cancer.json").getLines().toSeq

    JobTest[AfusTransformationPipeline.type]
      .args("--inputPrefix=in", "--outputPrefix=out")
      .input(TextIO("in/records/*.json"), lines)
      .output(TextIO("out/afus_owner"))(col => col should containInAnyOrder(expected_afus_owner))
      .output(TextIO("out/afus_dog"))(col => col should containInAnyOrder(expected_afus_dog))
      .output(TextIO("out/afus_health_condition"))(col =>
        col should containInAnyOrder(expected_afus_health)
      )
      .output(TextIO("out/afus_cancer_condition"))(col =>
        col should containInAnyOrder(expected_afus_cancer)
      )
      .run()
  }

  it should "not process an AFUS record without owner_id" in {
    val lines = Source.fromResource("afus_missing_owner.json").getLines().toSeq

    assertThrows[HLESurveyTransformationFailException] {
      JobTest[AfusTransformationPipeline.type]
        .args("--inputPrefix=in", "--outputPrefix=out")
        .input(TextIO("in/records/*.json"), lines)
        .output(TextIO("out/afus_owner"))(col => col should beEmpty)
        .output(TextIO("out/afus_dog"))(col => col should beEmpty)
        .output(TextIO("out/afus_health_condition"))(col => col should beEmpty)
        .output(TextIO("out/afus_cancer_condition"))(col => col should beEmpty)
        .run()
    }
  }
}
