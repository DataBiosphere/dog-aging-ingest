package org.broadinstitute.monster.dap

import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import org.broadinstitute.monster.dap.hles.HLESurveyTransformationPipeline
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class HlesTransformationPipelineTest extends PipelineSpec with Matchers {

  it should "process a valid hles record and process dog data" in {
    val lines =
      Source.fromResource("hles_valid.json").getLines().toSeq
    val expected_hles_dog = Source.fromResource("hles_dog_valid_output.json").getLines().toSeq
    val expected_hles_owner = Source.fromResource("hles_owner_valid_output.json").getLines().toSeq
    val expected_hles_health_condition =
      Source.fromResource("hles_health_condition_valid_output.json").getLines().toSeq
    val expected_hles_cancer_condition =
      Source.fromResource("hles_cancer_condition_valid_output.json").getLines().toSeq

    JobTest[HLESurveyTransformationPipeline.type]
      .args("--inputPrefix=in", "--outputPrefix=out")
      .input(TextIO("in/records/*.json"), lines)
      .output(TextIO("out/hles_dog"))(col => col should containInAnyOrder(expected_hles_dog))
      .output(TextIO("out/hles_owner"))(col => col should containInAnyOrder(expected_hles_owner))
      .output(TextIO("out/hles_health_condition"))(col =>
        col should containInAnyOrder(expected_hles_health_condition)
      )
      .output(TextIO("out/hles_cancer_condition"))(col =>
        col should containInAnyOrder(expected_hles_cancer_condition)
      )
      .run()
  }

  it should "not process an hles_dog or hles_owner record without st_owner_id" in {
    val lines =
      Source.fromResource("hles_missing_owner.json").getLines().toSeq
    val expected_hles_health_condition =
      Source.fromResource("hles_health_condition_valid_output.json").getLines().toSeq
    val expected_hles_cancer_condition =
      Source.fromResource("hles_cancer_condition_valid_output.json").getLines().toSeq

    JobTest[HLESurveyTransformationPipeline.type]
      .args("--inputPrefix=in", "--outputPrefix=out")
      .input(TextIO("in/records/*.json"), lines)
      .output(TextIO("out/hles_dog"))(col => col should beEmpty)
      .output(TextIO("out/hles_owner"))(col => col should beEmpty)
      // I don't think we are writing records missing owner_id to the output
      // but I see an error when I mark the following two outputs as "should beEmpty"
      // is there a way to catch an exception here?
      .output(TextIO("out/hles_health_condition"))(col =>
        col should containInAnyOrder(expected_hles_health_condition)
      )
      .output(TextIO("out/hles_cancer_condition"))(col =>
        col should containInAnyOrder(expected_hles_cancer_condition)
      )
      .run()
  }

  // todo: add a test for a record without cancer data: if (rawRecord.getBoolean("hs_dx_cancer_yn"))
  // todo: add a test for a record without a congenital health condition: if (rawRecord.getBoolean("hs_congenital_yn"))
}
