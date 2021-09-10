package org.broadinstitute.monster.dap

import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import org.broadinstitute.monster.dap.common.HLESurveyTransformationFailException
import org.broadinstitute.monster.dap.hles.HLESTransformationPipeline
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class HlesTransformationPipelineTest extends PipelineSpec with Matchers {

  val expected_hles_dog = Source.fromResource("hles_valid_dog.json").getLines().toSeq
  val expected_hles_owner = Source.fromResource("hles_valid_owner.json").getLines().toSeq

  val expected_hles_health_condition =
    Source.fromResource("hles_valid_health_condition.json").getLines().toSeq

  val expected_hles_cancer_condition =
    Source.fromResource("hles_valid_cancer_condition.json").getLines().toSeq

  it should "process a valid hles record and process all dog data" in {
    val lines =
      Source.fromResource("hles_valid.json").getLines().toSeq

    JobTest[HLESTransformationPipeline.type]
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

    assertThrows[HLESurveyTransformationFailException] {
      JobTest[HLESTransformationPipeline.type]
        .args("--inputPrefix=in", "--outputPrefix=out")
        .input(TextIO("in/records/*.json"), lines)
        .output(TextIO("out/hles_dog"))(col => col should beEmpty)
        .output(TextIO("out/hles_owner"))(col => col should beEmpty)
        .output(TextIO("out/hles_health_condition"))(col =>
          col should containInAnyOrder(expected_hles_health_condition)
        )
        .output(TextIO("out/hles_cancer_condition"))(col =>
          col should containInAnyOrder(expected_hles_cancer_condition)
        )
        .run()
    }
  }

  it should "not process cancer conditions if not specified" in {
    val lines =
      Source.fromResource("hles_no_cancer.json").getLines().toSeq
    val expected_hles_dog_no_cancer =
      Source.fromResource("hles_no_cancer_dog.json").getLines().toSeq

    JobTest[HLESTransformationPipeline.type]
      .args("--inputPrefix=in", "--outputPrefix=out")
      .input(TextIO("in/records/*.json"), lines)
      .output(TextIO("out/hles_dog"))(col =>
        col should containInAnyOrder(expected_hles_dog_no_cancer)
      )
      .output(TextIO("out/hles_owner"))(col => col should containInAnyOrder(expected_hles_owner))
      .output(TextIO("out/hles_health_condition"))(col =>
        col should containInAnyOrder(expected_hles_health_condition)
      )
      .output(TextIO("out/hles_cancer_condition"))(col => col should beEmpty)
      .run()
  }
}
