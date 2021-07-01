package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IllnessTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {

  behavior of "IllnessTransformations"

  it should "map illnesses caused by cancer" in {
    val cancerData = Map(
      "eol_illness_a" -> Array("1"),
      "eol_cancer_a" -> Array("1", "12", "21", "34", "98"),
      "eol_cancer_a_specify" -> Array("Some other type of cancer"),
      "eol_cancer_b" -> Array("1"),
      "eol_cancer_b_specify" -> Array("New type of cancer"),
      "eol_illness_b" -> Array("0"),
      "eol_illness_c" -> Array("7")
    )

    val cancerMapped = IllnessTransformations.mapIllnessFields(
      RawRecord(1, cancerData)
    )

    // output of the example record's illness transformations
    cancerMapped.eolIllnessType shouldBe Some(1L)
    cancerMapped.eolIllnessCancerAdrenal shouldBe Some(true)
    cancerMapped.eolIllnessCancerGallbladder shouldBe Some(true)
    cancerMapped.eolIllnessCancerOral shouldBe Some(true)
    cancerMapped.eolIllnessCancerVenereal shouldBe Some(true)
    cancerMapped.eolIllnessCancerOther shouldBe Some(true)
    cancerMapped.eolIllnessCancerOtherDescription shouldBe Some("Some other type of cancer")
    cancerMapped.eolIlllnessCancerNameKnown shouldBe Some(1L)
    cancerMapped.eolIllnessCancerNameDescription shouldBe Some("New type of cancer")
    cancerMapped.eolIllnessAwarenessTimeframe shouldBe Some(0L)
    cancerMapped.eolIllnessTreatment shouldBe Some(7L)
  }

  it should "map illnesses caused by infection" in {
    val infectionData = Map(
      "eol_illness_a" -> Array("2"),
      "eol_infection_a" -> Array("21"),
      "eol_infection_b" -> Array("16"),
      "eol_illness_b" -> Array("3"),
      "eol_illness_c" -> Array("1")
    )

    val infectionMapped = IllnessTransformations.mapIllnessFields(
      RawRecord(1, infectionData)
    )

    // output of the example record's illness transformations
    infectionMapped.eolIllnessType shouldBe Some(2L)
    infectionMapped.eolIllnessInfection shouldBe Some(21L)
    infectionMapped.eolIllnessInfectionSystem shouldBe Some(16L)
    infectionMapped.eolIllnessAwarenessTimeframe shouldBe Some(3L)
    infectionMapped.eolIllnessTreatment shouldBe Some(1L)
  }

  it should "map illnesses caused by other types of infection" in {
    val infectionData = Map(
      "eol_illness_a" -> Array("2"),
      "eol_infection_a" -> Array("98"),
      "eol_infection_a_specify" -> Array("Other type of infection"),
      "eol_infection_b" -> Array("98"),
      "eol_infection_b_specify" -> Array("Other body system"),
      "eol_illness_b" -> Array("4"),
      "eol_illness_c" -> Array("6")
    )

    val infectionMapped = IllnessTransformations.mapIllnessFields(
      RawRecord(1, infectionData)
    )

    // output of the example record's illness transformations
    infectionMapped.eolIllnessType shouldBe Some(2L)
    infectionMapped.eolIllnessInfection shouldBe Some(98L)
    infectionMapped.eolIllnessInfectionOtherDescription shouldBe Some("Other type of infection")
    infectionMapped.eolIllnessInfectionSystem shouldBe Some(98L)
    infectionMapped.eolIllnessInfectionSystemOtherDescription shouldBe Some("Other body system")
    infectionMapped.eolIllnessAwarenessTimeframe shouldBe Some(4L)
    infectionMapped.eolIllnessTreatment shouldBe Some(6L)
  }

  it should "map illnesses caused by other diseases" in {
    val otherIllnessData = Map(
      "eol_illness_a" -> Array("3"),
      "eol_otherillness_a" -> Array("98"),
      "eol_otherillness_a_specify" -> Array("Other type of illness"),
      "eol_otherillness_b" -> Array("1"),
      "eol_otherillness_b_specify" -> Array("Specific diagnosis")
    )

    val otherIllnessMapped = IllnessTransformations.mapIllnessFields(
      RawRecord(1, otherIllnessData)
    )

    // output of the example record's illness transformations
    otherIllnessMapped.eolIllnessType shouldBe Some(3L)
    otherIllnessMapped.eolIllnessOther shouldBe Some(98L)
    otherIllnessMapped.eolIllnessOtherOtherDescription shouldBe Some("Other type of illness")
    otherIllnessMapped.eolIllnessOtherDiagnosis shouldBe Some(true)
    otherIllnessMapped.eolIllnessOtherDiagnosisDescription shouldBe Some("Specific diagnosis")
  }

  it should "map illnesses caused by unknown reasons" in {
    val addIllnessData = Map(
      "eol_illness_a" -> Array("99"),
      "eol_illness_b" -> Array("2"),
      "eol_illness_c" -> Array("98"),
      "eol_illness_c_explain" -> Array("Additional treatment information")
    )

    val addIllnessMapped = IllnessTransformations.mapIllnessFields(
      RawRecord(1, addIllnessData)
    )

    // output of the example record's illness transformations
    addIllnessMapped.eolIllnessType shouldBe Some(99L)
    addIllnessMapped.eolIllnessAwarenessTimeframe shouldBe Some(2L)
    addIllnessMapped.eolIllnessTreatment shouldBe Some(98L)
    addIllnessMapped.eolIllnessTreatmentOtherDescription shouldBe Some(
      "Additional treatment information"
    )
  }
}
