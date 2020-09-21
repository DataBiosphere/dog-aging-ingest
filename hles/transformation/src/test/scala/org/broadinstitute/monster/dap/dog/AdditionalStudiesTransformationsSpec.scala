package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AdditionalStudiesTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {

  behavior of "AdditionalStatusTransformations"

  it should "map future-studies info with consent" in {
    val hasConsentExample = Map(
      "fs_pcvet" -> Array("1"),
      "fs_pcvet_consent" -> Array("1"),
      "fs_pcvet_email_yn" -> Array("1"),
      "fs_pcvet_st" -> Array("MA"),
      "fs_pcvet_zip" -> Array("02062-4444"),
      "fs_future_studies" -> Array("88"),
      "fs_pc_ppf_lifespan" -> Array("87"),
      "fs_gene_lifespan" -> Array("86"),
      "fs_med_aging" -> Array("85")
    )

    val hasConsentOut = AdditionalStudiesTransformations.mapFutureStudies(
      RawRecord(1, hasConsentExample)
    )

    // output of a record with primary care vet consent
    hasConsentOut.fsPrimaryCareVeterinarianExists.value shouldBe true
    hasConsentOut.fsPrimaryCareVeterinarianConsentShareVemr.value shouldBe true
    hasConsentOut.fsPrimaryCareVeterinarianCanProvideEmail.value shouldBe true
    hasConsentOut.fsPrimaryCareVeterinarianState.value shouldBe "MA"
    hasConsentOut.fsPrimaryCareVeterinarianZip.value shouldBe "02062-4444"
    hasConsentOut.fsFutureStudiesParticipationLikelihood.value shouldBe 88L
    hasConsentOut.fsPhenotypeVsLifespanParticipationLikelihood.value shouldBe 87L
    hasConsentOut.fsGenotypeVsLifespanParticipationLikelihood.value shouldBe 86L
    hasConsentOut.fsMedicallySlowedAgingParticipationLikelihood.value shouldBe 85L
  }

  it should "not map future-studies info without consent" in {
    val lacksConsentExample = Map(
      "fs_pcvet" -> Array("1"),
      "fs_pcvet_consent" -> Array("2"),
      "fs_pcvet_email_yn" -> Array("1"),
      "fs_pcvet_st" -> Array("MA"),
      "fs_pcvet_zip" -> Array("02062-4444"),
      "fs_future_studies" -> Array("3"),
      "fs_pc_ppf_lifespan" -> Array("2"),
      "fs_gene_lifespan" -> Array("5"),
      "fs_med_aging" -> Array("11")
    )

    val lacksConsentOut = AdditionalStudiesTransformations.mapFutureStudies(
      RawRecord(1, lacksConsentExample)
    )

    // output of a record without primary care vet consent
    lacksConsentOut.fsPrimaryCareVeterinarianExists.value shouldBe true
    lacksConsentOut.fsPrimaryCareVeterinarianConsentShareVemr.value shouldBe false
    lacksConsentOut.fsPrimaryCareVeterinarianCanProvideEmail shouldBe None
    lacksConsentOut.fsPrimaryCareVeterinarianState shouldBe None
    lacksConsentOut.fsPrimaryCareVeterinarianZip shouldBe None
    lacksConsentOut.fsFutureStudiesParticipationLikelihood.value shouldBe 3L
    lacksConsentOut.fsPhenotypeVsLifespanParticipationLikelihood shouldBe None
    lacksConsentOut.fsGenotypeVsLifespanParticipationLikelihood shouldBe None
    lacksConsentOut.fsMedicallySlowedAgingParticipationLikelihood shouldBe None
  }
}