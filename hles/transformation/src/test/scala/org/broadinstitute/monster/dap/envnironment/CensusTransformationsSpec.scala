package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CensusTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {

  behavior of "CensusTransformations"

  it should "map census variables where complete" in {
    val censusData = Map(
      "" -> Array(""),
      "cv_summary_est" -> Array(""),
      "cv_area_sqmi" -> Array(""),
      "cv_popdensity" -> Array(""),
      "cv_pctnothispanicwhite" -> Array(""),
      "cv_pctnothispanicblack" -> Array(""),
      "cv_pctnothispanicaian" -> Array(""),
      "cv_pctnothispanicasian" -> Array(""),
      "cv_pctnothispanicnhpi" -> Array(""),
      "cv_pctnothispanicother" -> Array(""),
      "cv_pctnothispanictwoormore" -> Array(""),
      "cv_pcthispanic" -> Array(""),
      "cv_pctfemale" -> Array(""),
      "cv_medianincomee" -> Array(""),
      "cv_ginie" -> Array(""),
      "cv_pctbelow125povline" -> Array(""),
      "cv_pctjobless16to64mf" -> Array(""),
      "cv_pctfamsownchildfemaleled" -> Array(""),
      "cv_pctlessthanba" -> Array(""),
      "cv_pctlessthan100k" -> Array(""),
      "cv_disadvantageindex" -> Array(""),
      "cv_pctsamehouse1yrago" -> Array(""),
      "cv_pctowneroccupied" -> Array(""),
      "cv_pctborninus" -> Array(""),
      "cv_stabilityindex" -> Array("")
    )

    val censusDataMapped = AdditionalStudiesTransformations.mapFutureStudies(
      RawRecord(1, censusData)
    )

    // output of a record with primary care vet consent
    censusDataMapped.fsPrimaryCareVeterinarianExists.value shouldBe true
    censusDataMapped.fsPrimaryCareVeterinarianConsentShareVemr.value shouldBe true
    censusDataMapped.fsPrimaryCareVeterinarianCanProvideEmail.value shouldBe true
    censusDataMapped.fsPrimaryCareVeterinarianState.value shouldBe "MA"
    censusDataMapped.fsPrimaryCareVeterinarianZip.value shouldBe "02062-4444"
    censusDataMapped.fsFutureStudiesParticipationLikelihood.value shouldBe 88L
    censusDataMapped.fsPhenotypeVsLifespanParticipationLikelihood.value shouldBe 87L
    censusDataMapped.fsGenotypeVsLifespanParticipationLikelihood.value shouldBe 86L
    censusDataMapped.fsMedicallySlowedAgingParticipationLikelihood.value shouldBe 85L
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
