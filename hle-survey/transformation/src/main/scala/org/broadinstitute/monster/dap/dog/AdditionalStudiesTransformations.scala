package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogFutureStudies

object AdditionalStudiesTransformations {

  /**
    * Parse all future-studies health fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapFutureStudies(rawRecord: RawRecord): HlesDogFutureStudies = {
    val init = HlesDogFutureStudies.init()

    val transformations = List(
      mapFutureStudiesFields _
    )

    transformations.foldLeft(init)((acc, f) => f(rawRecord, acc))
  }

  /**
    * Parse primary care vet and participation likelihood fields out of a
    * raw RedCap record, injecting them into a partially-modeled dog record.
    */
  def mapFutureStudiesFields(
    rawRecord: RawRecord,
    dog: HlesDogFutureStudies
  ): HlesDogFutureStudies = {
    val pcVetConsent = rawRecord.getBoolean("fs_pcvet_consent")
    dog.copy(
      fsPrimaryCareVeterinarianExists = rawRecord.getOptionalBoolean("fs_pcvet"),
      fsPrimaryCareVeterinarianConsentShareVemr = rawRecord.getOptionalBoolean("fs_pcvet_consent"),
      fsPrimaryCareVeterinarianCanProvideEmail =
        if (pcVetConsent) rawRecord.getOptionalBoolean("fs_pcvet_email_yn") else None,
      fsPrimaryCareVeterinarianState =
        if (pcVetConsent) rawRecord.getOptional("fs_pcvet_st") else None,
      fsPrimaryCareVeterinarianZip =
        if (pcVetConsent) rawRecord.getOptional("fs_pcvet_zip") else None,
      fsFutureStudiesParticipationLikelihood = rawRecord.getOptionalNumber("fs_future_studies"),
      fsPhenotypeVsLifespanParticipationLikelihood =
        if (pcVetConsent) rawRecord.getOptionalNumber("fs_pc_ppf_lifespan") else None,
      fsGenotypeVsLifespanParticipationLikelihood =
        if (pcVetConsent) rawRecord.getOptionalNumber("fs_gene_lifespan") else None,
      fsMedicallySlowedAgingParticipationLikelihood =
        if (pcVetConsent) rawRecord.getOptionalNumber("fs_med_aging") else None
    )
  }
}
