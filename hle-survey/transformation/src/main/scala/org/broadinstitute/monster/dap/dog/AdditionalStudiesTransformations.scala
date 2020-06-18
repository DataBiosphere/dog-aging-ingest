package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogFutureStudies

object AdditionalStudiesTransformations {

  /**
    * Parse all future-studies health fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapFutureStudies(rawRecord: RawRecord): HlesDogFutureStudies = {
    val pcVetConsent = rawRecord.getBoolean("fs_pcvet_consent")
    HlesDogFutureStudies(
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
