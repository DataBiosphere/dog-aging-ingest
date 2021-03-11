package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogFutureStudies

object AdditionalStudiesTransformations {

  /**
    * Parse all future-studies health fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapFutureStudies(rawRecord: RawRecord): HlesDogFutureStudies = {
    val pcVetConsent = rawRecord.getOptionalBoolean("fs_pcvet_consent")
    HlesDogFutureStudies(
      fsPrimaryCareVeterinarianExists = rawRecord.getOptionalBoolean("fs_pcvet"),
      fsPrimaryCareVeterinarianConsentShareVemr = pcVetConsent,
      fsPrimaryCareVeterinarianCanProvideEmail = pcVetConsent.flatMap {
        if (_)
          rawRecord.getOptionalNumber("fs_pcvet_email_yn", permittedValues = Set(1L, 7L, 8L))
        else None
      },
      fsPrimaryCareVeterinarianState = pcVetConsent.flatMap {
        if (_) rawRecord.getOptional("fs_pcvet_st") else None
      },
      fsFutureStudiesParticipationLikelihood = rawRecord.getOptionalNumber("fs_future_studies"),
      fsPhenotypeVsLifespanParticipationLikelihood = pcVetConsent.flatMap {
        if (_) rawRecord.getOptionalNumber("fs_pc_ppf_lifespan") else None
      },
      fsGenotypeVsLifespanParticipationLikelihood = pcVetConsent.flatMap {
        if (_) rawRecord.getOptionalNumber("fs_gene_lifespan") else None
      },
      fsMedicallySlowedAgingParticipationLikelihood = pcVetConsent.flatMap {
        if (_) rawRecord.getOptionalNumber("fs_med_aging") else None
      }
    )
  }
}
