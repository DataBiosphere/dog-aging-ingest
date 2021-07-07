package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.EolsRecentSymptom

object RecentSymptomsTransformations {

  /**
    * Parse all eol_med_symptoms data out of a raw RedCap record,
    * injecting them into a partially-modeled Eols record.
    */
  def mapRecentSymptoms(rawRecord: RawRecord): EolsRecentSymptom = {
    val recentSymptoms = Some(rawRecord.getArray("eol_med_symptoms").map(_.toLong))
    val otherRecentSymptoms = recentSymptoms.map(_.contains(98L))
    val qolDeclineReason = rawRecord.getOptionalNumber("eol_contribute_qol_decined")
    EolsRecentSymptom(
      eolRecentSymptomNone = recentSymptoms.map(_.contains(0L)),
      eolRecentSymptomLethargy = recentSymptoms.map(_.contains(1L)),
      eolRecentSymptomVomiting = recentSymptoms.map(_.contains(2L)),
      eolRecentSymptomDiarrhea = recentSymptoms.map(_.contains(3L)),
      eolRecentSymptomAppetite = recentSymptoms.map(_.contains(4L)),
      eolRecentSymptomWeight = recentSymptoms.map(_.contains(5L)),
      eolRecentSymptomCoughing = recentSymptoms.map(_.contains(6L)),
      eolRecentSymptomSneezing = recentSymptoms.map(_.contains(7L)),
      eolRecentSymptomBreathing = recentSymptoms.map(_.contains(8L)),
      eolRecentSymptomBleedingBruising = recentSymptoms.map(_.contains(9L)),
      eolRecentSymptomDrinkLot = recentSymptoms.map(_.contains(10L)),
      eolRecentSymptomDrinkLittle = recentSymptoms.map(_.contains(11L)),
      eolRecentSymptomUrinating = recentSymptoms.map(_.contains(12L)),
      eolRecentSymptomIncontinence = recentSymptoms.map(_.contains(13L)),
      eolRecentSymptomSores = recentSymptoms.map(_.contains(14L)),
      eolRecentSymptomSeizures = recentSymptoms.map(_.contains(15L)),
      eolRecentSymptomSwolenAbdomen = recentSymptoms.map(_.contains(16L)),
      eolRecentSymptomOther = otherRecentSymptoms,
      eolRecentSymptomOtherDescription =
        if (otherRecentSymptoms.contains(true))
          rawRecord.getOptionalStripped("eol_med_symptoms_specify")
        else None,
      eolRecentQol = rawRecord.getOptionalNumber("eol_qol"),
      eolQolDeclined = rawRecord.getOptionalNumber("eol_qol_declined"),
      eolQolDeclinedReason = qolDeclineReason,
      eolQolDeclinedReasonOtherDescription =
        if (qolDeclineReason.contains(98L))
          rawRecord.getOptionalStripped("eol_qol_declined_specify")
        else None,
      eolRecentVetDiscuss = rawRecord.getOptionalNumber("eol_vet_discuss_yn"),
      eolRecentVetStay = rawRecord.getOptionalNumber("eol_stayed_at_vet"),
      eolRecentVetStayLength = rawRecord.getOptionalNumber("eol_length_stayed_at_vet"),
      eolRecentSedation = rawRecord.getOptionalNumber("eol_sedation_yn"),
      eolUnderstandPrognosis = rawRecord.getOptionalNumber("eol_understand_prognosis")
    )
  }
}
