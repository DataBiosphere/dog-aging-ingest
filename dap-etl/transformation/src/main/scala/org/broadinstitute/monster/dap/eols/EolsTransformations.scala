package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.Eols
import com.spotify.scio.ScioMetrics.counter

object EolsTransformations {

  /** Parse all eols related fields out of a RedCap record.
    * The schema for eols variables has been separated into 6 jade-fragments.
    */
  def mapEols(rawRecord: RawRecord): Option[Eols] = {
    val willingToComplete = rawRecord.getRequiredBoolean("eol_willing_to_complete")
    if (willingToComplete) {
      Some(
        Eols(
          dogId = rawRecord.id,
          eolWillingToComplete = Some(willingToComplete),
          eolExactDeathDateKnown = rawRecord.getOptionalBoolean("eol_know_date_dog_died_yn"),
          eolDeathDate = rawRecord.getOptionalDate("eol_date_dog_died"),
          eolCauseOfDeathPrimary = rawRecord.getOptionalNumber("eol_cause1_death"),
          eolCauseOfDeathPrimaryOtherDescription =
            rawRecord.getOptionalStripped("eol_cause1_death_specify"),
          eolCauseOfDeathSecondary = rawRecord.getOptionalNumber("eol_cause2_death"),
          eolCauseOfDeathSecondaryOtherDescription =
            rawRecord.getOptionalStripped("eol_cause2_death_specify"),
          eolOldAgePrimary = rawRecord.getOptionalNumber("eol_old_age_a"),
          eolOldAgePrimaryOtherDescription = rawRecord.getOptionalStripped("eol_old_age_a_specify"),
          eolTrauma = rawRecord.getOptionalNumber("eol_trauma_a"),
          eolTraumaOtherDescription = rawRecord.getOptionalStripped("eol_trauma_a_specify"),
          eolToxin = rawRecord.getOptionalNumber("eol_toxin_a"),
          eolToxinOtherDescription = rawRecord.getOptionalStripped("eol_toxin_a_specify"),
          eolOldAgeSecondary = rawRecord.getOptionalNumber("eol_old_age_cod_2"),
          eolOldAgeSecondaryOtherDescription =
            rawRecord.getOptionalStripped("eol_old_age_cod_2_specify"),
          eolNotesDescription = rawRecord.getOptionalStripped("eol_notes"),
          eolAddVemr = rawRecord.getOptionalBoolean("eol_add_med_record_yn"),
          eolsNewCondition = Some(NewConditionTransformations.mapNewConditionMetadata(rawRecord)),
          eolsRecentAgingChar =
            Some(RecentAgingCharsTransformations.mapRecentAgingChars(rawRecord)),
          eolsRecentSymptom = Some(RecentSymptomsTransformations.mapRecentSymptoms(rawRecord)),
          eolsDeath = Some(DeathTransformations.mapDeathTransformations(rawRecord)),
          eolsEuthan = Some(EuthanasiaTransformations.mapEuthanasiaFields(rawRecord)),
          eolsIllness = Some(IllnessTransformations.mapIllnessFields(rawRecord))
        )
      )
    } else {
      counter("eols", "not_willing_to_complete").inc()
      None
    }
  }
}
