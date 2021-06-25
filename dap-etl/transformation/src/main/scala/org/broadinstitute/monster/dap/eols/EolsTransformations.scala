package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.Eols

object EolsTransformations {

  /** Parse all eols related fields out of a RedCap record.
    * The schema for eols variables has been separated into 6 jade-fragments.
    */
  def mapEols(rawRecord: RawRecord): Option[Eols] = {
    val dogId = rawRecord.id
    val willingToComplete = rawRecord.getRequiredBoolean("eol_willing_to_complete")
    if (willingToComplete) {
      Some(
        Eols(
          dogId = dogId,
          eolWillingToComplete = Some(willingToComplete),
          eolExactDeathDateKnown = rawRecord.getOptionalBoolean("eol_know_date_dog_died_yn"),
          eolDeathDate = rawRecord.getOptional("eol_date_dog_died"),
          eolCauseOfDeathPrimary = rawRecord.getOptionalNumber("eol_cause1_death"),
          eolCauseOfDeathPrimaryOtherDescription =
            rawRecord.getOptional("eol_cause1_death_specify"),
          eolCauseOfDeathSecondary = rawRecord.getOptionalNumber("eol_cause2_death"),
          eolCauseOfDeathSecondaryOtherDescription =
            rawRecord.getOptional("eol_cause2_death_specify"),
          eolOldAgePrimary = rawRecord.getOptionalNumber("eol_old_age_a"),
          eolOldAgePrimaryOtherDescription = rawRecord.getOptional("eol_old_age_a_specify"),
          eolTrauma = rawRecord.getOptionalNumber("eol_trauma_a"),
          eolTraumaOtherDescription = rawRecord.getOptional("eol_trauma_a_specify"),
          eolToxin = rawRecord.getOptionalNumber("eol_toxin_a"),
          eolToxinOtherDescription = rawRecord.getOptional("eol_toxin_a_specify"),
          eolOldAgeSecondary = rawRecord.getOptionalNumber("eol_old_age_cod_2"),
          eolOldAgeSecondaryOtherDescription = rawRecord.getOptional("eol_old_age_cod_2_specify"),
          eolNotesDescription = rawRecord.getOptional("eol_notes"),
          eolAddVemr = rawRecord.getOptionalBoolean("eol_add_med_record_yn"),
          // todo: add transformation logic for additional fragments
          eolsNewCondition = Some(NewConditionTransformations.mapNewConditionMetadata(rawRecord)),
          eolsRecentAgingChar = None,
          eolsRecentSymptom = None,
          eolsDeath = None,
          eolsEuthan = None,
          eolsIllness = None
        )
      )
    } else {
      None
    }
  }
}
