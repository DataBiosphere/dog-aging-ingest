package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.EolsDeath

object DeathTransformations {

  def mapDeathTransformations(rawRecord: RawRecord): EolsDeath = {
    val deathLocation = rawRecord.getOptionalNumber("eol_death_location")
    val deathWitness = rawRecord.fields.get("eol_who_present")
    val deathWitnessOther = deathWitness.map(_.contains("98"))
    EolsDeath(
      eolDeathLocation = deathLocation,
      eolDeathLocationOtherDescription =
        if (deathLocation.contains(98))
          rawRecord.getOptional("eol_death_location_specify")
        else None,
      eolDeathWitness = rawRecord.getOptionalNumber("eol_anyone_present"),
      eolDeathWitnessWhoYou = deathWitness.map(_.contains("1")),
      eolDeathWitnessWhoFamily = deathWitness.map(_.contains("2")),
      eolDeathWitnessWhoAcquaintance = deathWitness.map(_.contains("3")),
      eolDeathWitnessWhoVet = deathWitness.map(_.contains("4")),
      eolDeathWitnessWhoBoarder = deathWitness.map(_.contains("5")),
      eolDeathWitnessWhoOther = deathWitnessOther,
      eolDeathWitnessWhoOtherDescription =
        if (deathWitness.contains(true))
          rawRecord.getOptional("eol_who_present_specify")
        else None
    )
  }
}
