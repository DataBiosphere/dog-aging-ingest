package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.EolsRecentAgingChar

object RecentAgingCharsTransformations {

  /**
    * Parse all eol_aging_characteristics data out of a raw RedCap record,
    * injecting them into a partially-modeled Eols record.
    */
  def mapRecentAgingChars(rawRecord: RawRecord): EolsRecentAgingChar = {
    val agingChars = Some(rawRecord.getArray("eol_aging_characteristics").map(_.toLong))
    val otherChar = agingChars.map(_.contains(98L))
    EolsRecentAgingChar(
      eolRecentAgingCharNone = agingChars.map(_.contains(0L)),
      eolRecentAgingCharBlind = agingChars.map(_.contains(1L)),
      eolRecentAgingCharDeaf = agingChars.map(_.contains(2L)),
      eolRecentAgingCharWeightloss = agingChars.map(_.contains(3L)),
      eolRecentAgingCharMobilityWeak = agingChars.map(_.contains(4L)),
      eolRecentAgingCharMobilityPain = agingChars.map(_.contains(5L)),
      eolRecentAgingCharOtherPain = agingChars.map(_.contains(6L)),
      eolRecentAgingCharCleanliness = agingChars.map(_.contains(7L)),
      eolRecentAgingCharConfusion = agingChars.map(_.contains(8L)),
      eolRecentAgingCharInteractionChange = agingChars.map(_.contains(9L)),
      eolRecentAgingCharSleep = agingChars.map(_.contains(10L)),
      eolRecentAgingCharHousesoiling = agingChars.map(_.contains(11L)),
      eolRecentAgingCharAnxiety = agingChars.map(_.contains(12L)),
      eolRecentAgingCharEatDrink = agingChars.map(_.contains(13L)),
      eolRecentAgingCharInactivity = agingChars.map(_.contains(14L)),
      eolRecentAgingCharRepetitiveActivity = agingChars.map(_.contains(15L)),
      eolRecentAgingCharMemory = agingChars.map(_.contains(16L)),
      eolRecentAgingCharRecognition = agingChars.map(_.contains(17L)),
      eolRecentAgingCharSundowning = agingChars.map(_.contains(18L)),
      eolRecentAgingCharOther = otherChar,
      eolRecentAgingCharOtherDescription =
        if (otherChar.contains(true))
          rawRecord.getOptionalStripped("eol_aging_characteristics_specify")
        else None
    )
  }
}
