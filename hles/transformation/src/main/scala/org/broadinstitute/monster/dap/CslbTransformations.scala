package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesCslb

object CslbTransformations {

  def mapCslbData(rawRecord: RawRecord): Option[HlesCslb] =
    rawRecord.getOptionalDate("cslb_date") match {
      case Some(cslbDate) =>
        Some(
          HlesCslb(
            dogId = rawRecord.id,
            cslbDate = cslbDate,
            cslbPace = rawRecord.getOptionalNumber("cslb_pace"),
            cslbStare = rawRecord.getOptionalNumber("cslb_stare"),
            cslbStuck = rawRecord.getOptionalNumber("cslb_stuck"),
            cslbRecognize = rawRecord.getOptionalNumber("cslb_recognize"),
            cslbWalkWalls = rawRecord.getOptionalNumber("cslb_walk_walls"),
            cslbAvoid = rawRecord.getOptionalNumber("cslb_avoid"),
            cslbFindFood = rawRecord.getOptionalNumber("cslb_find_food"),
            cslbPace6mo = rawRecord.getOptionalNumber("cslb_pace_6mo"),
            cslbStare6mo = rawRecord.getOptionalNumber("cslb_stare_6mo"),
            cslbDefecate6mo = rawRecord.getOptionalNumber("cslb_defecate_6mo"),
            cslbFood6mo = rawRecord.getOptionalNumber("cslb_food_6mo"),
            cslbRecognize6mo = rawRecord.getOptionalNumber("cslb_recognize_6mo"),
            cslbActive6mo = rawRecord.getOptionalNumber("cslb_active_6mo"),
            cslbScore = rawRecord.getOptionalNumber("cslb_score"),
            cslbOtherChanges = rawRecord.getOptional("cslb_other_changes")
          )
        )
      case _ =>
        None
    }
}
