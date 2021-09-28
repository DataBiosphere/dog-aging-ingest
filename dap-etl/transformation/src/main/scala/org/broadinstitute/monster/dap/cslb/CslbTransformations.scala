package org.broadinstitute.monster.dap.cslb

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.Cslb
import com.spotify.scio.ScioMetrics.counter

object CslbTransformations {

  def mapCslbData(rawRecord: RawRecord): Option[Cslb] = {
    rawRecord.getOptionalDate("cslb_date") match {
      case Some(cslbDate) =>
        Some(
          Cslb(
            dogId = rawRecord.getRequired("study_id").toLong,
            cslbDate = cslbDate,
            cslbYear =
              rawRecord.getRequired("redcap_event_name").split("_")(1).filter(_.isDigit).toLong,
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
            cslbOtherChanges = rawRecord.getOptionalStripped("cslb_other_changes")
          )
        )
      case _ => {
        counter("cslb", "missing_cslb_date").inc()
        None
      }
    }
  }
}
