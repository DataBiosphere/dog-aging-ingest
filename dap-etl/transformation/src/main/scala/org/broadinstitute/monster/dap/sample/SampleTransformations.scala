package org.broadinstitute.monster.dap.sample

import org.broadinstitute.monster.dogaging.jadeschema.table.Sample
import org.broadinstitute.monster.dap.common.RawRecord

object SampleTransformations {

  def mapSampleData(rawRecord: RawRecord): Option[Sample] = {
    case Some(dogId) =>
      Sample(
        dogId = rawRecord.getRequired("study_id").toLong,
        cohort = rawRecord.getOptionalNumber("ce_enroll_stat"),
        sampleId = rawRecord.getRequired("k1_tube_serial"),
        dateCollected = rawRecord.getOptionalDate("k1_rtn_tracking_date")
      )
    case _ =>
      None
  }

}
