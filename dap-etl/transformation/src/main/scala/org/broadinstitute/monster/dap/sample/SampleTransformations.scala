package org.broadinstitute.monster.dap.sample

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.Sample

object SampleTransformations {

  def mapSampleData(rawRecord: RawRecord): Option[Sample] = {
    val dateCollected = rawRecord.getOptionalDateTime("k1_rtn_tracking_date")
    val kitId = rawRecord.getOptionalNumber("k1_tube_serial")
    val cohort = rawRecord.getOptionalNumber("ce_enroll_stat")
    (dateCollected, kitId, cohort) match {
      case (Some(dateCollected), Some(kitId), Some(cohort)) =>
        Some(
          Sample(
            dogId = rawRecord.getRequired("study_id").toLong,
            cohort = Some(cohort),
            sampleId = kitId,
            sampleType = "saliva_DNA_lowcov",
            dateSwabArrivalLaboratory = Some(dateCollected)
          )
        )
      case _ =>
        None
    }
  }

}
