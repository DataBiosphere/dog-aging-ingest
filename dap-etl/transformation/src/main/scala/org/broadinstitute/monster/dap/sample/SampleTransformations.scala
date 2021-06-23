package org.broadinstitute.monster.dap.sample

import org.broadinstitute.monster.dogaging.jadeschema.table.Sample
import org.broadinstitute.monster.dap.common.RawRecord

object SampleTransformations {

  def mapSampleData(rawRecord: RawRecord): Option[Sample] = {
    rawRecord.getOptionalDateTime("k1_rtn_tracking_date") match {
      case Some(dateCollected) =>
        Some(
          Sample(
            dogId = rawRecord.getRequired("study_id").toLong,
            cohort = rawRecord.getRequired("ce_enroll_stat").toLong,
            sampleId = rawRecord.getRequired("k1_tube_serial").toLong,
            sampleType = "saliva_DNA_lowcov",
            dateSwabArrivalLaboratory = dateCollected
          )
        )
      case _ =>
        None
    }
  }

}
