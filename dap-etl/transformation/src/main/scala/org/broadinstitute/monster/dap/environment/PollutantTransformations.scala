package org.broadinstitute.monster.dap.environment

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.EnvironmentPollutants

object PollutantTransformations {

  /**
    * Parse all pollutant variables out of a raw RedCap record,
    * injecting them into a partially-modeled environment record.
    */
  def mapPollutantVariables(rawRecord: RawRecord): EnvironmentPollutants = {
    EnvironmentPollutants(
      pvDataYear = rawRecord.getOptionalNumber("pv_data_year"),
      pvCo = rawRecord.getOptional("pv_co").map(_.toDouble),
      pvNo2 = rawRecord.getOptional("pv_no2").map(_.toDouble),
      pvO3 = rawRecord.getOptional("pv_o3").map(_.toDouble),
      pvPm10 = rawRecord.getOptional("pv_pm10").map(_.toDouble),
      pvPm25 = rawRecord.getOptional("pv_pm25").map(_.toDouble),
      pvSo2 = rawRecord.getOptional("pv_so2").map(_.toDouble),
      pvComplete = rawRecord.getOptionalNumber("pollutant_variables_complete")
    )
  }
}
