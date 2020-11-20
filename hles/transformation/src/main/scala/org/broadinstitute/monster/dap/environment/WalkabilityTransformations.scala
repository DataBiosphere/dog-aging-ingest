package org.broadinstitute.monster.dap.environment

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.EnvironmentWalkability

object WalkabilityTransformations {

  /**
    * Parse all walkability variables out of a raw RedCap record,
    * injecting them into a partially-modeled environment record.
    */
  def mapWalkabilityVariables(rawRecord: RawRecord): EnvironmentWalkability = {
    EnvironmentWalkability(
      wvWalkscore = rawRecord.getOptional("wv_walkscore").map(_.toDouble),
      wvWalkscoreDescrip = rawRecord.getOptionalNumber("wv_walkscore_descrip"),
      wvWalkscoreDate = rawRecord.getOptional("wv_walkscore_date").map(_.toDouble),
      wvHousingUnits = rawRecord.getOptional("wv_housing_units").map(_.toDouble),
      wvResDensity = rawRecord.getOptional("wv_res_density").map(_.toDouble),
      wvDensityDataYear = rawRecord.getOptionalNumber("wv_density_data_year")
    )
  }
}
