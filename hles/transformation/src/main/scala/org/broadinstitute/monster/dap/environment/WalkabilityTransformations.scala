package org.broadinstitute.monster.dap.environment

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.EnvironmentWalkability

object WalkabilityTransformations {

  /**
    * Parse all walkability variables out of a raw RedCap record,
    * injecting them into a partially-modeled environment record.
    */
  def mapWalkabilityVariables(rawRecord: RawRecord): EnvironmentWalkability = {
    var walkscore: Option[Double] = None
    var walkscoreDate: Option[String] = None

    if (!rawRecord.getOptional("wv_walkscore").contains("NA")) {
      walkscore = rawRecord.getOptional("wv_walkscore").map(_.toDouble)
    }
    // We observed instances of "NA" in data returned from RedCap
    if (rawRecord.getArray("wv_walkscore_date").nonEmpty) {
      if (!rawRecord.getArray("wv_walkscore_date")(0).contains("NA")) {
        walkscoreDate = Some(rawRecord.getArray("wv_walkscore_date")(0))
      }
    }

    EnvironmentWalkability(
      wvWalkscore = walkscore,
      wvWalkscoreDescrip = rawRecord.getOptionalNumber("wv_walkscore_descrip"),
      wvWalkscoreDate = walkscoreDate,
      wvHousingUnits = rawRecord.getOptional("wv_housing_units").map(_.toDouble),
      wvResDensity = rawRecord.getOptional("wv_res_density").map(_.toDouble),
      wvDensityDataYear = rawRecord.getOptionalNumber("wv_density_data_year")
    )
  }
}
