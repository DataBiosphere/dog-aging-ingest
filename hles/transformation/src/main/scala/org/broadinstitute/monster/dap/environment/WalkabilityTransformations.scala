package org.broadinstitute.monster.dap.environment

import java.time.LocalDate

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.EnvironmentWalkability

object WalkabilityTransformations {

  /**
    * Parse all walkability variables out of a raw RedCap record,
    * injecting them into a partially-modeled environment record.
    */
  def mapWalkabilityVariables(rawRecord: RawRecord): EnvironmentWalkability = {
    var walkscore: Option[Double] = None
    var walkscoreDate: Option[LocalDate] = None

    if (!rawRecord.getOptional("wv_walkscore").contains("NA")) {
      walkscore = rawRecord.getOptional("wv_walkscore").map(_.toDouble)
    }
    // We observed instances of "NA" in data returned from RedCap
    // We are checking to see if there is an array (>0 values)
    if (rawRecord.getArray("wv_walkscore_date").nonEmpty) {
      // If there is an array and its first item is NOT "NA"
      if (!rawRecord.getArray("wv_walkscore_date")(0).contains("NA")) {
        walkscoreDate = rawRecord.getOptionalDateTime("wv_walkscore_date")
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
