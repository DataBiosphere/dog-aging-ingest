package org.broadinstitute.monster.dap.environment

import org.broadinstitute.monster.dap.common.RawRecord

import java.time.LocalDate
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
    // Get all values for walkscore date into an array, remove "NA"
    // "NA" values are removed from arrays within RawRecord.scala
    val walkscoreDateArray = rawRecord.getArray("wv_walkscore_date").toSet.--(Array("NA"))
    if (walkscoreDateArray.nonEmpty) {
      walkscoreDate = rawRecord.getOptionalDateTime("wv_walkscore_date")
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
