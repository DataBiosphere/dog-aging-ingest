package org.broadinstitute.monster.dap.environment

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.EnvironmentCensus

object CensusTransformations {

  /**
    * Parse all census variables out of a raw RedCap record,
    * injecting them into a partially-modeled environment record.
    */
  def mapCensusVariables(rawRecord: RawRecord): EnvironmentCensus = {
    EnvironmentCensus(
      cvPopulationEstimate = rawRecord.getOptionalNumber("cv_population_estimate"),
      cvAreaSqmi = rawRecord.getOptional("cv_area_sqmi").map(_.toDouble),
      cvPopulationDensity = rawRecord.getOptional("cv_population_density").map(_.toDouble),
      cvPctNothispanicWhite = rawRecord.getOptional("cv_pct_nothispanic_white").map(_.toDouble),
      cvPctNothispanicBlack = rawRecord.getOptional("cv_pct_nothispanic_black").map(_.toDouble),
      cvPctNothispanicaIan = rawRecord.getOptional("cv_pct_nothispanica_ian").map(_.toDouble),
      cvPctNothispanicAsian = rawRecord.getOptional("cv_pct_nothispanic_asian").map(_.toDouble),
      cvPctNothispanicnHpi = rawRecord.getOptional("cv_pct_nothispanicn_hpi").map(_.toDouble),
      cvPctNothispanicOther = rawRecord.getOptional("cv_pct_nothispanic_other").map(_.toDouble),
      cvPctNothispanicTwoOrMore =
        rawRecord.getOptional("cv_pct_nothispanic_two_or_more").map(_.toDouble),
      cvPctHispanic = rawRecord.getOptional("cv_pct_hispanic").map(_.toDouble),
      cvPctFemale = rawRecord.getOptional("cv_pct_female").map(_.toDouble),
      cvMedianIncome = rawRecord.getOptionalNumber("cv_median_income"),
      cvGiniIndex = rawRecord.getOptional("cv_gini_index").map(_.toDouble),
      cvPctBelow125povline = rawRecord.getOptional("cv_pct_below_125povline").map(_.toDouble),
      cvPctJobless16to64mf = rawRecord.getOptional("cv_pct_jobless16to64mf").map(_.toDouble),
      cvPctFamsownchildFemaleLed =
        rawRecord.getOptional("cv_pct_famsownchild_female_led").map(_.toDouble),
      cvPctLessThanBaDegree = rawRecord.getOptional("cv_pct_less_than_ba_degree").map(_.toDouble),
      cvPctLessThan100k = rawRecord.getOptional("cv_pct_less_than_100k").map(_.toDouble),
      cvDisadvantageIndex = rawRecord.getOptional("cv_disadvantage_index").map(_.toDouble),
      cvPctSameHouse1yrago = rawRecord.getOptional("cv_pct_same_house_1yrago").map(_.toDouble),
      cvPctOwnerOccupied = rawRecord.getOptional("cv_pct_owner_occupied").map(_.toDouble),
      cvPctUsBorn = rawRecord.getOptional("cv_pct_us_born").map(_.toDouble),
      cvStabilityIndex = rawRecord.getOptional("cv_stability_index").map(_.toDouble),
      cvDataYear = rawRecord.getOptionalNumber("cv_data_year")
    )
  }
}
