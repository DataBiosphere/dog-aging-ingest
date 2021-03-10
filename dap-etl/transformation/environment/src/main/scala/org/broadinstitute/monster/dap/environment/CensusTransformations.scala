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
      cvPopulationEstimate = rawRecord.getOptionalNumber("cv_summary_est"),
      cvAreaSqmi = rawRecord.getOptional("cv_areasqmi").map(_.toDouble),
      cvPopulationDensity = rawRecord.getOptional("cv_popdensity").map(_.toDouble),
      cvPctNothispanicWhite = rawRecord.getOptional("cv_pctnothispanicwhite").map(_.toDouble),
      cvPctNothispanicBlack = rawRecord.getOptional("cv_pctnothispanicblack").map(_.toDouble),
      cvPctNothispanicaIan = rawRecord.getOptional("cv_pctnothispanicaian").map(_.toDouble),
      cvPctNothispanicAsian = rawRecord.getOptional("cv_pctnothispanicasian").map(_.toDouble),
      cvPctNothispanicnHpi = rawRecord.getOptional("cv_pctnothispanicnhpi").map(_.toDouble),
      cvPctNothispanicOther = rawRecord.getOptional("cv_pctnothispanicother").map(_.toDouble),
      cvPctNothispanicTwoOrMore =
        rawRecord.getOptional("cv_pctnothispanictwoormore").map(_.toDouble),
      cvPctHispanic = rawRecord.getOptional("cv_pcthispanic").map(_.toDouble),
      cvPctFemale = rawRecord.getOptional("cv_pctfemale").map(_.toDouble),
      cvMedianIncome = rawRecord.getOptionalNumber("cv_medianincomee"),
      cvGiniIndex = rawRecord.getOptional("cv_ginie").map(_.toDouble),
      cvPctBelow125povline = rawRecord.getOptional("cv_pctbelow125povline").map(_.toDouble),
      cvPctJobless16to64mf = rawRecord.getOptional("cv_pctjobless16to64mf").map(_.toDouble),
      cvPctFamsownchildFemaleLed =
        rawRecord.getOptional("cv_pctfamsownchildfemaleled").map(_.toDouble),
      cvPctLessThanBaDegree = rawRecord.getOptional("cv_pctlessthanba").map(_.toDouble),
      cvPctLessThan100k = rawRecord.getOptional("cv_pctlessthan100k").map(_.toDouble),
      cvDisadvantageIndex = rawRecord.getOptional("cv_disadvantageindex").map(_.toDouble),
      cvPctSameHouse1yrago = rawRecord.getOptional("cv_pctsamehouse1yrago").map(_.toDouble),
      cvPctOwnerOccupied = rawRecord.getOptional("cv_pctowneroccupied").map(_.toDouble),
      cvPctUsBorn = rawRecord.getOptional("cv_pctborninus").map(_.toDouble),
      cvStabilityIndex = rawRecord.getOptional("cv_stabilityindex").map(_.toDouble),
      cvDataYear = rawRecord.getOptionalNumber("cv_data_year")
    )
  }
}
