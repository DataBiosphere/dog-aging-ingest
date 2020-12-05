package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dap.environment.CensusTransformations
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CensusTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {

  behavior of "CensusTransformations"

  it should "map census variables where complete" in {
    val censusData = Map(
      "cv_summary_est" -> Array("3623"),
      "cv_areasqmi" -> Array("0.59936841406215"),
      "cv_popdensity" -> Array("6044.7"),
      "cv_pctnothispanicwhite" -> Array("88.02"),
      "cv_pctnothispanicblack" -> Array("1.82"),
      "cv_pctnothispanicaian" -> Array("0.19"),
      "cv_pctnothispanicasian" -> Array("5.24"),
      "cv_pctnothispanicnhpi" -> Array("0"),
      "cv_pctnothispanicother" -> Array("0"),
      "cv_pctnothispanictwoormore" -> Array("2.24"),
      "cv_pcthispanic" -> Array("2.48"),
      "cv_pctfemale" -> Array("53.13"),
      "cv_medianincomee" -> Array("130909"),
      "cv_ginie" -> Array("0.3981"),
      "cv_pctbelow125povline" -> Array("6.9"),
      "cv_pctjobless16to64mf" -> Array("20.14"),
      "cv_pctfamsownchildfemaleled" -> Array("8.67"),
      "cv_pctlessthanba" -> Array("21.8"),
      "cv_pctlessthan100k" -> Array("42.46"),
      "cv_disadvantageindex" -> Array(" -1.4"),
      "cv_pctsamehouse1yrago" -> Array("80.9"),
      "cv_pctowneroccupied" -> Array("66.55"),
      "cv_pctborninus" -> Array("88.85"),
      "cv_stabilityindex" -> Array("-0.1"),
      "cv_data_year" -> Array("1")
    )

    val censusDataMapped = CensusTransformations.mapCensusVariables(
      RawRecord(1, censusData)
    )

    // output of the example record's census transformations
    censusDataMapped.cvPopulationEstimate.value shouldBe 3623L
    censusDataMapped.cvAreaSqmi.value shouldBe Some(0.59936841406215)
    censusDataMapped.cvPopulationDensity.value shouldBe Some(6044.7)
    censusDataMapped.cvPctNothispanicWhite.value shouldBe Some(88.02)
    censusDataMapped.cvPctNothispanicBlack.value shouldBe Some(1.82)
    censusDataMapped.cvPctNothispanicaIan.value shouldBe Some(0.19)
    censusDataMapped.cvPctNothispanicAsian.value shouldBe Some(5.24)
    censusDataMapped.cvPctNothispanicnHpi.value shouldBe Some(0)
    censusDataMapped.cvPctNothispanicOther.value shouldBe Some(0)
    censusDataMapped.cvPctNothispanicTwoOrMore.value shouldBe Some(2.24)
    censusDataMapped.cvPctHispanic.value shouldBe Some(2.48)
    censusDataMapped.cvPctFemale.value shouldBe Some(53.13)
    censusDataMapped.cvMedianIncome.value shouldBe 130909L
    censusDataMapped.cvGiniIndex.value shouldBe Some(0.3981)
    censusDataMapped.cvPctBelow125povline.value shouldBe Some(6.9)
    censusDataMapped.cvPctJobless16to64mf.value shouldBe Some(20.14)
    censusDataMapped.cvPctFamsownchildFemaleLed.value shouldBe Some(8.67)
    censusDataMapped.cvPctLessThanBaDegree.value shouldBe Some(21.8)
    censusDataMapped.cvPctLessThan100k.value shouldBe Some(42.46)
    censusDataMapped.cvDisadvantageIndex.value shouldBe Some(-1.4)
    censusDataMapped.cvPctSameHouse1yrago.value shouldBe Some(80.9)
    censusDataMapped.cvPctOwnerOccupied.value shouldBe Some(66.55)
    censusDataMapped.cvPctUsBorn.value shouldBe Some(88.85)
    censusDataMapped.cvStabilityIndex.value shouldBe Some(-0.1)
    censusDataMapped.cvDataYear.value shouldBe 1L
  }
}
