package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dap.environment.TemperaturePrecipitationTransformations
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TemperaturePrecipitationTransformationsSpec
    extends AnyFlatSpec
    with Matchers
    with OptionValues {

  behavior of "TemperaturePrecipitationTransformations"

  it should "map temperature and precipitation variables where complete" in {
    val tempAndPrecipData = Map(
      "tpDataYear" -> Array("1"),
      "tpPcpnAnnual01" -> Array("6.48"),
      "tpPcpnAnnual02" -> Array("6.28"),
      "tpPcpnAnnual03" -> Array("2.26"),
      "tpPcpnAnnual04" -> Array("7.17"),
      "tpPcpnAnnual05" -> Array("2.39"),
      "tpPcpnAnnual06" -> Array("2.39"),
      "tpPcpnAnnual07" -> Array("1.99"),
      "tpPcpnAnnual08" -> Array("1.43"),
      "tpPcpnAnnual09" -> Array("5.87"),
      "tpPcpnAnnual10" -> Array("8.16"),
      "tpPcpnAnnual11" -> Array("3.79"),
      "tpPcpnAnnual12" -> Array("10.65"),
      "tpTmpcAnnual01" -> Array("39.5"),
      "tpTmpcAnnual02" -> Array("30.7"),
      "tpTmpcAnnual03" -> Array("41.5"),
      "tpTmpcAnnual04" -> Array("46"),
      "tpTmpcAnnual05" -> Array("54.9"),
      "tpTmpcAnnual06" -> Array("57"),
      "tpTmpcAnnual07" -> Array("60.9"),
      "tpTmpcAnnual08" -> Array("63.6"),
      "tpTmpcAnnual09" -> Array("56.6"),
      "tpTmpcAnnual10" -> Array("45"),
      "tpTmpcAnnual11" -> Array("41.7"),
      "tpTmpcAnnual12" -> Array("38.2"),
      "tpTmaxAnnual01" -> Array("45.1"),
      "tpTmaxAnnual02" -> Array("36.4"),
      "tpTmaxAnnual03" -> Array("50.8"),
      "tpTmaxAnnual04" -> Array("53.6"),
      "tpTmaxAnnual05" -> Array("64.6"),
      "tpTmaxAnnual06" -> Array("66.5"),
      "tpTmaxAnnual07" -> Array("69.7"),
      "tpTmaxAnnual08" -> Array("73"),
      "tpTmaxAnnual09" -> Array("63.6"),
      "tpTmaxAnnual10" -> Array("52.5"),
      "tpTmaxAnnual11" -> Array("47.9"),
      "tpTmaxAnnual12" -> Array("42"),
      "tpTminAnnual01" -> Array("33.9"),
      "tpTminAnnual02" -> Array("25"),
      "tpTminAnnual03" -> Array("32.2"),
      "tpTminAnnual04" -> Array("38.5"),
      "tpTminAnnual05" -> Array("45.2"),
      "tpTminAnnual06" -> Array("47.5"),
      "tpTminAnnual07" -> Array("52.1"),
      "tpTminAnnual08" -> Array("54.1"),
      "tpTminAnnual09" -> Array("49.6"),
      "tpTminAnnual10" -> Array("37.6"),
      "tpTminAnnual11" -> Array("35.4"),
      "tpTminAnnual12" -> Array("34.3"),
      "tpNormDataYear" -> Array("1"),
      "tpPcpnNorm01" -> Array("10.55"),
      "tpPcpnNorm02" -> Array("6.92"),
      "tpPcpnNorm03" -> Array("7.42"),
      "tpPcpnNorm04" -> Array("5.65"),
      "tpPcpnNorm05" -> Array("4.46"),
      "tpPcpnNorm06" -> Array("3.47"),
      "tpPcpnNorm07" -> Array("1.6"),
      "tpPcpnNorm08" -> Array("1.65"),
      "tpPcpnNorm09" -> Array("3.19"),
      "tpPcpnNorm10" -> Array("6.83"),
      "tpPcpnNorm11" -> Array("12.05"),
      "tpPcpnNorm12" -> Array("9.73"),
      "tpTmpcNorm01" -> Array("36.4"),
      "tpTmpcNorm02" -> Array("37.9"),
      "tpTmpcNorm03" -> Array("41.2"),
      "tpTmpcNorm04" -> Array("45.2"),
      "tpTmpcNorm05" -> Array("50.9"),
      "tpTmpcNorm06" -> Array("55.9"),
      "tpTmpcNorm07" -> Array("61.5"),
      "tpTmpcNorm08" -> Array("62"),
      "tpTmpcNorm09" -> Array("56.9"),
      "tpTmpcNorm10" -> Array("48"),
      "tpTmpcNorm11" -> Array("40"),
      "tpTmpcNorm12" -> Array("34.9"),
      "tpTmaxNorm01" -> Array("41.2"),
      "tpTmaxNorm02" -> Array("44.3"),
      "tpTmaxNorm03" -> Array("48.6"),
      "tpTmaxNorm04" -> Array("53.8"),
      "tpTmaxNorm05" -> Array("60"),
      "tpTmaxNorm06" -> Array("65.1"),
      "tpTmaxNorm07" -> Array("71.9"),
      "tpTmaxNorm08" -> Array("72.5"),
      "tpTmaxNorm09" -> Array("66.7"),
      "tpTmaxNorm10" -> Array("55.5"),
      "tpTmaxNorm11" -> Array("45.1"),
      "tpTmaxNorm12" -> Array("39.4"),
      "tpTminNorm01" -> Array("31.6"),
      "tpTminNorm02" -> Array("31.5"),
      "tpTminNorm03" -> Array("33.7"),
      "tpTminNorm04" -> Array("36.6"),
      "tpTminNorm05" -> Array("41.7"),
      "tpTminNorm06" -> Array("46.6"),
      "tpTminNorm07" -> Array("51"),
      "tpTminNorm08" -> Array("51.4"),
      "tpTminNorm09" -> Array("47.1"),
      "tpTminNorm10" -> Array("40.5"),
      "tpTminNorm11" -> Array("34.9"),
      "tpTminNorm12" -> Array("30.3")
    )

    val tempAndPrecipDataMapped =
      TemperaturePrecipitationTransformations.mapTemperaturePrecipitationVariables(
        RawRecord(1, tempAndPrecipData)
      )

    // output of the example record's temperature and preciptation transformations
    tempAndPrecipDataMapped.tpDataYear.value shouldBe 1L
    tempAndPrecipDataMapped.tpPcpnAnnual01.value shouldBe Some(6.48)
    tempAndPrecipDataMapped.tpPcpnAnnual02.value shouldBe Some(6.28)
    tempAndPrecipDataMapped.tpPcpnAnnual03.value shouldBe Some(2.26)
    tempAndPrecipDataMapped.tpPcpnAnnual04.value shouldBe Some(7.17)
    tempAndPrecipDataMapped.tpPcpnAnnual05.value shouldBe Some(2.39)
    tempAndPrecipDataMapped.tpPcpnAnnual06.value shouldBe Some(2.39)
    tempAndPrecipDataMapped.tpPcpnAnnual07.value shouldBe Some(1.99)
    tempAndPrecipDataMapped.tpPcpnAnnual08.value shouldBe Some(1.43)
    tempAndPrecipDataMapped.tpPcpnAnnual09.value shouldBe Some(5.87)
    tempAndPrecipDataMapped.tpPcpnAnnual10.value shouldBe Some(8.16)
    tempAndPrecipDataMapped.tpPcpnAnnual11.value shouldBe Some(3.79)
    tempAndPrecipDataMapped.tpPcpnAnnual12.value shouldBe Some(10.65)
    tempAndPrecipDataMapped.tpTmpcAnnual01.value shouldBe Some(39.5)
    tempAndPrecipDataMapped.tpTmpcAnnual02.value shouldBe Some(30.7)
    tempAndPrecipDataMapped.tpTmpcAnnual03.value shouldBe Some(41.5)
    tempAndPrecipDataMapped.tpTmpcAnnual04.value shouldBe Some(46)
    tempAndPrecipDataMapped.tpTmpcAnnual05.value shouldBe Some(54.9)
    tempAndPrecipDataMapped.tpTmpcAnnual06.value shouldBe Some(57)
    tempAndPrecipDataMapped.tpTmpcAnnual07.value shouldBe Some(60.9)
    tempAndPrecipDataMapped.tpTmpcAnnual08.value shouldBe Some(63.6)
    tempAndPrecipDataMapped.tpTmpcAnnual09.value shouldBe Some(56.6)
    tempAndPrecipDataMapped.tpTmpcAnnual10.value shouldBe Some(45)
    tempAndPrecipDataMapped.tpTmpcAnnual11.value shouldBe Some(41.7)
    tempAndPrecipDataMapped.tpTmpcAnnual12.value shouldBe Some(38.2)
    tempAndPrecipDataMapped.tpTmaxAnnual01.value shouldBe Some(45.1)
    tempAndPrecipDataMapped.tpTmaxAnnual02.value shouldBe Some(36.4)
    tempAndPrecipDataMapped.tpTmaxAnnual03.value shouldBe Some(50.8)
    tempAndPrecipDataMapped.tpTmaxAnnual04.value shouldBe Some(53.6)
    tempAndPrecipDataMapped.tpTmaxAnnual05.value shouldBe Some(64.6)
    tempAndPrecipDataMapped.tpTmaxAnnual06.value shouldBe Some(66.5)
    tempAndPrecipDataMapped.tpTmaxAnnual07.value shouldBe Some(69.7)
    tempAndPrecipDataMapped.tpTmaxAnnual08.value shouldBe Some(73)
    tempAndPrecipDataMapped.tpTmaxAnnual09.value shouldBe Some(63.6)
    tempAndPrecipDataMapped.tpTmaxAnnual10.value shouldBe Some(52.5)
    tempAndPrecipDataMapped.tpTmaxAnnual11.value shouldBe Some(47.9)
    tempAndPrecipDataMapped.tpTmaxAnnual12.value shouldBe Some(42)
    tempAndPrecipDataMapped.tpTminAnnual01.value shouldBe Some(33.9)
    tempAndPrecipDataMapped.tpTminAnnual02.value shouldBe Some(25)
    tempAndPrecipDataMapped.tpTminAnnual03.value shouldBe Some(32.2)
    tempAndPrecipDataMapped.tpTminAnnual04.value shouldBe Some(38.5)
    tempAndPrecipDataMapped.tpTminAnnual05.value shouldBe Some(45.2)
    tempAndPrecipDataMapped.tpTminAnnual06.value shouldBe Some(47.5)
    tempAndPrecipDataMapped.tpTminAnnual07.value shouldBe Some(52.1)
    tempAndPrecipDataMapped.tpTminAnnual08.value shouldBe Some(54.1)
    tempAndPrecipDataMapped.tpTminAnnual09.value shouldBe Some(49.6)
    tempAndPrecipDataMapped.tpTminAnnual10.value shouldBe Some(37.6)
    tempAndPrecipDataMapped.tpTminAnnual11.value shouldBe Some(35.4)
    tempAndPrecipDataMapped.tpTminAnnual12.value shouldBe Some(34.3)
    tempAndPrecipDataMapped.tpNormDataYear.value shouldBe 1L
    tempAndPrecipDataMapped.tpPcpnNorm01.value shouldBe Some(10.55)
    tempAndPrecipDataMapped.tpPcpnNorm02.value shouldBe Some(6.92)
    tempAndPrecipDataMapped.tpPcpnNorm03.value shouldBe Some(7.42)
    tempAndPrecipDataMapped.tpPcpnNorm04.value shouldBe Some(5.65)
    tempAndPrecipDataMapped.tpPcpnNorm05.value shouldBe Some(4.46)
    tempAndPrecipDataMapped.tpPcpnNorm06.value shouldBe Some(3.47)
    tempAndPrecipDataMapped.tpPcpnNorm07.value shouldBe Some(1.6)
    tempAndPrecipDataMapped.tpPcpnNorm08.value shouldBe Some(1.65)
    tempAndPrecipDataMapped.tpPcpnNorm09.value shouldBe Some(3.19)
    tempAndPrecipDataMapped.tpPcpnNorm10.value shouldBe Some(6.83)
    tempAndPrecipDataMapped.tpPcpnNorm11.value shouldBe Some(12.05)
    tempAndPrecipDataMapped.tpPcpnNorm12.value shouldBe Some(9.73)
    tempAndPrecipDataMapped.tpTmpcNorm01.value shouldBe Some(36.4)
    tempAndPrecipDataMapped.tpTmpcNorm02.value shouldBe Some(37.9)
    tempAndPrecipDataMapped.tpTmpcNorm03.value shouldBe Some(41.2)
    tempAndPrecipDataMapped.tpTmpcNorm04.value shouldBe Some(45.2)
    tempAndPrecipDataMapped.tpTmpcNorm05.value shouldBe Some(50.9)
    tempAndPrecipDataMapped.tpTmpcNorm06.value shouldBe Some(55.9)
    tempAndPrecipDataMapped.tpTmpcNorm07.value shouldBe Some(61.5)
    tempAndPrecipDataMapped.tpTmpcNorm08.value shouldBe Some(62)
    tempAndPrecipDataMapped.tpTmpcNorm09.value shouldBe Some(56.9)
    tempAndPrecipDataMapped.tpTmpcNorm10.value shouldBe Some(48)
    tempAndPrecipDataMapped.tpTmpcNorm11.value shouldBe Some(40)
    tempAndPrecipDataMapped.tpTmpcNorm12.value shouldBe Some(34.9)
    tempAndPrecipDataMapped.tpTmaxNorm01.value shouldBe Some(41.2)
    tempAndPrecipDataMapped.tpTmaxNorm02.value shouldBe Some(44.3)
    tempAndPrecipDataMapped.tpTmaxNorm03.value shouldBe Some(48.6)
    tempAndPrecipDataMapped.tpTmaxNorm04.value shouldBe Some(53.8)
    tempAndPrecipDataMapped.tpTmaxNorm05.value shouldBe Some(60)
    tempAndPrecipDataMapped.tpTmaxNorm06.value shouldBe Some(65.1)
    tempAndPrecipDataMapped.tpTmaxNorm07.value shouldBe Some(71.9)
    tempAndPrecipDataMapped.tpTmaxNorm08.value shouldBe Some(72.5)
    tempAndPrecipDataMapped.tpTmaxNorm09.value shouldBe Some(66.7)
    tempAndPrecipDataMapped.tpTmaxNorm10.value shouldBe Some(55.5)
    tempAndPrecipDataMapped.tpTmaxNorm11.value shouldBe Some(45.1)
    tempAndPrecipDataMapped.tpTmaxNorm12.value shouldBe Some(39.4)
    tempAndPrecipDataMapped.tpTminNorm01.value shouldBe Some(31.6)
    tempAndPrecipDataMapped.tpTminNorm02.value shouldBe Some(31.5)
    tempAndPrecipDataMapped.tpTminNorm03.value shouldBe Some(33.7)
    tempAndPrecipDataMapped.tpTminNorm04.value shouldBe Some(36.6)
    tempAndPrecipDataMapped.tpTminNorm05.value shouldBe Some(41.7)
    tempAndPrecipDataMapped.tpTminNorm06.value shouldBe Some(46.6)
    tempAndPrecipDataMapped.tpTminNorm07.value shouldBe Some(51)
    tempAndPrecipDataMapped.tpTminNorm08.value shouldBe Some(51.4)
    tempAndPrecipDataMapped.tpTminNorm09.value shouldBe Some(47.1)
    tempAndPrecipDataMapped.tpTminNorm10.value shouldBe Some(40.5)
    tempAndPrecipDataMapped.tpTminNorm11.value shouldBe Some(34.9)
    tempAndPrecipDataMapped.tpTminNorm12.value shouldBe Some(30.3)
  }
}
