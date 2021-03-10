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
      "tp_data_year" -> Array("1"),
      "tp_pcpn_annual_01" -> Array("6.48"),
      "tp_pcpn_annual_02" -> Array("6.28"),
      "tp_pcpn_annual_03" -> Array("2.26"),
      "tp_pcpn_annual_04" -> Array("7.17"),
      "tp_pcpn_annual_05" -> Array("2.39"),
      "tp_pcpn_annual_06" -> Array("2.39"),
      "tp_pcpn_annual_07" -> Array("1.99"),
      "tp_pcpn_annual_08" -> Array("1.43"),
      "tp_pcpn_annual_09" -> Array("5.87"),
      "tp_pcpn_annual_10" -> Array("8.16"),
      "tp_pcpn_annual_11" -> Array("3.79"),
      "tp_pcpn_annual_12" -> Array("10.65"),
      "tp_tmpc_annual_01" -> Array("39.5"),
      "tp_tmpc_annual_02" -> Array("30.7"),
      "tp_tmpc_annual_03" -> Array("41.5"),
      "tp_tmpc_annual_04" -> Array("46"),
      "tp_tmpc_annual_05" -> Array("54.9"),
      "tp_tmpc_annual_06" -> Array("57"),
      "tp_tmpc_annual_07" -> Array("60.9"),
      "tp_tmpc_annual_08" -> Array("63.6"),
      "tp_tmpc_annual_09" -> Array("56.6"),
      "tp_tmpc_annual_10" -> Array("45"),
      "tp_tmpc_annual_11" -> Array("41.7"),
      "tp_tmpc_annual_12" -> Array("38.2"),
      "tp_tmax_annual_01" -> Array("45.1"),
      "tp_tmax_annual_02" -> Array("36.4"),
      "tp_tmax_annual_03" -> Array("50.8"),
      "tp_tmax_annual_04" -> Array("53.6"),
      "tp_tmax_annual_05" -> Array("64.6"),
      "tp_tmax_annual_06" -> Array("66.5"),
      "tp_tmax_annual_07" -> Array("69.7"),
      "tp_tmax_annual_08" -> Array("73"),
      "tp_tmax_annual_09" -> Array("63.6"),
      "tp_tmax_annual_10" -> Array("52.5"),
      "tp_tmax_annual_11" -> Array("47.9"),
      "tp_tmax_annual_12" -> Array("42"),
      "tp_tmin_annual_01" -> Array("33.9"),
      "tp_tmin_annual_02" -> Array("25"),
      "tp_tmin_annual_03" -> Array("32.2"),
      "tp_tmin_annual_04" -> Array("38.5"),
      "tp_tmin_annual_05" -> Array("45.2"),
      "tp_tmin_annual_06" -> Array("47.5"),
      "tp_tmin_annual_07" -> Array("52.1"),
      "tp_tmin_annual_08" -> Array("54.1"),
      "tp_tmin_annual_09" -> Array("49.6"),
      "tp_tmin_annual_10" -> Array("37.6"),
      "tp_tmin_annual_11" -> Array("35.4"),
      "tp_tmin_annual_12" -> Array("34.3"),
      "tp_norm_data_year" -> Array("1"),
      "tp_pcpn_norm_01" -> Array("10.55"),
      "tp_pcpn_norm_02" -> Array("6.92"),
      "tp_pcpn_norm_03" -> Array("7.42"),
      "tp_pcpn_norm_04" -> Array("5.65"),
      "tp_pcpn_norm_05" -> Array("4.46"),
      "tp_pcpn_norm_06" -> Array("3.47"),
      "tp_pcpn_norm_07" -> Array("1.6"),
      "tp_pcpn_norm_08" -> Array("1.65"),
      "tp_pcpn_norm_09" -> Array("3.19"),
      "tp_pcpn_norm_10" -> Array("6.83"),
      "tp_pcpn_norm_11" -> Array("12.05"),
      "tp_pcpn_norm_12" -> Array("9.73"),
      "tp_tmpc_norm_01" -> Array("36.4"),
      "tp_tmpc_norm_02" -> Array("37.9"),
      "tp_tmpc_norm_03" -> Array("41.2"),
      "tp_tmpc_norm_04" -> Array("45.2"),
      "tp_tmpc_norm_05" -> Array("50.9"),
      "tp_tmpc_norm_06" -> Array("55.9"),
      "tp_tmpc_norm_07" -> Array("61.5"),
      "tp_tmpc_norm_08" -> Array("62"),
      "tp_tmpc_norm_09" -> Array("56.9"),
      "tp_tmpc_norm_10" -> Array("48"),
      "tp_tmpc_norm_11" -> Array("40"),
      "tp_tmpc_norm_12" -> Array("34.9"),
      "tp_tmax_norm_01" -> Array("41.2"),
      "tp_tmax_norm_02" -> Array("44.3"),
      "tp_tmax_norm_03" -> Array("48.6"),
      "tp_tmax_norm_04" -> Array("53.8"),
      "tp_tmax_norm_05" -> Array("60"),
      "tp_tmax_norm_06" -> Array("65.1"),
      "tp_tmax_norm_07" -> Array("71.9"),
      "tp_tmax_norm_08" -> Array("72.5"),
      "tp_tmax_norm_09" -> Array("66.7"),
      "tp_tmax_norm_10" -> Array("55.5"),
      "tp_tmax_norm_11" -> Array("45.1"),
      "tp_tmax_norm_12" -> Array("39.4"),
      "tp_tmin_norm_01" -> Array("31.6"),
      "tp_tmin_norm_02" -> Array("31.5"),
      "tp_tmin_norm_03" -> Array("33.7"),
      "tp_tmin_norm_04" -> Array("36.6"),
      "tp_tmin_norm_05" -> Array("41.7"),
      "tp_tmin_norm_06" -> Array("46.6"),
      "tp_tmin_norm_07" -> Array("51"),
      "tp_tmin_norm_08" -> Array("51.4"),
      "tp_tmin_norm_09" -> Array("47.1"),
      "tp_tmin_norm_10" -> Array("40.5"),
      "tp_tmin_norm_11" -> Array("34.9"),
      "tp_tmin_norm_12" -> Array("30.3")
    )

    val tempAndPrecipDataMapped =
      TemperaturePrecipitationTransformations.mapTemperaturePrecipitationVariables(
        RawRecord(1, tempAndPrecipData)
      )

    // output of the example record's temperature and preciptation transformations
    tempAndPrecipDataMapped.tpDataYear.value shouldBe 1L
    tempAndPrecipDataMapped.tpPcpnAnnual01 shouldBe Some(6.48)
    tempAndPrecipDataMapped.tpPcpnAnnual02 shouldBe Some(6.28)
    tempAndPrecipDataMapped.tpPcpnAnnual03 shouldBe Some(2.26)
    tempAndPrecipDataMapped.tpPcpnAnnual04 shouldBe Some(7.17)
    tempAndPrecipDataMapped.tpPcpnAnnual05 shouldBe Some(2.39)
    tempAndPrecipDataMapped.tpPcpnAnnual06 shouldBe Some(2.39)
    tempAndPrecipDataMapped.tpPcpnAnnual07 shouldBe Some(1.99)
    tempAndPrecipDataMapped.tpPcpnAnnual08 shouldBe Some(1.43)
    tempAndPrecipDataMapped.tpPcpnAnnual09 shouldBe Some(5.87)
    tempAndPrecipDataMapped.tpPcpnAnnual10 shouldBe Some(8.16)
    tempAndPrecipDataMapped.tpPcpnAnnual11 shouldBe Some(3.79)
    tempAndPrecipDataMapped.tpPcpnAnnual12 shouldBe Some(10.65)
    tempAndPrecipDataMapped.tpTmpcAnnual01 shouldBe Some(39.5)
    tempAndPrecipDataMapped.tpTmpcAnnual02 shouldBe Some(30.7)
    tempAndPrecipDataMapped.tpTmpcAnnual03 shouldBe Some(41.5)
    tempAndPrecipDataMapped.tpTmpcAnnual04 shouldBe Some(46)
    tempAndPrecipDataMapped.tpTmpcAnnual05 shouldBe Some(54.9)
    tempAndPrecipDataMapped.tpTmpcAnnual06 shouldBe Some(57)
    tempAndPrecipDataMapped.tpTmpcAnnual07 shouldBe Some(60.9)
    tempAndPrecipDataMapped.tpTmpcAnnual08 shouldBe Some(63.6)
    tempAndPrecipDataMapped.tpTmpcAnnual09 shouldBe Some(56.6)
    tempAndPrecipDataMapped.tpTmpcAnnual10 shouldBe Some(45)
    tempAndPrecipDataMapped.tpTmpcAnnual11 shouldBe Some(41.7)
    tempAndPrecipDataMapped.tpTmpcAnnual12 shouldBe Some(38.2)
    tempAndPrecipDataMapped.tpTmaxAnnual01 shouldBe Some(45.1)
    tempAndPrecipDataMapped.tpTmaxAnnual02 shouldBe Some(36.4)
    tempAndPrecipDataMapped.tpTmaxAnnual03 shouldBe Some(50.8)
    tempAndPrecipDataMapped.tpTmaxAnnual04 shouldBe Some(53.6)
    tempAndPrecipDataMapped.tpTmaxAnnual05 shouldBe Some(64.6)
    tempAndPrecipDataMapped.tpTmaxAnnual06 shouldBe Some(66.5)
    tempAndPrecipDataMapped.tpTmaxAnnual07 shouldBe Some(69.7)
    tempAndPrecipDataMapped.tpTmaxAnnual08 shouldBe Some(73)
    tempAndPrecipDataMapped.tpTmaxAnnual09 shouldBe Some(63.6)
    tempAndPrecipDataMapped.tpTmaxAnnual10 shouldBe Some(52.5)
    tempAndPrecipDataMapped.tpTmaxAnnual11 shouldBe Some(47.9)
    tempAndPrecipDataMapped.tpTmaxAnnual12 shouldBe Some(42)
    tempAndPrecipDataMapped.tpTminAnnual01 shouldBe Some(33.9)
    tempAndPrecipDataMapped.tpTminAnnual02 shouldBe Some(25)
    tempAndPrecipDataMapped.tpTminAnnual03 shouldBe Some(32.2)
    tempAndPrecipDataMapped.tpTminAnnual04 shouldBe Some(38.5)
    tempAndPrecipDataMapped.tpTminAnnual05 shouldBe Some(45.2)
    tempAndPrecipDataMapped.tpTminAnnual06 shouldBe Some(47.5)
    tempAndPrecipDataMapped.tpTminAnnual07 shouldBe Some(52.1)
    tempAndPrecipDataMapped.tpTminAnnual08 shouldBe Some(54.1)
    tempAndPrecipDataMapped.tpTminAnnual09 shouldBe Some(49.6)
    tempAndPrecipDataMapped.tpTminAnnual10 shouldBe Some(37.6)
    tempAndPrecipDataMapped.tpTminAnnual11 shouldBe Some(35.4)
    tempAndPrecipDataMapped.tpTminAnnual12 shouldBe Some(34.3)
    tempAndPrecipDataMapped.tpNormDataYear.value shouldBe 1L
    tempAndPrecipDataMapped.tpPcpnNorm01 shouldBe Some(10.55)
    tempAndPrecipDataMapped.tpPcpnNorm02 shouldBe Some(6.92)
    tempAndPrecipDataMapped.tpPcpnNorm03 shouldBe Some(7.42)
    tempAndPrecipDataMapped.tpPcpnNorm04 shouldBe Some(5.65)
    tempAndPrecipDataMapped.tpPcpnNorm05 shouldBe Some(4.46)
    tempAndPrecipDataMapped.tpPcpnNorm06 shouldBe Some(3.47)
    tempAndPrecipDataMapped.tpPcpnNorm07 shouldBe Some(1.6)
    tempAndPrecipDataMapped.tpPcpnNorm08 shouldBe Some(1.65)
    tempAndPrecipDataMapped.tpPcpnNorm09 shouldBe Some(3.19)
    tempAndPrecipDataMapped.tpPcpnNorm10 shouldBe Some(6.83)
    tempAndPrecipDataMapped.tpPcpnNorm11 shouldBe Some(12.05)
    tempAndPrecipDataMapped.tpPcpnNorm12 shouldBe Some(9.73)
    tempAndPrecipDataMapped.tpTmpcNorm01 shouldBe Some(36.4)
    tempAndPrecipDataMapped.tpTmpcNorm02 shouldBe Some(37.9)
    tempAndPrecipDataMapped.tpTmpcNorm03 shouldBe Some(41.2)
    tempAndPrecipDataMapped.tpTmpcNorm04 shouldBe Some(45.2)
    tempAndPrecipDataMapped.tpTmpcNorm05 shouldBe Some(50.9)
    tempAndPrecipDataMapped.tpTmpcNorm06 shouldBe Some(55.9)
    tempAndPrecipDataMapped.tpTmpcNorm07 shouldBe Some(61.5)
    tempAndPrecipDataMapped.tpTmpcNorm08 shouldBe Some(62)
    tempAndPrecipDataMapped.tpTmpcNorm09 shouldBe Some(56.9)
    tempAndPrecipDataMapped.tpTmpcNorm10 shouldBe Some(48)
    tempAndPrecipDataMapped.tpTmpcNorm11 shouldBe Some(40)
    tempAndPrecipDataMapped.tpTmpcNorm12 shouldBe Some(34.9)
    tempAndPrecipDataMapped.tpTmaxNorm01 shouldBe Some(41.2)
    tempAndPrecipDataMapped.tpTmaxNorm02 shouldBe Some(44.3)
    tempAndPrecipDataMapped.tpTmaxNorm03 shouldBe Some(48.6)
    tempAndPrecipDataMapped.tpTmaxNorm04 shouldBe Some(53.8)
    tempAndPrecipDataMapped.tpTmaxNorm05 shouldBe Some(60)
    tempAndPrecipDataMapped.tpTmaxNorm06 shouldBe Some(65.1)
    tempAndPrecipDataMapped.tpTmaxNorm07 shouldBe Some(71.9)
    tempAndPrecipDataMapped.tpTmaxNorm08 shouldBe Some(72.5)
    tempAndPrecipDataMapped.tpTmaxNorm09 shouldBe Some(66.7)
    tempAndPrecipDataMapped.tpTmaxNorm10 shouldBe Some(55.5)
    tempAndPrecipDataMapped.tpTmaxNorm11 shouldBe Some(45.1)
    tempAndPrecipDataMapped.tpTmaxNorm12 shouldBe Some(39.4)
    tempAndPrecipDataMapped.tpTminNorm01 shouldBe Some(31.6)
    tempAndPrecipDataMapped.tpTminNorm02 shouldBe Some(31.5)
    tempAndPrecipDataMapped.tpTminNorm03 shouldBe Some(33.7)
    tempAndPrecipDataMapped.tpTminNorm04 shouldBe Some(36.6)
    tempAndPrecipDataMapped.tpTminNorm05 shouldBe Some(41.7)
    tempAndPrecipDataMapped.tpTminNorm06 shouldBe Some(46.6)
    tempAndPrecipDataMapped.tpTminNorm07 shouldBe Some(51)
    tempAndPrecipDataMapped.tpTminNorm08 shouldBe Some(51.4)
    tempAndPrecipDataMapped.tpTminNorm09 shouldBe Some(47.1)
    tempAndPrecipDataMapped.tpTminNorm10 shouldBe Some(40.5)
    tempAndPrecipDataMapped.tpTminNorm11 shouldBe Some(34.9)
    tempAndPrecipDataMapped.tpTminNorm12 shouldBe Some(30.3)
  }
}
