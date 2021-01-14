package org.broadinstitute.monster.dap

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class RawRecordSpec extends AnyFlatSpec with Matchers {
  behavior of "RawRecord"

  it should "parse an optional datetime properly" in {
    val fields =
      Map("wv_walkscore_date" -> Array("2020-07-26 02:58:54.112005", "2020-07-26 02:58:54"))

    val record = RawRecord(123L, fields)
    val result: Option[LocalDate] = record.getOptionalDateTime("wv_walkscore_date")
    result shouldBe defined
    result shouldBe Some(LocalDate.parse("2020-07-26"))
  }

  it should "strip tabs and both styles of newline when using getOptionalStripped" in {
    val fields =
      Map(
        "dd_steve_greatness_other" -> Array(
          "Much has\tbeen written\nabout the\r\ngreatness\nof Steve[clipped remaining 4098 chars]"
        )
      )

    val record = RawRecord(456L, fields)
    val result: Option[String] = record.getOptionalStripped("dd_steve_greatness_other")
    result shouldBe defined
    result shouldBe Some(
      "Much has been written about the greatness of Steve[clipped remaining 4098 chars]"
    )
  }
}
