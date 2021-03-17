package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dap.cslb.CslbTransformations

import java.time.LocalDate
import java.time.format.DateTimeParseException
import org.broadinstitute.monster.dogaging.jadeschema.table.Cslb
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class CslbTransformationSpec extends AnyFlatSpec {
  behavior of "CslbTransformations"

  private val exampleCslbFields = Map[String, Array[String]](
    "study_id" -> Array("12345"),
    "cslb_date" -> Array("2020-11-19"),
    "cslb_pace" -> Array("4"),
    "cslb_stare" -> Array("4"),
    "cslb_stuck" -> Array("4"),
    "cslb_recognize" -> Array("4"),
    "cslb_walk_walls" -> Array("4"),
    "cslb_avoid" -> Array("4"),
    "cslb_find_food" -> Array("4"),
    "cslb_pace_6mo" -> Array("4"),
    "cslb_stare_6mo" -> Array("4"),
    "cslb_defecate_6mo" -> Array("4"),
    "cslb_food_6mo" -> Array("4"),
    "cslb_recognize_6mo" -> Array("4"),
    "cslb_active_6mo" -> Array("4"),
    "cslb_score" -> Array("52"),
    "cslb_other_changes" -> Array("No new changes")
  )

  it should "strip unwanted DOS and unix newlines from cslb_other_changes" in {
    val exampleCslbRecord = RawRecord(
      id = 1,
      exampleCslbFields.updated(
        "cslb_other_changes",
        Array("this is some text\r\nwith a DOS line break\nand a unix line break")
      )
    )

    val output = CslbTransformations.mapCslbData(exampleCslbRecord).get

    output.cslbOtherChanges shouldBe Some(
      "this is some text with a DOS line break and a unix line break"
    )
  }

  it should "return None when not processing a cslb record" in {
    val exampleNonCslbRecord = RawRecord(id = 1, Map("foo" -> Array("Bar")))
    val output = CslbTransformations.mapCslbData(exampleNonCslbRecord)
    output shouldBe None
  }

  it should "raise when date is invalid" in {
    val invalidDateRecord = RawRecord(id = 1, Map("cslb_date" -> Array("2020-142-124")))
    assertThrows[DateTimeParseException] {
      CslbTransformations.mapCslbData(invalidDateRecord)
    }
  }

  it should "correctly maps cslb values" in {
    val exampleCslbRecord = RawRecord(id = 1, exampleCslbFields)
    val output = CslbTransformations.mapCslbData(exampleCslbRecord).get
    output.dogId shouldBe 12345L
    output.cslbDate shouldBe LocalDate.parse("2020-11-19")
    output.cslbPace shouldBe Some(4)
    output.cslbStare shouldBe Some(4)
    output.cslbStuck shouldBe Some(4)
    output.cslbRecognize shouldBe Some(4)
    output.cslbWalkWalls shouldBe Some(4)
    output.cslbAvoid shouldBe Some(4)
    output.cslbFindFood shouldBe Some(4)
    output.cslbPace6mo shouldBe Some(4)
    output.cslbStare6mo shouldBe Some(4)
    output.cslbDefecate6mo shouldBe Some(4)
    output.cslbFood6mo shouldBe Some(4)
    output.cslbRecognize6mo shouldBe Some(4)
    output.cslbActive6mo shouldBe Some(4)
    output.cslbScore shouldBe Some(52)
    output.cslbOtherChanges shouldBe Some("No new changes")
  }

  it should "correctly map data when optional fields are null" in {
    val emptyRecord =
      RawRecord(id = 1, Map("study_id" -> Array("1234"), "cslb_date" -> Array("2020-11-19")))
    val output = CslbTransformations.mapCslbData(emptyRecord).get
    output shouldBe
      Cslb(
        1234,
        LocalDate.parse("2020-11-19"),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None
      )
  }
}
