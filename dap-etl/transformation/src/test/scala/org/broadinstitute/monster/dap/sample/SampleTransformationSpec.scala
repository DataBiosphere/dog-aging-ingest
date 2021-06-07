package org.broadinstitute.monster.dap.sample

import org.broadinstitute.monster.dap.common.RawRecord
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeParseException}

class SampleTransformationSpec extends AnyFlatSpec {
  behavior of "SampleTransformations"

  private val exampleSampleFields = Map[String, Array[String]](
    "study_id" -> Array("12345"),
    "ce_enroll_stat" -> Array("9"),
    "sample_type" -> Array("saliva_DNA_lowcov"),
    "k1_tube_serial" -> Array("54321"),
    "k1_rtn_tracking_date" -> Array("2021-05-20 00:00:00")
  )

  it should "return None when not processing a sample record" in {
    val exampleNonSampleRecord = RawRecord(id = 1, Map("foo" -> Array("Bar")))
    val output = SampleTransformations.mapSampleData(exampleNonSampleRecord)
    output shouldBe None
  }

  it should "raise when date is invalid" in {
    for ((k, v) <- exampleSampleFields) printf("key: %s, value: %s\n", k, v)
    val invalidDateRecord = RawRecord(id = 1, Map("k1_rtn_tracking_date" -> Array("2020-142-124")))
    assertThrows[DateTimeParseException] {
      SampleTransformations.mapSampleData(invalidDateRecord)
    }
  }

  it should "correctly maps sample values" in {
    val DAPDateTimeFormatter: DateTimeFormatter =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]")
    val exampleSampleRecord = RawRecord(id = 1, exampleSampleFields)
    val output = SampleTransformations.mapSampleData(exampleSampleRecord).get
    output.dogId shouldBe 12345L
    output.cohort shouldBe 9L
    output.sampleId shouldBe Some(54321L)
    output.sampleType shouldBe "saliva_DNA_lowcov"
    output.dateCollected shouldBe LocalDate.parse("2021-05-20 00:00:00", DAPDateTimeFormatter)
  }
}
