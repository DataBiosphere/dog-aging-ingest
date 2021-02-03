package org.broadinstitute.monster.dap

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import io.circe.Json
import io.circe.syntax._

class TsvUtilsSpec extends AnyFlatSpec with Matchers {
  behavior of "TsvUtils"

  "stringSeqToTsvRow" should "turn a row of strings into a TSV row" in {
    TsvUtils.stringSeqToTsvRow(Seq("abc", "def", "123.45")) should be("abc\tdef\t123.45\n")
  }

  it should "escape single quotes in fields" in {
    TsvUtils.stringSeqToTsvRow(Seq("steve pletcher", "wrote a \"test case\"")) should be(
      "steve pletcher\t\"wrote a \"\"test case\"\"\"\n"
    )
  }

  it should "wrap fields containing delimiter characters in quotes" in {
    TsvUtils.stringSeqToTsvRow(Seq("tabbity\ttabs", "normal_field")) should be(
      "\"tabbity\ttabs\"\tnormal_field\n"
    )
  }

  "jsonElementToString" should "convert JSON strings to regular strings" in {
    TsvUtils.jsonElementToString("steve".asJson) should be("steve")
  }

  it should "convert JSON booleans to the string true or false" in {
    TsvUtils.jsonElementToString(true.asJson) should be("true")
    TsvUtils.jsonElementToString(false.asJson) should be("false")
  }

  it should "convert JSON integers to strings" in {
    TsvUtils.jsonElementToString(4.asJson) should be("4")
    TsvUtils.jsonElementToString(5L.asJson) should be("5")
  }

  it should "convert JSON decimals to strings" in {
    TsvUtils.jsonElementToString(4.65.asJson) should be("4.65")
    TsvUtils.jsonElementToString(501.2222233333d.asJson) should be("501.2222233333")
  }

  it should "convert null to the empty string" in {
    TsvUtils.jsonElementToString(Json.Null) should be("")
  }

  "jsonStringToTsvString" should "translate JSON objects to TSV rows" in {
    TsvUtils.jsonStringToTsvString("{\"a\": \"b\", \"c\": \"d\"}", Seq("a", "c"), x => x) should be(
      "b\td\n"
    )
  }

  it should "apply the provided transform function to the json object" in {
    TsvUtils.jsonStringToTsvString(
      "{\"a\": \"b\", \"c\": \"d\"}",
      Seq("a", "c", "e"),
      x => x.add("e", "f".asJson)
    ) should be("b\td\tf\n")
  }

  it should "skip over values not in the provided headers" in {
    TsvUtils.jsonStringToTsvString(
      "{\"a\": \"b\", \"c\": \"d\"}",
      Seq("a", "e"),
      x => x.add("e", "f".asJson)
    ) should be("b\tf\n")
  }

  it should "leave headers with no corresponding key in the json blank" in {
    TsvUtils.jsonStringToTsvString(
      "{\"a\": \"b\", \"c\": \"d\"}",
      Seq("a", "c", "steve"),
      x => x
    ) should be("b\td\t\n")
  }
}
