package org.broadinstitute.monster.dap

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class TestA(firstField: String, secondField: String)
case class OptionalTestA(firstField: String, option: Option[Long], thirdField: String)
case class TestB(firstField2: String, secondField1: String)
case class Inner(innerOne: String, innerTwo: Int)
case class InnerAgain(innerAgainOne: Long, innerAgainTwo: Boolean)
case class TestC(id: Double, inner: Inner, innerAgain: InnerAgain)

class CaseClassInspectorSpec extends AnyFlatSpec with Matchers {
  behavior of "CaseClassInspector"

  it should "generate snake-cased headers for a flat case class" in {
    CaseClassInspector.snakeCaseHeaderList[TestA] should contain theSameElementsInOrderAs (Seq(
      "first_field",
      "second_field"
    ))
  }

  it should "properly snakify numeric field names" in {
    CaseClassInspector.snakeCaseHeaderList[TestB] should contain theSameElementsInOrderAs (Seq(
      "first_field_2",
      "second_field_1"
    ))
  }

  it should "flatten fields from nested case classes in the order their case classes appear" in {
    CaseClassInspector.snakeCaseHeaderList[TestC] should contain theSameElementsInOrderAs (Seq(
      "id",
      "inner_one",
      "inner_two",
      "inner_again_one",
      "inner_again_two"
    ))
  }

  it should "properly handle optional fields" in {
    CaseClassInspector
      .snakeCaseHeaderList[OptionalTestA] should contain theSameElementsInOrderAs (Seq(
      "first_field",
      "option",
      "third_field"
    ))
  }

  it should "produce a tab-delimited header row from a field list" in {
    CaseClassInspector.snakeCaseHeaderRow[TestB]("\t") should be("first_field_2\tsecond_field_1")
  }
}
