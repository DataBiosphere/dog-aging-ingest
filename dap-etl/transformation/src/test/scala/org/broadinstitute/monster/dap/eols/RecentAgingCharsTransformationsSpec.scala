package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dap.eols.RecentAgingCharsTransformations
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RecentAgingCharsTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {

  behavior of "RecentAgingCharsTransformations"

  it should "map recent aging characteristics where available" in {
    val recentAgingChars = Map(
      "eol_aging_characteristics" -> Array("2", "3", "17", "98"),
      "eol_aging_characteristics_specify" -> Array("Other description")
    )

    val recentAgingCharsMapped = RecentAgingCharsTransformations.mapRecentAgingChars(
      RawRecord(1, recentAgingChars)
    )

    // output of the example record's recent aging characteristics transformations
    recentAgingCharsMapped.eolRecentAgingCharDeaf shouldBe Some(true)
    recentAgingCharsMapped.eolRecentAgingCharWeightloss shouldBe Some(true)
    recentAgingCharsMapped.eolRecentAgingCharRecognition shouldBe Some(true)
    recentAgingCharsMapped.eolRecentAgingCharOther shouldBe Some(true)
    recentAgingCharsMapped.eolRecentAgingCharOtherDescription shouldBe
      Some("Other description")
  }
}
