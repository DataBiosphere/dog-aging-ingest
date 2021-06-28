package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RecentSymptomsTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {

  behavior of "RecentSymptomsTransformations"

  it should "map recent medical symptoms where available" in {
    val recentMedSymptoms = Map(
      "eol_med_symptoms" -> Array("1", "5", "16", "98"),
      "eol_med_symptoms_specify" -> Array("Other description"),
      "eol_qol" -> Array("7"),
      "eol_qol_declined" -> Array("4"),
      "eol_contribute_qol_decined" -> Array("98"),
      "eol_qol_declined_specify" -> Array("Other contributing factor"),
      "eol_vet_discuss_yn" -> Array("1"),
      "eol_stayed_at_vet" -> Array("1"),
      "eol_length_stayed_at_vet" -> Array("1"),
      "eol_sedation_yn" -> Array("9"),
      "eol_understand_prognosis" -> Array("5")
    )

    val recentSymptomsMapped = RecentSymptomsTransformations.mapRecentSymptoms(
      RawRecord(1, recentMedSymptoms)
    )

    // output of the example record's recent symptoms transformations
    recentSymptomsMapped.eolRecentSymptomLethargy shouldBe Some(true)
    recentSymptomsMapped.eolRecentSymptomWeight shouldBe Some(true)
    recentSymptomsMapped.eolRecentSymptomSwolenAbdomen shouldBe Some(true)
    recentSymptomsMapped.eolRecentSymptomOther shouldBe Some(true)
    recentSymptomsMapped.eolRecentSymptomOtherDescription shouldBe
      Some("Other description")
    recentSymptomsMapped.eolRecentQol shouldBe Some(7L)
    recentSymptomsMapped.eolQolDeclined shouldBe Some(4L)
    recentSymptomsMapped.eolQolDeclinedReason shouldBe Some(98L)
    recentSymptomsMapped.eolQolDeclinedReasonOtherDescription shouldBe
      Some("Other contributing factor")
    recentSymptomsMapped.eolRecentVetDiscuss shouldBe Some(1L)
    recentSymptomsMapped.eolRecentVetStay shouldBe Some(1L)
    recentSymptomsMapped.eolRecentVetStayLength shouldBe Some(1L)
    recentSymptomsMapped.eolRecentSedation shouldBe Some(9L)
    recentSymptomsMapped.eolUnderstandPrognosis shouldBe Some(5L)
  }
}
