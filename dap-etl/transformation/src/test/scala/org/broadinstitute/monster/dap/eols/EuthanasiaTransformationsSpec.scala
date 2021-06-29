package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EuthanasiaTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {

  behavior of "EuthanasiaTransformations"

  it should "map euthanasia data where available" in {
    val euthanasiaData = Map(
      "eol_euthan_yn" -> Array("1"),
      "eol_euthan_who" -> Array("1"),
      "eol_euthan_why1" -> Array("2"),
      "eol_euthan_why2" -> Array("0", "1", "2", "3", "4")
    )

    val euthanasiaMapped = EuthanasiaTransformations.mapEuthanasiaFields(
      RawRecord(1, euthanasiaData)
    )

    // output of the example record's euthanasia transformations
    euthanasiaMapped.eolEuthan shouldBe Some(true)
    euthanasiaMapped.eolEuthanWho shouldBe Some(1L)
    euthanasiaMapped.eolEuthanMainReason shouldBe Some(2L)
    euthanasiaMapped.eolEuthanAddReasonNone shouldBe Some(true)
    euthanasiaMapped.eolEuthanAddReasonQualityOfLife shouldBe Some(true)
    euthanasiaMapped.eolEuthanAddReasonPain shouldBe Some(true)
    euthanasiaMapped.eolEuthanAddReasonPrognosis shouldBe Some(true)
    euthanasiaMapped.eolEuthanAddReasonMedicProb shouldBe Some(true)
  }

  it should "map other responses where available" in {
    val euthanasiaData = Map(
      "eol_euthan_yn" -> Array("1"),
      "eol_euthan_who" -> Array("98"),
      "eol_euthan_who_specify" -> Array("A professional euthanasia service"),
      "eol_euthan_why1" -> Array("98"),
      "eol_euthan_why1_specify" -> Array("Other reason not listed"),
      "eol_euthan_why2" -> Array("5", "6", "7", "8", "98"),
      "eol_euthan_why2_specify" -> Array("Some other reason")
    )

    val euthanasiaMapped = EuthanasiaTransformations.mapEuthanasiaFields(
      RawRecord(1, euthanasiaData)
    )

    // output of the example record's euthanasia transformations
    euthanasiaMapped.eolEuthan shouldBe Some(true)
    euthanasiaMapped.eolEuthanWho shouldBe Some(98L)
    euthanasiaMapped.eolEuthanWhoOtherDescription shouldBe Some("A professional euthanasia service")
    euthanasiaMapped.eolEuthanMainReason shouldBe Some(98L)
    euthanasiaMapped.eolEuthanMainReasonOtherDescription shouldBe Some("Other reason not listed")
    euthanasiaMapped.eolEuthanAddReasonBehaviorProb shouldBe Some(true)
    euthanasiaMapped.eolEuthanAddReasonHarmToAnother shouldBe Some(true)
    euthanasiaMapped.eolEuthanAddReasonIncompatible shouldBe Some(true)
    euthanasiaMapped.eolEuthanAddReasonCost shouldBe Some(true)
    euthanasiaMapped.eolEuthanAddReasonOther shouldBe Some(true)
    euthanasiaMapped.eolEuthanAddReasonOtherDescription shouldBe Some("Some other reason")
  }
}
