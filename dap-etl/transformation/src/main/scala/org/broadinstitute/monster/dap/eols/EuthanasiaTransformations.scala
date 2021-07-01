package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.EolsEuthan

object EuthanasiaTransformations {

  /**
    * Parse all eol_euthan data out of a raw RedCap record,
    * injecting them into a partially-modeled Eols record.
    */
  def mapEuthanasiaFields(rawRecord: RawRecord): EolsEuthan = {
    val euthanasiaWho = rawRecord.getOptionalNumber("eol_euthan_who")
    val euthanasiaWhy = rawRecord.getOptionalNumber("eol_euthan_why1")
    val euthanasiaWhy2 = Some(rawRecord.getArray("eol_euthan_why2").map(_.toLong))
    val euthanasiaWhyOther = euthanasiaWhy2.map(_.contains(98L))
    EolsEuthan(
      eolEuthan = rawRecord.getOptionalBoolean("eol_euthan_yn"),
      eolEuthanWho = euthanasiaWho,
      eolEuthanWhoOtherDescription =
        if (euthanasiaWho.contains(98)) rawRecord.getOptionalStripped("eol_euthan_who_specify")
        else None,
      eolEuthanMainReason = euthanasiaWhy,
      eolEuthanMainReasonOtherDescription =
        if (euthanasiaWhy.contains(98)) rawRecord.getOptionalStripped("eol_euthan_why1_specify")
        else None,
      eolEuthanAddReasonNone = euthanasiaWhy2.map(_.contains(0L)),
      eolEuthanAddReasonQualityOfLife = euthanasiaWhy2.map(_.contains(1L)),
      eolEuthanAddReasonPain = euthanasiaWhy2.map(_.contains(2L)),
      eolEuthanAddReasonPrognosis = euthanasiaWhy2.map(_.contains(3L)),
      eolEuthanAddReasonMedicProb = euthanasiaWhy2.map(_.contains(4L)),
      eolEuthanAddReasonBehaviorProb = euthanasiaWhy2.map(_.contains(5L)),
      eolEuthanAddReasonHarmToAnother = euthanasiaWhy2.map(_.contains(6L)),
      eolEuthanAddReasonIncompatible = euthanasiaWhy2.map(_.contains(7L)),
      eolEuthanAddReasonCost = euthanasiaWhy2.map(_.contains(8L)),
      eolEuthanAddReasonOther = euthanasiaWhyOther,
      eolEuthanAddReasonOtherDescription =
        if (euthanasiaWhyOther.contains(true))
          rawRecord.getOptionalStripped("eol_euthan_why2_specify")
        else None
    )
  }
}
