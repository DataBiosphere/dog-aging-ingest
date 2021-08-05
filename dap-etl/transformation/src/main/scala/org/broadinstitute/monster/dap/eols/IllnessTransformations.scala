package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.EolsIllness

object IllnessTransformations {

  /**
    * Parse all eol_euthan data out of a raw RedCap record,
    * injecting them into a partially-modeled Eols record.
    */
  def mapIllnessFields(rawRecord: RawRecord): EolsIllness = {
    val cancerConditions = Some(rawRecord.getArray("eol_cancer_a").map(_.toLong))
    val otherCancerCondition = cancerConditions.map(_.contains(98L))
    val cancerNameKnown = rawRecord.getOptionalNumber("eol_cancer_b")
    val infectionName = rawRecord.getOptionalNumber("eol_infection_a")
    val infectionSystem = rawRecord.getOptionalNumber("eol_infection_b")
    val otherIllness = rawRecord.getOptionalNumber("eol_otherillness_a")
    val diagnosisKnown = rawRecord.getOptionalBoolean("eol_otherillness_b")
    val illnessTreatment = rawRecord.getOptionalNumber("eol_illness_c")
    EolsIllness(
      eolIllnessType = rawRecord.getOptionalNumber("eol_illness_a"),
      eolIllnessCancerAdrenal = cancerConditions.map(_.contains(1L)),
      eolIllnessCancerAnalSac = cancerConditions.map(_.contains(2L)),
      eolIllnessCancerBladderUrethra = cancerConditions.map(_.contains(3L)),
      eolIllnessCancerBlood = cancerConditions.map(_.contains(4L)),
      eolIllnessCancerBoneJoint = cancerConditions.map(_.contains(5L)),
      eolIllnessCancerBrain = cancerConditions.map(_.contains(6L)),
      eolIllnessCancerMammary = cancerConditions.map(_.contains(7L)),
      eolIllnessCancerCardiac = cancerConditions.map(_.contains(8L)),
      eolIllnessCancerEar = cancerConditions.map(_.contains(9L)),
      eolIllnessCancerEsophagus = cancerConditions.map(_.contains(10L)),
      eolIllnessCancerEye = cancerConditions.map(_.contains(11L)),
      eolIllnessCancerGallbladder = cancerConditions.map(_.contains(12L)),
      eolIllnessCancerGastro = cancerConditions.map(_.contains(13L)),
      eolIllnessCancerKidney = cancerConditions.map(_.contains(14L)),
      eolIllnessCancerLiver = cancerConditions.map(_.contains(15L)),
      eolIllnessCancerLung = cancerConditions.map(_.contains(16L)),
      eolIllnessCancerLymphNodes = cancerConditions.map(_.contains(17L)),
      eolIllnessCancerMuscle = cancerConditions.map(_.contains(18L)),
      eolIllnessCancerNose = cancerConditions.map(_.contains(19L)),
      eolIllnessCancerNerveSheath = cancerConditions.map(_.contains(20L)),
      eolIllnessCancerOral = cancerConditions.map(_.contains(21L)),
      eolIllnessCancerOvaryUterus = cancerConditions.map(_.contains(22L)),
      eolIllnessCancerPancreas = cancerConditions.map(_.contains(23L)),
      eolIllnessCancerPerianal = cancerConditions.map(_.contains(24L)),
      eolIllnessCancerPituitary = cancerConditions.map(_.contains(25L)),
      eolIllnessCancerProstate = cancerConditions.map(_.contains(26L)),
      eolIllnessCancerRectum = cancerConditions.map(_.contains(27L)),
      eolIllnessCancerSkinTrunkBodyHead = cancerConditions.map(_.contains(28L)),
      eolIllnessCancerSkinLimbFood = cancerConditions.map(_.contains(29L)),
      eolIllnessCancerSpinalCord = cancerConditions.map(_.contains(30L)),
      eolIllnessCancerSpleen = cancerConditions.map(_.contains(31L)),
      eolIllnessCancerTesticle = cancerConditions.map(_.contains(32L)),
      eolIllnessCancerThyroid = cancerConditions.map(_.contains(33L)),
      eolIllnessCancerVenereal = cancerConditions.map(_.contains(34L)),
      eolIllnessCancerUnidentifiedThorax = cancerConditions.map(_.contains(35L)),
      eolIllnessCancerUnidentifiedAbdomen = cancerConditions.map(_.contains(36L)),
      eolIllnessCancerUnknown = cancerConditions.map(_.contains(99L)),
      eolIllnessCancerOther = otherCancerCondition,
      eolIllnessCancerOtherDescription =
        if (otherCancerCondition.contains(true))
          rawRecord.getOptionalStripped("eol_cancer_a_specify")
        else None,
      eolIllnessCancerNameKnown = cancerNameKnown,
      eolIllnessCancerNameDescription =
        if (cancerNameKnown.contains(1L))
          rawRecord.getOptionalStripped("eol_cancer_b_specify")
        else None,
      eolIllnessInfection = infectionName,
      eolIllnessInfectionOtherDescription =
        if (infectionName.contains(98L))
          rawRecord.getOptionalStripped("eol_infection_a_specify")
        else None,
      eolIllnessInfectionSystem = infectionSystem,
      eolIllnessInfectionSystemOtherDescription =
        if (infectionSystem.contains(98L))
          rawRecord.getOptionalStripped("eol_infection_b_specify")
        else None,
      eolIllnessOther = otherIllness,
      eolIllnessOtherOtherDescription =
        if (otherIllness.contains(98L))
          rawRecord.getOptionalStripped("eol_otherillness_a_specify")
        else None,
      eolIllnessOtherDiagnosis = diagnosisKnown,
      eolIllnessOtherDiagnosisDescription =
        if (diagnosisKnown.contains(true))
          rawRecord.getOptionalStripped("eol_otherillness_b_specify")
        else None,
      eolIllnessAwarenessTimeframe = rawRecord.getOptionalNumber("eol_illness_b"),
      eolIllnessTreatment = illnessTreatment,
      eolIllnessTreatmentOtherDescription =
        if (illnessTreatment.contains(98L))
          rawRecord.getOptionalStripped("eol_illness_c_explain")
        else None
    )
  }
}
