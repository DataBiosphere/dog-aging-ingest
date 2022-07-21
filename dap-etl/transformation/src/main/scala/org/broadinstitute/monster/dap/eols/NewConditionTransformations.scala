package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.EolsNewCondition

object NewConditionTransformations {

  def mapNewConditionMetadata(rawRecord: RawRecord): EolsNewCondition = {
    val diagnosisType = rawRecord.fields.get("eol_dx")
    val infectiousDisease = diagnosisType.map(_.contains("1"))
    val toxinConsumption = diagnosisType.map(_.contains("2"))
    val trauma = diagnosisType.map(_.contains("3"))
    val cancer = diagnosisType.map(_.contains("4"))
    val eye = diagnosisType.map(_.contains("5"))
    val ear = diagnosisType.map(_.contains("6"))
    val oral = diagnosisType.map(_.contains("7"))
    val skin = diagnosisType.map(_.contains("8"))
    val cardiac = diagnosisType.map(_.contains("9"))
    val respiratory = diagnosisType.map(_.contains("10"))
    val gastrointestinal = diagnosisType.map(_.contains("11"))
    val liver = diagnosisType.map(_.contains("12"))
    val kidney = diagnosisType.map(_.contains("13"))
    val reproductive = diagnosisType.map(_.contains("14"))
    val orthopedic = diagnosisType.map(_.contains("15"))
    val neurological = diagnosisType.map(_.contains("16"))
    val endocrine = diagnosisType.map(_.contains("17"))
    val hematologic = diagnosisType.map(_.contains("18"))
    val immune = diagnosisType.map(_.contains("19"))
    val other = diagnosisType.map(_.contains("20"))
    EolsNewCondition(
      eolNewConditionNone = diagnosisType.map(_.contains("0")),
      eolNewConditionInfectiousDisease = infectiousDisease,
      eolNewConditionToxinConsumption = toxinConsumption,
      eolNewConditionTrauma = trauma,
      eolNewConditionCancer = cancer,
      eolNewConditionEye = eye,
      eolNewConditionEar = ear,
      eolNewConditionOral = oral,
      eolNewConditionSkin = skin,
      eolNewConditionCardiac = cardiac,
      eolNewConditionRespiratory = respiratory,
      eolNewConditionGastrointestinal = gastrointestinal,
      eolNewConditionLiver = liver,
      eolNewConditionKidney = kidney,
      eolNewConditionReproductive = reproductive,
      eolNewConditionOrthopedic = orthopedic,
      eolNewConditionNeurological = neurological,
      eolNewConditionEndocrine = endocrine,
      eolNewConditionHematologic = hematologic,
      eolNewConditionImmune = immune,
      eolNewConditionOther = other,
      eolNewConditionInfectiousDiseaseMonth =
        if (infectiousDisease.contains(true)) rawRecord.getOptionalNumber("eol_dx_month1")
        else None,
      eolNewConditionInfectiousDiseaseYear =
        if (infectiousDisease.contains(true)) rawRecord.getOptionalNumber("eol_dx_year1") else None,
      eolNewConditionInfectiousDiseaseSpecify =
        if (infectiousDisease.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify1")
        else None,
      eolNewConditionToxinConsumptionMonth =
        if (toxinConsumption.contains(true)) rawRecord.getOptionalNumber("eol_dx_month2") else None,
      eolNewConditionToxinConsumptionYear =
        if (toxinConsumption.contains(true)) rawRecord.getOptionalNumber("eol_dx_year2") else None,
      eolNewConditionToxinConsumptionSpecify =
        if (toxinConsumption.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify2")
        else None,
      eolNewConditionTraumaMonth =
        if (trauma.contains(true)) rawRecord.getOptionalNumber("eol_dx_month3") else None,
      eolNewConditionTraumaYear =
        if (trauma.contains(true)) rawRecord.getOptionalNumber("eol_dx_year3") else None,
      eolNewConditionTraumaSpecify =
        if (trauma.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify3") else None,
      eolNewConditionCancerMonth =
        if (cancer.contains(true)) rawRecord.getOptionalNumber("eol_dx_month4") else None,
      eolNewConditionCancerYear =
        if (cancer.contains(true)) rawRecord.getOptionalNumber("eol_dx_year4") else None,
      eolNewConditionCancerSpecify =
        if (cancer.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify4") else None,
      eolNewConditionEyeMonth =
        if (eye.contains(true)) rawRecord.getOptionalNumber("eol_dx_month5") else None,
      eolNewConditionEyeYear =
        if (eye.contains(true)) rawRecord.getOptionalNumber("eol_dx_year5") else None,
      eolNewConditionEyeSpecify =
        if (eye.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify5") else None,
      eolNewConditionEarMonth =
        if (ear.contains(true)) rawRecord.getOptionalNumber("eol_dx_month6") else None,
      eolNewConditionEarYear =
        if (ear.contains(true)) rawRecord.getOptionalNumber("eol_dx_year6") else None,
      eolNewConditionEarSpecify =
        if (ear.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify6") else None,
      eolNewConditionOralMonth =
        if (oral.contains(true)) rawRecord.getOptionalNumber("eol_dx_month7") else None,
      eolNewConditionOralYear =
        if (oral.contains(true)) rawRecord.getOptionalNumber("eol_dx_year7") else None,
      eolNewConditionOralSpecify =
        if (oral.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify7") else None,
      eolNewConditionSkinMonth =
        if (skin.contains(true)) rawRecord.getOptionalNumber("eol_dx_month8") else None,
      eolNewConditionSkinYear =
        if (skin.contains(true)) rawRecord.getOptionalNumber("eol_dx_year8") else None,
      eolNewConditionSkinSpecify =
        if (skin.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify8") else None,
      eolNewConditionCardiacMonth =
        if (cardiac.contains(true)) rawRecord.getOptionalNumber("eol_dx_month9") else None,
      eolNewConditionCardiacYear =
        if (cardiac.contains(true)) rawRecord.getOptionalNumber("eol_dx_year9") else None,
      eolNewConditionCardiacSpecify =
        if (cardiac.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify9") else None,
      eolNewConditionRespiratoryMonth =
        if (respiratory.contains(true)) rawRecord.getOptionalNumber("eol_dx_month10") else None,
      eolNewConditionRespiratoryYear =
        if (respiratory.contains(true)) rawRecord.getOptionalNumber("eol_dx_year10") else None,
      eolNewConditionRespiratorySpecify =
        if (respiratory.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify10") else None,
      eolNewConditionGastrointestinalMonth =
        if (gastrointestinal.contains(true)) rawRecord.getOptionalNumber("eol_dx_month11")
        else None,
      eolNewConditionGastrointestinalYear =
        if (gastrointestinal.contains(true)) rawRecord.getOptionalNumber("eol_dx_year11") else None,
      eolNewConditionGastrointestinalSpecify =
        if (gastrointestinal.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify11")
        else None,
      eolNewConditionLiverMonth =
        if (liver.contains(true)) rawRecord.getOptionalNumber("eol_dx_month12") else None,
      eolNewConditionLiverYear =
        if (liver.contains(true)) rawRecord.getOptionalNumber("eol_dx_year12") else None,
      eolNewConditionLiverSpecify =
        if (liver.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify12") else None,
      eolNewConditionKidneyMonth =
        if (kidney.contains(true)) rawRecord.getOptionalNumber("eol_dx_month13") else None,
      eolNewConditionKidneyYear =
        if (kidney.contains(true)) rawRecord.getOptionalNumber("eol_dx_year13") else None,
      eolNewConditionKidneySpecify =
        if (kidney.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify13") else None,
      eolNewConditionReproductiveMonth =
        if (reproductive.contains(true)) rawRecord.getOptionalNumber("eol_dx_month14") else None,
      eolNewConditionReproductiveYear =
        if (reproductive.contains(true)) rawRecord.getOptionalNumber("eol_dx_year14") else None,
      eolNewConditionReproductiveSpecify =
        if (reproductive.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify14")
        else None,
      eolNewConditionOrthopedicMonth =
        if (orthopedic.contains(true)) rawRecord.getOptionalNumber("eol_dx_month15") else None,
      eolNewConditionOrthopedicYear =
        if (orthopedic.contains(true)) rawRecord.getOptionalNumber("eol_dx_year15") else None,
      eolNewConditionOrthopedicSpecify =
        if (orthopedic.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify15") else None,
      eolNewConditionNeurologicalMonth =
        if (neurological.contains(true)) rawRecord.getOptionalNumber("eol_dx_month16") else None,
      eolNewConditionNeurologicalYear =
        if (neurological.contains(true)) rawRecord.getOptionalNumber("eol_dx_year16") else None,
      eolNewConditionNeurologicalSpecify =
        if (neurological.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify16")
        else None,
      eolNewConditionEndocrineMonth =
        if (endocrine.contains(true)) rawRecord.getOptionalNumber("eol_dx_month17") else None,
      eolNewConditionEndocrineYear =
        if (endocrine.contains(true)) rawRecord.getOptionalNumber("eol_dx_year17") else None,
      eolNewConditionEndocrineSpecify =
        if (endocrine.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify17") else None,
      eolNewConditionHematologicMonth =
        if (hematologic.contains(true)) rawRecord.getOptionalNumber("eol_dx_month18") else None,
      eolNewConditionHematologicYear =
        if (hematologic.contains(true)) rawRecord.getOptionalNumber("eol_dx_year18") else None,
      eolNewConditionHematologicSpecify =
        if (hematologic.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify18") else None,
      eolNewConditionImmuneMonth =
        if (immune.contains(true)) rawRecord.getOptionalNumber("eol_dx_month19") else None,
      eolNewConditionImmuneYear =
        if (immune.contains(true)) rawRecord.getOptionalNumber("eol_dx_year19") else None,
      eolNewConditionImmuneSpecify =
        if (immune.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify19") else None,
      eolNewConditionOtherMonth =
        if (other.contains(true)) rawRecord.getOptionalNumber("eol_dx_month20") else None,
      eolNewConditionOtherYear =
        if (other.contains(true)) rawRecord.getOptionalNumber("eol_dx_year20") else None,
      eolNewConditionOtherSpecify =
        if (other.contains(true)) rawRecord.getOptionalStripped("eol_dx_specify20") else None
    )
  }
}
