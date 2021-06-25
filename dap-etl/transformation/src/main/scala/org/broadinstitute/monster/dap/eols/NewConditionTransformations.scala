package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.EolsNewCondition

object NewConditionTransformations {

  def mapNewConditionMetadata(rawRecord: RawRecord): EolsNewCondition = {
    //    val ToxinConsumption = Some(diagnosisType.contains("2"))
    val diagnosisType = rawRecord.fields.get("eol_dx")
    val InfectiousDisease = diagnosisType.map(_.contains("1"))
    val ToxinConsumption = diagnosisType.map(_.contains("2"))
    val Trauma = diagnosisType.map(_.contains("3"))
    val Cancer = diagnosisType.map(_.contains("4"))
    val Eye = diagnosisType.map(_.contains("5"))
    val Ear = diagnosisType.map(_.contains("6"))
    val Oral = diagnosisType.map(_.contains("7"))
    val Skin = diagnosisType.map(_.contains("8"))
    val Cardiac = diagnosisType.map(_.contains("9"))
    val Respiratory = diagnosisType.map(_.contains("10"))
    val Gastrointestinal = diagnosisType.map(_.contains("11"))
    val Liver = diagnosisType.map(_.contains("12"))
    val Kidney = diagnosisType.map(_.contains("13"))
    val Reproductive = diagnosisType.map(_.contains("14"))
    val Orthopedic = diagnosisType.map(_.contains("15"))
    val Neurological = diagnosisType.map(_.contains("16"))
    val Endocrine = diagnosisType.map(_.contains("17"))
    val Homatologic = diagnosisType.map(_.contains("18"))
    val Immune = diagnosisType.map(_.contains("19"))
    val Other = diagnosisType.map(_.contains("20"))
    EolsNewCondition(
      eolNewConditionNone = diagnosisType.map(_.contains("0")),
      eolNewConditionInfectiousDisease = InfectiousDisease,
      eolNewConditionToxinConsumption = ToxinConsumption,
      eolNewConditionTrauma = Trauma,
      eolNewConditionCancer = Cancer,
      eolNewConditionEye = Eye,
      eolNewConditionEar = Ear,
      eolNewConditionOral = Oral,
      eolNewConditionSkin = Skin,
      eolNewConditionCardiac = Cardiac,
      eolNewConditionRespiratory = Respiratory,
      eolNewConditionGastrointestinal = Gastrointestinal,
      eolNewConditionLiver = Liver,
      eolNewConditionKidney = Kidney,
      eolNewConditionReproductive = Reproductive,
      eolNewConditionOrthopedic = Orthopedic,
      eolNewConditionNeurological = Neurological,
      eolNewConditionEndocrine = Endocrine,
      eolNewConditionHomatologic = Homatologic,
      eolNewConditionImmune = Immune,
      eolNewConditionOther = Other,
      eolNewConditionInfectiousDiseaseMonth =
        if (InfectiousDisease.contains(true)) rawRecord.getOptionalNumber("eol_dx_month1")
        else None,
      eolNewConditionInfectiousDiseaseYear =
        if (InfectiousDisease.contains(true)) rawRecord.getOptionalNumber("eol_dx_year1") else None,
      eolNewConditionInfectiousDiseaseSpecify =
        if (InfectiousDisease.contains(true)) rawRecord.getOptional("eol_dx_specify1") else None,
      eolNewConditionToxinConsumptionMonth =
        if (ToxinConsumption.contains(true)) rawRecord.getOptionalNumber("eol_dx_month2") else None,
      eolNewConditionToxinConsumptionYear =
        if (ToxinConsumption.contains(true)) rawRecord.getOptionalNumber("eol_dx_year2") else None,
      eolNewConditionToxinConsumptionSpecify =
        if (ToxinConsumption.contains(true)) rawRecord.getOptional("eol_dx_specify2") else None,
      eolNewConditionTraumaMonth =
        if (Trauma.contains(true)) rawRecord.getOptionalNumber("eol_dx_month3") else None,
      eolNewConditionTraumaYear =
        if (Trauma.contains(true)) rawRecord.getOptionalNumber("eol_dx_year3") else None,
      eolNewConditionTraumaSpecify =
        if (Trauma.contains(true)) rawRecord.getOptional("eol_dx_specify3") else None,
      eolNewConditionCancerMonth =
        if (Cancer.contains(true)) rawRecord.getOptionalNumber("eol_dx_month4") else None,
      eolNewConditionCancerYear =
        if (Cancer.contains(true)) rawRecord.getOptionalNumber("eol_dx_year4") else None,
      eolNewConditionCancerSpecify =
        if (Cancer.contains(true)) rawRecord.getOptional("eol_dx_specify4") else None,
      eolNewConditionEyeMonth =
        if (Eye.contains(true)) rawRecord.getOptionalNumber("eol_dx_month5") else None,
      eolNewConditionEyeYear =
        if (Eye.contains(true)) rawRecord.getOptionalNumber("eol_dx_year5") else None,
      eolNewConditionEyeSpecify =
        if (Eye.contains(true)) rawRecord.getOptional("eol_dx_specify5") else None,
      eolNewConditionEarMonth =
        if (Ear.contains(true)) rawRecord.getOptionalNumber("eol_dx_month6") else None,
      eolNewConditionEarYear =
        if (Ear.contains(true)) rawRecord.getOptionalNumber("eol_dx_year6") else None,
      eolNewConditionEarSpecify =
        if (Ear.contains(true)) rawRecord.getOptional("eol_dx_specify6") else None,
      eolNewConditionOralMonth =
        if (Oral.contains(true)) rawRecord.getOptionalNumber("eol_dx_month7") else None,
      eolNewConditionOralYear =
        if (Oral.contains(true)) rawRecord.getOptionalNumber("eol_dx_year7") else None,
      eolNewConditionOralSpecify =
        if (Oral.contains(true)) rawRecord.getOptional("eol_dx_specify7") else None,
      eolNewConditionSkinMonth =
        if (Skin.contains(true)) rawRecord.getOptionalNumber("eol_dx_month8") else None,
      eolNewConditionSkinYear =
        if (Skin.contains(true)) rawRecord.getOptionalNumber("eol_dx_year8") else None,
      eolNewConditionSkinSpecify =
        if (Skin.contains(true)) rawRecord.getOptional("eol_dx_specify8") else None,
      eolNewConditionCardiacMonth =
        if (Cardiac.contains(true)) rawRecord.getOptionalNumber("eol_dx_month9") else None,
      eolNewConditionCardiacYear =
        if (Cardiac.contains(true)) rawRecord.getOptionalNumber("eol_dx_year9") else None,
      eolNewConditionCardiacSpecify =
        if (Cardiac.contains(true)) rawRecord.getOptional("eol_dx_specify9") else None,
      eolNewConditionRespiratoryMonth =
        if (Respiratory.contains(true)) rawRecord.getOptionalNumber("eol_dx_month10") else None,
      eolNewConditionRespiratoryYear =
        if (Respiratory.contains(true)) rawRecord.getOptionalNumber("eol_dx_year10") else None,
      eolNewConditionRespiratorySpecify =
        if (Respiratory.contains(true)) rawRecord.getOptional("eol_dx_specify10") else None,
      eolNewConditionGastrointestinalMonth =
        if (Gastrointestinal.contains(true)) rawRecord.getOptionalNumber("eol_dx_month11")
        else None,
      eolNewConditionGastrointestinalYear =
        if (Gastrointestinal.contains(true)) rawRecord.getOptionalNumber("eol_dx_year11") else None,
      eolNewConditionGastrointestinalSpecify =
        if (Gastrointestinal.contains(true)) rawRecord.getOptional("eol_dx_specify11") else None,
      eolNewConditionLiverMonth =
        if (Liver.contains(true)) rawRecord.getOptionalNumber("eol_dx_month12") else None,
      eolNewConditionLiverYear =
        if (Liver.contains(true)) rawRecord.getOptionalNumber("eol_dx_year12") else None,
      eolNewConditionLiverSpecify =
        if (Liver.contains(true)) rawRecord.getOptional("eol_dx_specify12") else None,
      eolNewConditionKidneyMonth =
        if (Kidney.contains(true)) rawRecord.getOptionalNumber("eol_dx_month13") else None,
      eolNewConditionKidneyYear =
        if (Kidney.contains(true)) rawRecord.getOptionalNumber("eol_dx_year13") else None,
      eolNewConditionKidneySpecify =
        if (Kidney.contains(true)) rawRecord.getOptional("eol_dx_specify13") else None,
      eolNewConditionReproductiveMonth =
        if (Reproductive.contains(true)) rawRecord.getOptionalNumber("eol_dx_month14") else None,
      eolNewConditionReproductiveYear =
        if (Reproductive.contains(true)) rawRecord.getOptionalNumber("eol_dx_year14") else None,
      eolNewConditionReproductiveSpecify =
        if (Reproductive.contains(true)) rawRecord.getOptional("eol_dx_specify14") else None,
      eolNewConditionOrthopedicMonth =
        if (Orthopedic.contains(true)) rawRecord.getOptionalNumber("eol_dx_month15") else None,
      eolNewConditionOrthopedicYear =
        if (Orthopedic.contains(true)) rawRecord.getOptionalNumber("eol_dx_year15") else None,
      eolNewConditionOrthopedicSpecify =
        if (Orthopedic.contains(true)) rawRecord.getOptional("eol_dx_specify15") else None,
      eolNewConditionNeurologicalMonth =
        if (Neurological.contains(true)) rawRecord.getOptionalNumber("eol_dx_month16") else None,
      eolNewConditionNeurologicalYear =
        if (Neurological.contains(true)) rawRecord.getOptionalNumber("eol_dx_year16") else None,
      eolNewConditionNeurologicalSpecify =
        if (Neurological.contains(true)) rawRecord.getOptional("eol_dx_specify16") else None,
      eolNewConditionEndocrineMonth =
        if (Endocrine.contains(true)) rawRecord.getOptionalNumber("eol_dx_month17") else None,
      eolNewConditionEndocrineYear =
        if (Endocrine.contains(true)) rawRecord.getOptionalNumber("eol_dx_year17") else None,
      eolNewConditionEndocrineSpecify =
        if (Endocrine.contains(true)) rawRecord.getOptional("eol_dx_specify17") else None,
      eolNewConditionHomatologicMonth =
        if (Homatologic.contains(true)) rawRecord.getOptionalNumber("eol_dx_month18") else None,
      eolNewConditionHomatologicYear =
        if (Homatologic.contains(true)) rawRecord.getOptionalNumber("eol_dx_year18") else None,
      eolNewConditionHomatologicSpecify =
        if (Homatologic.contains(true)) rawRecord.getOptional("eol_dx_specify18") else None,
      eolNewConditionImmuneMonth =
        if (Immune.contains(true)) rawRecord.getOptionalNumber("eol_dx_month19") else None,
      eolNewConditionImmuneYear =
        if (Immune.contains(true)) rawRecord.getOptionalNumber("eol_dx_year19") else None,
      eolNewConditionImmuneSpecify =
        if (Immune.contains(true)) rawRecord.getOptional("eol_dx_specify19") else None,
      eolNewConditionOtherMonth =
        if (Other.contains(true)) rawRecord.getOptionalNumber("eol_dx_month20") else None,
      eolNewConditionOtherYear =
        if (Other.contains(true)) rawRecord.getOptionalNumber("eol_dx_year20") else None,
      eolNewConditionOtherSpecify =
        if (Other.contains(true)) rawRecord.getOptional("eol_dx_specify20") else None
    )
  }
}
