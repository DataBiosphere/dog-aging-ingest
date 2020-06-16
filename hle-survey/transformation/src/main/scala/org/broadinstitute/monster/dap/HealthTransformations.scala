package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesHealthCondition

object HealthTransformations {

  /** Parse all health-condition-related fields out of a raw RedCap record. */
  def mapHealthConditions(rawRecord: RawRecord): Iterable[HlesHealthCondition] =
    // condition type is congenital vs infectious vs tox vs trauma vs etc
    health_conditions.filter { case (condition, _) => rawRecord.getBoolean(s"hs_${condition}_yn") }.flatMap {
      // infectious disease
      case ("dx_infectious", conditionCategorical) =>
        // process infectious disease data
        infectious_diseases.filter {
          case (infection, _) => rawRecord.getBoolean(s"hs_dx_${infection}")
        }.map {
          case (infection, infectionCategorical) =>
            HlesHealthCondition(
              dogId = rawRecord.getRequired("study_id").toLong,
              hsConditionType = conditionCategorical.toLong,
              hsCondition = infectionCategorical.toLong,
              // should only be populated if hs_dx_infect_other is the current infectionCategorical (value is 40)
              hsConditionOtherDescription = if (infectionCategorical == 40) {
                rawRecord.getOptional("hs_dx_infect_other_spec")
              } else { None },
              // infectious diseases are not categorized as congenital
              hsConditionIsCongenital = false,
              // no relevant field for infectious diseases
              hsConditionCause = None,
              // no relevant field for infectious diseases
              hsConditionCauseOtherDescription = None,
              hsDiagnosisYear = rawRecord.getOptionalNumber(s"hs_dx_${infection}_year"),
              hsDiagnosisMonth = rawRecord.getOptionalNumber(s"hs_dx_${infection}_month"),
              hsRequiredSurgeryOrHospitalization =
                rawRecord.getOptionalNumber(s"hs_dx_${infection}_surg"),
              hsFollowUpOngoing = rawRecord.getOptionalBoolean(s"hs_dx_${infection}_fu")
            )
        }
      // an "else" case to please the compiler gods for now, will eventually be replaced by all the other health
      // condition cases
      case (_, conditionCategorical) =>
        List(
          HlesHealthCondition(
            dogId = rawRecord.getRequired("study_id").toLong,
            hsConditionType = conditionCategorical.toLong,
            hsCondition = 100L,
            hsConditionOtherDescription = Some("falafel"),
            hsConditionIsCongenital = false,
            hsConditionCause = None,
            hsConditionCauseOtherDescription = None,
            hsDiagnosisYear = Some(2020),
            hsDiagnosisMonth = Some(3),
            hsRequiredSurgeryOrHospitalization = Some(1),
            hsFollowUpOngoing = Some(true)
          )
        )
    }

  val health_conditions: List[(String, Int)] = List(
    "congenital",
    "dx_infectious",
    "dx_tox",
    "dx_trauma",
    "dx_eye",
    "dx_ear",
    "dx_oral",
    "dx_skin",
    "dx_cardiac",
    "dx_respire",
    "dx_gi",
    "dx_liver",
    "dx_kidney",
    "dx_repro",
    "dx_ortho",
    "dx_neuro",
    "dx_endo",
    "dx_hema",
    "dx_immune"
  ).zipWithIndex

  val infectious_diseases: List[(String, Int)] = List(
    "anaplasmosis",
    "asperg",
    "babesio",
    "blastomy",
    "bordetella",
    "brucellosis",
    "campylo",
    "chagas",
    "ccdia",
    "ccdio",
    "crypto",
    "dermato",
    "dstmp",
    "ehrlich",
    "fever",
    "gp",
    "giar",
    "granu",
    "hrtworm",
    "histo",
    "hepato",
    "hkworm",
    "influ",
    "isosp",
    "leish",
    "lepto",
    "lyme",
    "mrsa",
    "mycob",
    "parvo",
    "plague",
    "pythium",
    "rmsf",
    "rndworm",
    "slmosis",
    "slmpois",
    "tpworm",
    "toxop",
    "tular",
    "whpworm",
    "infect_other"
  ).zipWithIndex
}
