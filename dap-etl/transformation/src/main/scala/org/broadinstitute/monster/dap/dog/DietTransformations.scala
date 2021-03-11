package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogDiet

object DietTransformations {

  /** Map all diet-related RedCap fields into our target schema. */
  def mapDiet(rawRecord: RawRecord): HlesDogDiet = {
    val transforms = List(
      mapSummary _,
      mapPrimaryComponent _,
      mapSecondaryComponent _,
      mapTreats _,
      mapDailySupplements _,
      mapInfrequentSupplements _
    )

    transforms.foldLeft(HlesDogDiet.init())((acc, f) => f(rawRecord, acc))
  }

  /** Map high-level information about dog diet. */
  def mapSummary(rawRecord: RawRecord, dog: HlesDogDiet): HlesDogDiet = {
    val consistency = rawRecord.getOptionalNumber("df_consistent")
    val appetiteChange = rawRecord.getOptionalNumber("df_app_change")
    val weightChange = rawRecord.getOptionalNumber("df_weight_change")

    dog.copy(
      dfFeedingsPerDay = rawRecord.getOptionalNumber("df_frequency"),
      dfDietConsistency = consistency,
      dfDietConsistencyOtherDescription = if (consistency.contains(98L)) {
        rawRecord.getOptionalStripped("df_consistent_other")
      } else {
        None
      },
      dfAppetite = rawRecord.getOptionalNumber("df_app"),
      dfAppetiteChangeLastYear = appetiteChange.flatMap {
        case 1L    => Some(0L)
        case 0L    => rawRecord.getOptionalNumber("df_app_change_how")
        case other => Some(other)
      },
      dfEverMalnourished = rawRecord.getOptionalNumber("df_malnourished"),
      dfEverUnderweight = rawRecord.getOptionalNumber("df_underweight"),
      // Note: "df_overrweight" raw field name is misspelled in RedCap
      dfEverOverweight = rawRecord.getOptionalNumber("df_overrweight"),
      // "1" from gated question maps to "yes, weight stayed the same" so we inject 0
      // "2" from gated question maps to "puppy weight gain" so we inject a 3 to keep the distinction
      dfWeightChangeLastYear = weightChange.flatMap {
        case 1L    => Some(0L)
        case 0L    => Some(rawRecord.getRequired("df_weight_change_how").toLong)
        case 2L    => Some(3L)
        case other => Some(other)
      }
    )
  }

  /** Map fields about the primary component of a dog's diet. */
  def mapPrimaryComponent(rawRecord: RawRecord, dog: HlesDogDiet): HlesDogDiet = {
    val component = rawRecord.getOptionalNumber("df_prim")
    val recentlyChanged = rawRecord.getOptionalBoolean("df_prim_change_12m")
    val changeReasons = recentlyChanged.flatMap {
      if (_) rawRecord.get("df_prim_change_why") else None
    }
    val changeOther = changeReasons.map(_.contains("98"))

    dog.copy(
      dfPrimaryDietComponent = component,
      dfPrimaryDietComponentOtherDescription = if (component.contains(98L)) {
        rawRecord.getOptionalStripped("df_prim_other")
      } else {
        None
      },
      dfPrimaryDietComponentOrganic = rawRecord.getOptionalBoolean("df_prim_org"),
      dfPrimaryDietComponentGrainFree = rawRecord.getOptionalBoolean("df_prim_gf"),
      dfPrimaryDietComponentGrainFreePast = rawRecord.getOptionalBoolean("df_prim_gf_past"),
      dfPrimaryDietComponentChangeRecent = recentlyChanged,
      dfPrimaryDietComponentChangeMonthsAgo = recentlyChanged.flatMap {
        if (_) rawRecord.getOptionalNumber("df_prim_change_12m_when") else None
      },
      dfPrimaryDietComponentChangeAllergyRelated = changeReasons.map(_.contains("1")),
      dfPrimaryDietComponentChangeDifferentLifeStage = changeReasons.map(_.contains("2")),
      dfPrimaryDietComponentChangeStopGrainFree = changeReasons.map(_.contains("3")),
      dfPrimaryDietComponentChangeHealthConditionSpecific = changeReasons.map(_.contains("4")),
      dfPrimaryDietComponentChangeBrandChange = changeReasons.map(_.contains("5")),
      dfPrimaryDietComponentChangeNewFoodSameBrand = changeReasons.map(_.contains("6")),
      dfPrimaryDietComponentChangeOther = changeOther,
      dfPrimaryDietComponentChangeOtherDescription = changeOther.flatMap {
        if (_) rawRecord.getOptionalStripped("df_prim_change_why_other") else None
      }
    )
  }

  /** Map fields about the secondary component of a dog's diet. */
  def mapSecondaryComponent(rawRecord: RawRecord, dog: HlesDogDiet): HlesDogDiet = {
    val secondaryUsed = rawRecord.getOptionalBoolean("df_sec_yn")
    val component = secondaryUsed.flatMap {
      if (_) rawRecord.getOptionalNumber("df_sec") else None
    }
    val recentlyChanged = secondaryUsed.flatMap {
      if (_) rawRecord.getOptionalBoolean("df_sec_change_12m") else None
    }
    val changeReasons = recentlyChanged.flatMap {
      if (_) rawRecord.get("df_sec_change_why") else None
    }
    val changeOther = changeReasons.map(_.contains("98"))

    dog.copy(
      dfSecondaryDietComponentUsed = secondaryUsed,
      dfSecondaryDietComponent = component,
      dfSecondaryDietComponentOtherDescription = if (component.contains(98L)) {
        rawRecord.getOptionalStripped("df_sec_other")
      } else {
        None
      },
      dfSecondaryDietComponentOrganic = rawRecord.getOptionalBoolean("df_sec_org"),
      dfSecondaryDietComponentGrainFree = rawRecord.getOptionalBoolean("df_sec_gf"),
      dfSecondaryDietComponentGrainFreePast = rawRecord.getOptionalBoolean("df_sec_gf_past"),
      dfSecondaryDietComponentChangeRecent = recentlyChanged,
      dfSecondaryDietComponentChangeMonthsAgo = recentlyChanged.flatMap {
        if (_) rawRecord.getOptionalNumber("df_sec_change_12m_when") else None
      },
      dfSecondaryDietComponentChangeAllergyRelated = changeReasons.map(_.contains("1")),
      dfSecondaryDietComponentChangeDifferentLifeStage = changeReasons.map(_.contains("2")),
      dfSecondaryDietComponentChangeStopGrainFree = changeReasons.map(_.contains("3")),
      dfSecondaryDietComponentChangeHealthConditionSpecific = changeReasons.map(_.contains("4")),
      dfSecondaryDietComponentChangeBrandChange = changeReasons.map(_.contains("5")),
      dfSecondaryDietComponentChangeNewFoodSameBrand = changeReasons.map(_.contains("6")),
      dfSecondaryDietComponentChangeOther = changeOther,
      dfSecondaryDietComponentChangeOtherDescription = changeOther.flatMap {
        if (_) rawRecord.getOptionalStripped("df_sec_change_why_other") else None
      }
    )
  }

  /** Map fields about the treats a dog regularly eats. */
  def mapTreats(rawRecord: RawRecord, dog: HlesDogDiet): HlesDogDiet = {
    val otherTreats = rawRecord.getOptionalBoolean("df_t_other")

    dog.copy(
      dfTreatsFrequency = rawRecord.getOptionalNumber("df_treat_freq"),
      dfTreatsCommercialBiscuits = rawRecord.getOptionalBoolean("df_t_biscuit"),
      dfTreatsRawhide = rawRecord.getOptionalBoolean("df_t_rawhide"),
      dfTreatsBones = rawRecord.getOptionalBoolean("df_t_bones"),
      dfTreatsTableMeat = rawRecord.getOptionalBoolean("df_t_table_meat"),
      dfTreatsTableCarbs = rawRecord.getOptionalBoolean("df_t_table_carbs"),
      dfTreatsVegetables = rawRecord.getOptionalBoolean("df_t_veg"),
      dfTreatsHomemadeProtein = rawRecord.getOptionalBoolean("df_t_hm_prot"),
      dfTreatsHomemadeBiscuits = rawRecord.getOptionalBoolean("df_t_hm_biscuit"),
      dfTreatsPumpkin = rawRecord.getOptionalBoolean("df_t_pumpkin"),
      dfTreatsPeanutButter = rawRecord.getOptionalBoolean("df_t_pb"),
      dfTreatsOther = otherTreats,
      dfTreatsOtherDescription = otherTreats.flatMap {
        // this variable was given the wrong instrument prefix in REDCap early in study
        if (_) rawRecord.getOptional("db_t_other_text") else None
      }
    )
  }

  /** Map fields about supplements taken daily by a dog when available. */
  def mapDailySupplements(rawRecord: RawRecord, dog: HlesDogDiet): HlesDogDiet = {
    val dailySupps = rawRecord.getRequiredBoolean("df_supplement_daily")
    if (dailySupps) {
      val otherSupps = rawRecord.getOptionalNumber("df_s_other")

      dog.copy(
        dfDailySupplements = Some(dailySupps),
        dfDailySupplementsBoneMeal = rawRecord.getOptionalNumber("df_s_bone_meal"),
        dfDailySupplementsGlucosamine = rawRecord.getOptionalNumber("df_s_glucosamine"),
        dfDailySupplementsChondroitin = rawRecord.getOptionalNumber("df_s_chondroitin"),
        dfDailySupplementsOtherJoint = rawRecord.getOptionalNumber("df_s_joint_other"),
        dfDailySupplementsOmega3 = rawRecord.getOptionalNumber("df_s_omega3"),
        dfDailySupplementsNonOilSkin = rawRecord.getOptionalNumber("df_s_skin"),
        dfDailySupplementsVitamins = rawRecord.getOptionalNumber("df_s_vitamin"),
        dfDailySupplementsEnzyme = rawRecord.getOptionalNumber("df_s_enzyme"),
        dfDailySupplementsProbiotics = rawRecord.getOptionalNumber("df_s_probiotics"),
        dfDailySupplementsFiber = rawRecord.getOptionalNumber("df_s_fiber"),
        dfDailySupplementsAlkalinize = rawRecord.getOptionalNumber("df_s_alkalinize"),
        dfDailySupplementsAcidify = rawRecord.getOptionalNumber("df_s_acidify"),
        dfDailySupplementsTaurine = rawRecord.getOptionalNumber("df_s_taurine"),
        dfDailySupplementsAntiox = rawRecord.getOptionalNumber("df_s_antiox"),
        dfDailySupplementsCoenzymeQ10 = rawRecord.getOptionalNumber("df_s_q10"),
        dfDailySupplementsOther = otherSupps,
        dfDailySupplementsOtherDescription = if (otherSupps.getOrElse(0L) != 0L) {
          rawRecord.getOptional("df_s_other_text")
        } else {
          None
        }
      )
    } else {
      dog.copy(
        dfDailySupplements = Some(dailySupps)
      )
    }
  }

  /** Map fields about supplements taken less-than-daily by a dog. */
  def mapInfrequentSupplements(rawRecord: RawRecord, dog: HlesDogDiet): HlesDogDiet = {
    val infreqSupps = rawRecord.getRequiredBoolean("df_supplement_ltd")
    if (infreqSupps) {
      val otherSupps = rawRecord.getOptionalNumber("df_s_other_ltd")

      dog.copy(
        dfInfrequentSupplements = Some(infreqSupps),
        dfInfrequentSupplementsBoneMeal = rawRecord.getOptionalNumber("df_s_bone_meal_ltd"),
        dfInfrequentSupplementsGlucosamine = rawRecord.getOptionalNumber("df_s_glucosamine_ltd"),
        dfInfrequentSupplementsChondroitin = rawRecord.getOptionalNumber("df_s_chondroitin_ltd"),
        dfInfrequentSupplementsOtherJoint = rawRecord.getOptionalNumber("df_s_joint_other_ltd"),
        dfInfrequentSupplementsOmega3 = rawRecord.getOptionalNumber("df_s_omega3_ltd"),
        dfInfrequentSupplementsNonOilSkin = rawRecord.getOptionalNumber("df_s_skin_ltd"),
        dfInfrequentSupplementsVitamins = rawRecord.getOptionalNumber("df_s_vitamin_ltd"),
        dfInfrequentSupplementsEnzyme = rawRecord.getOptionalNumber("df_s_enzyme_ltd"),
        dfInfrequentSupplementsProbiotics = rawRecord.getOptionalNumber("df_s_probiotics_ltd"),
        dfInfrequentSupplementsFiber = rawRecord.getOptionalNumber("df_s_fiber_ltd"),
        dfInfrequentSupplementsAlkalinize = rawRecord.getOptionalNumber("df_s_alkalinize_ltd"),
        dfInfrequentSupplementsAcidify = rawRecord.getOptionalNumber("df_s_acidify_ltd"),
        dfInfrequentSupplementsTaurine = rawRecord.getOptionalNumber("df_s_taurine_ltd"),
        dfInfrequentSupplementsAntiox = rawRecord.getOptionalNumber("df_s_antiox_ltd"),
        dfInfrequentSupplementsCoenzymeQ10 = rawRecord.getOptionalNumber("df_s_q10_ltd"),
        dfInfrequentSupplementsOther = otherSupps,
        dfInfrequentSupplementsOtherDescription = if (otherSupps.getOrElse(0L) != 0L) {
          rawRecord.getOptional("df_s_other_ltd_text")
        } else {
          None
        }
      )
    } else {
      dog.copy(
        dfInfrequentSupplements = Some(infreqSupps)
      )
    }
  }
}
