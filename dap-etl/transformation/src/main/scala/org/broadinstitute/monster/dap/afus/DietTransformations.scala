package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.AfusDogDiet

object DietTransformations {

  def mapDiet(rawRecord: RawRecord): AfusDogDiet = {
    val init = AfusDogDiet.init()

    val transformations = List(
      mapPrimaryComponent _,
      mapSecondaryComponent _,
      mapTreats _,
      mapDailySupplements _,
      mapInfrequentSupplements _,
      mapDietChanges _
    )

    transformations.foldLeft(init)((acc, f) => f(rawRecord, acc))
  }

  def mapPrimaryComponent(rawRecord: RawRecord, dog: AfusDogDiet): AfusDogDiet = {
    val component = rawRecord.getOptionalNumber("fu_df_prim")
    val recentlyChanged = rawRecord.getOptionalBoolean("fu_df_prim_change_12m")
    val changeReasons = recentlyChanged.flatMap {
      if (_) rawRecord.get("fu_df_prim_change_why") else None
    }
    val changeOther = changeReasons.map(_.contains("98"))

    dog.copy(
      afusDfFeedingsPerDay = rawRecord.getOptionalNumber("fu_df_frequency"),
      afusDfPrimaryDietComponent = component,
      afusDfPrimaryDietComponentOtherDescription =
        rawRecord.getOptionalStripped("fu_df_prim_other"),
      afusDfPrimaryDietComponentOrganic = rawRecord.getOptionalBoolean("fu_df_prim_org"),
      afusDfPrimaryDietComponentGrainFree = rawRecord.getOptionalBoolean("fu_df_prim_gf"),
      afusDfPrimaryDietBrand = rawRecord.getOptionalNumber("fu_df_prim_brand"),
      afusDfPrimaryDietBrandOtherDescription =
        rawRecord.getOptionalStripped("fu_df_prim_brand_other"),
      afusDfPrimaryDietComponentChangeRecent = recentlyChanged,
      afusDfPrimaryDietComponentChangeMonthsAgo =
        rawRecord.getOptionalNumber("fu_df_prim_change_12m_when"),
      afusDfPrimaryDietComponentChangeAllergyRelated = changeReasons.map(_.contains("1")),
      afusDfPrimaryDietComponentChangeDifferentLifeStage = changeReasons.map(_.contains("2")),
      afusDfPrimaryDietComponentChangeStopGrainFree = changeReasons.map(_.contains("3")),
      afusDfPrimaryDietComponentChangeConditionSpecific = changeReasons.map(_.contains("4")),
      afusDfPrimaryDietComponentChangeBrandChange = changeReasons.map(_.contains("5")),
      afusDfPrimaryDietComponentChangeNewFoodSameBrand = changeReasons.map(_.contains("6")),
      afusDfPrimaryDietComponentChangeOther = changeOther,
      afusDfPrimaryDietComponentChangeOtherDescription =
        rawRecord.getOptionalStripped("fu_df_prim_change_why_other")
    )
  }

  def mapSecondaryComponent(rawRecord: RawRecord, dog: AfusDogDiet): AfusDogDiet = {
    val component = rawRecord.getOptionalNumber("fu_df_sec")
    val recentlyChanged = rawRecord.getOptionalBoolean("fu_df_sec_change_12m")
    val changeReasons = recentlyChanged.flatMap {
      if (_) rawRecord.get("fu_df_sec_change_why") else None
    }
    val changeOther = changeReasons.map(_.contains("98"))

    dog.copy(
      afusDfSecondaryDietComponentUsed = rawRecord.getOptionalBoolean("fu_df_sec_yn"),
      afusDfSecondaryDietComponent = component,
      afusDfSecondaryDietComponentOtherDescription =
        rawRecord.getOptionalStripped("fu_df_sec_other"),
      afusDfSecondaryDietComponentOrganic = rawRecord.getOptionalBoolean("fu_df_sec_org"),
      afusDfSecondaryDietComponentGrainFree = rawRecord.getOptionalBoolean("fu_df_sec_gf"),
      afusDfSecondaryDietBrand = rawRecord.getOptionalNumber("fu_df_sec_brand"),
      afusDfSecondaryDietBrandOtherDescription =
        rawRecord.getOptionalStripped("fu_df_sec_brand_other"),
      afusDfSecondaryDietComponentChangeRecent = recentlyChanged,
      afusDfSecondaryDietComponentChangeMonthsAgo =
        rawRecord.getOptionalNumber("fu_df_sec_change_12m_when"),
      afusDfSecondaryDietComponentChangeAllergyRelated = changeReasons.map(_.contains("1")),
      afusDfSecondaryDietComponentChangeDifferentLifeStage = changeReasons.map(_.contains("2")),
      afusDfSecondaryDietComponentChangeStopGrainFree = changeReasons.map(_.contains("3")),
      afusDfSecondaryDietComponentChangeConditionSpecific = changeReasons.map(_.contains("4")),
      afusDfSecondaryDietComponentChangeBrandChange = changeReasons.map(_.contains("5")),
      afusDfSecondaryDietComponentChangeNewFoodSameBrand = changeReasons.map(_.contains("6")),
      afusDfSecondaryDietComponentChangeOther = changeOther,
      afusDfSecondaryDietComponentChangeOtherDescription =
        rawRecord.getOptionalStripped("fu_df_sec_change_why_other")
    )
  }

  def mapTreats(rawRecord: RawRecord, dog: AfusDogDiet): AfusDogDiet = {
    dog.copy(
      afusDfTreatsFrequency = rawRecord.getOptionalNumber("fu_df_treat_freq"),
      afusDfTreatsCommercialBiscuits = rawRecord.getOptionalBoolean("fu_df_t_biscuit"),
      afusDfTreatsRawhide = rawRecord.getOptionalBoolean("fu_df_t_rawhide"),
      afusDfTreatsBones = rawRecord.getOptionalBoolean("fu_df_t_bones"),
      afusDfTreatsTableMeat = rawRecord.getOptionalBoolean("fu_df_t_table_meat"),
      afusDfTreatsTableCarbs = rawRecord.getOptionalBoolean("fu_df_t_table_carbs"),
      afusDfTreatsVegetables = rawRecord.getOptionalBoolean("fu_df_t_veg"),
      afusDfTreatsHomemadeProtein = rawRecord.getOptionalBoolean("fu_df_t_hm_prot"),
      afusDfTreatsHomemadeBiscuits = rawRecord.getOptionalBoolean("fu_df_t_hm_biscuit"),
      afusDfTreatsPumpkin = rawRecord.getOptionalBoolean("fu_df_t_pumpkin"),
      afusDfTreatsPeanutButter = rawRecord.getOptionalBoolean("fu_df_t_pb"),
      afusDfTreatsDental = rawRecord.getOptionalBoolean("fu_df_t_dental"),
      afusDfTreatsFruit = rawRecord.getOptionalBoolean("fu_df_t_fruit"),
      afusDfTreatsDairy = rawRecord.getOptionalBoolean("fu_df_t_dairy"),
      afusDfTreatsMeatCooked = rawRecord.getOptionalBoolean("fu_df_t_meat_cooked"),
      afusDfTreatsMeatRaw = rawRecord.getOptionalBoolean("fu_df_t_meat_raw"),
      afusDfTreatsOil = rawRecord.getOptionalBoolean("fu_df_t_oil"),
      afusDfTreatsJerky = rawRecord.getOptionalBoolean("fu_df_t_jerky"),
      afusDfTreatsEggs = rawRecord.getOptionalBoolean("fu_df_t_eggs"),
      afusDfTreatsOther = rawRecord.getOptionalBoolean("fu_df_t_other"),
      afusDfTreatsOtherDescription = rawRecord.getOptionalStripped("fu_df_t_other_text")
    )
  }

  def mapDailySupplements(rawRecord: RawRecord, dog: AfusDogDiet): AfusDogDiet = {
    dog.copy(
      afusDfDailySupplements = rawRecord.getOptionalBoolean("fu_df_supplement_daily"),
      afusDfDailySupplementsBoneMeal = rawRecord.getOptionalNumber("fu_df_s_bone_meal"),
      afusDfDailySupplementsGlucosamine = rawRecord.getOptionalNumber("fu_df_s_glucosamine"),
      afusDfDailySupplementsChondroitin = rawRecord.getOptionalNumber("fu_df_s_chondroitin"),
      afusDfDailySupplementsOtherJoint = rawRecord.getOptionalNumber("fu_df_s_joint_other"),
      afusDfDailySupplementsOmega3 = rawRecord.getOptionalNumber("fu_df_s_omega3"),
      afusDfDailySupplementsNonOilSkin = rawRecord.getOptionalNumber("fu_df_s_skin"),
      afusDfDailySupplementsVitamins = rawRecord.getOptionalNumber("fu_df_s_vitamin"),
      afusDfDailySupplementsEnzyme = rawRecord.getOptionalNumber("fu_df_s_enzyme"),
      afusDfDailySupplementsProbiotics = rawRecord.getOptionalNumber("fu_df_s_probiotics"),
      afusDfDailySupplementsFiber = rawRecord.getOptionalNumber("fu_df_s_fiber"),
      afusDfDailySupplementsAlkalinize = rawRecord.getOptionalNumber("fu_df_s_alkalinize"),
      afusDfDailySupplementsAcidify = rawRecord.getOptionalNumber("fu_df_s_acidify"),
      afusDfDailySupplementsTaurine = rawRecord.getOptionalNumber("fu_df_s_taurine"),
      afusDfDailySupplementsAntiox = rawRecord.getOptionalNumber("fu_df_s_antiox"),
      afusDfDailySupplementsCoenzymeQ10 = rawRecord.getOptionalNumber("fu_df_s_q10"),
      afusDfDailySupplementsCbd = rawRecord.getOptionalNumber("fu_df_s_cbd"),
      afusDfDailySupplementsTurmeric = rawRecord.getOptionalNumber("fu_df_s_tumeric"),
      afusDfDailySupplementsAnxiety = rawRecord.getOptionalNumber("fu_df_s_anxiety"),
      afusDfDailySupplementsOtherOils = rawRecord.getOptionalNumber("fu_df_s_oils_other"),
      afusDfDailySupplementsCranberry = rawRecord.getOptionalNumber("fu_df_s_cranberry"),
      afusDfDailySupplementsImmune = rawRecord.getOptionalNumber("fu_df_s_immune"),
      afusDfDailySupplementsLiver = rawRecord.getOptionalNumber("fu_df_s_liver"),
      afusDfDailySupplementsAnal = rawRecord.getOptionalNumber("fu_df_s_anal"),
      afusDfDailySupplementsAllergy = rawRecord.getOptionalNumber("fu_df_s_allergy"),
      afusDfDailySupplementsOther = rawRecord.getOptionalNumber("fu_df_s_other"),
      afusDfDailySupplementsOtherDescription = rawRecord.getOptionalStripped("fu_df_s_other_text")
    )
  }

  def mapInfrequentSupplements(rawRecord: RawRecord, dog: AfusDogDiet): AfusDogDiet = {
    dog.copy(
      afusDfInfrequentSupplements = rawRecord.getOptionalNumber("fu_df_supplement_ltd"),
      afusDfInfrequentSupplementsBoneMeal = rawRecord.getOptionalNumber("fu_df_s_bone_meal_ltd"),
      afusDfInfrequentSupplementsGlucosamine =
        rawRecord.getOptionalNumber("fu_df_s_glucosamine_ltd"),
      afusDfInfrequentSupplementsChondroitin =
        rawRecord.getOptionalNumber("fu_df_s_chondroitin_ltd"),
      afusDfInfrequentSupplementsOtherJoint =
        rawRecord.getOptionalNumber("fu_df_s_joint_other_ltd"),
      afusDfInfrequentSupplementsOmega3 = rawRecord.getOptionalNumber("fu_df_s_omega3_ltd"),
      afusDfInfrequentSupplementsNonOilSkin = rawRecord.getOptionalNumber("fu_df_s_skin_ltd"),
      afusDfInfrequentSupplementsVitamins = rawRecord.getOptionalNumber("fu_df_s_vitamin_ltd"),
      afusDfInfrequentSupplementsEnzyme = rawRecord.getOptionalNumber("fu_df_s_enzyme_ltd"),
      afusDfInfrequentSupplementsProbiotics = rawRecord.getOptionalNumber("fu_df_s_probiotics_ltd"),
      afusDfInfrequentSupplementsFiber = rawRecord.getOptionalNumber("fu_df_s_fiber_ltd"),
      afusDfInfrequentSupplementsAlkalinize = rawRecord.getOptionalNumber("fu_df_s_alkalinize_ltd"),
      afusDfInfrequentSupplementsAcidify = rawRecord.getOptionalNumber("fu_df_s_acidify_ltd"),
      afusDfInfrequentSupplementsTaurine = rawRecord.getOptionalNumber("fu_df_s_taurine_ltd"),
      afusDfInfrequentSupplementsAntiox = rawRecord.getOptionalNumber("fu_df_s_antiox_ltd"),
      afusDfInfrequentSupplementsCoenzymeQ10 = rawRecord.getOptionalNumber("fu_df_s_q10_ltd"),
      afusDfInfrequentSupplementsCbd = rawRecord.getOptionalNumber("fu_df_s_cbd_ltd"),
      afusDfInfrequentSupplementsTurmeric = rawRecord.getOptionalNumber("fu_df_s_tumeric_ltd"),
      afusDfInfrequentSupplementsAnxiety = rawRecord.getOptionalNumber("fu_df_s_anxiety_ltd"),
      afusDfInfrequentSupplementsOtherOils = rawRecord.getOptionalNumber("fu_df_s_oils_other_ltd"),
      afusDfInfrequentSupplementsCranberry = rawRecord.getOptionalNumber("fu_df_s_cranberry_ltd"),
      afusDfInfrequentSupplementsImmune = rawRecord.getOptionalNumber("fu_df_s_immune_ltd"),
      afusDfInfrequentSupplementsLiver = rawRecord.getOptionalNumber("fu_df_s_liver_ltd"),
      afusDfInfrequentSupplementsAnal = rawRecord.getOptionalNumber("fu_df_s_anal_ltd"),
      afusDfInfrequentSupplementsAllergy = rawRecord.getOptionalNumber("fu_df_s_allergy_ltd"),
      afusDfInfrequentSupplementsOther = rawRecord.getOptionalNumber("fu_df_s_other_ltd"),
      afusDfInfrequentSupplementsOtherDescription =
        rawRecord.getOptionalStripped("fu_df_s_other_ltd_text")
    )
  }

  //If fu_df_app_change = Unknown, pass it through. If fu_df_app_change = Yes, inject 0 for "No change". Otherwise use fu_df_app_change_how.
  def mapDietChanges(rawRecord: RawRecord, dog: AfusDogDiet): AfusDogDiet = {
    val consistency = rawRecord.getOptionalNumber("fu_df_consistent")
    val appetiteChange = rawRecord.getOptionalNumber("fu_df_app_change")
    val appetiteChangeHow = rawRecord.getOptionalNumber("fu_df_app_change_how")
    val weightChange = rawRecord.getOptionalNumber("fu_df_weight_change")
    val weightChangeHow = rawRecord.getOptionalNumber("fu_df_weight_change_how")

    dog.copy(
      afusDfDietConsistency = consistency,
      afusDfDietConsistencyOtherDescription =
        rawRecord.getOptionalStripped("fu_df_consistent_other"),
      afusDfAppetite = rawRecord.getOptionalNumber("fu_df_app"),
      afusDfAppetiteChangeLastYear = appetiteChange.flatMap {
        case 99L => Some(99L)
        case 1L  => Some(0L)
        case _   => appetiteChangeHow
      },
      afusDfWeightChangeLastYear = weightChange.flatMap {
        case 99L => Some(99L)
        case 1L  => Some(0L)
        case 2L  => Some(3L)
        case _   => weightChangeHow
      },
      afusDfMalnourishedLastYear = rawRecord.getOptionalNumber("fu_df_malnourished"),
      afusDfUnderweightLastYear = rawRecord.getOptionalNumber("fu_df_underweight"),
      afusDfOverweightLastYear = rawRecord.getOptionalNumber("fu_df_overrweight")
    )
  }
}
