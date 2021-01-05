package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogDiet
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DietTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "DietTransformations"

  it should "map summary-level diet information" in {
    val noAppChangeUnknownWeightChange = Map(
      "df_frequency" -> Array("1"),
      "df_consistent" -> Array("98"),
      "df_consistent_other" -> Array("dunno"),
      "df_app" -> Array("2"),
      "df_app_change" -> Array("1"),
      "df_malnourished" -> Array("0"),
      "df_underweight" -> Array("1"),
      "df_overrweight" -> Array("99"),
      "df_weight_change" -> Array("99"),
      "df_weight_change_how" -> Array("3")
    )
    val appChangeNoWeightChange = Map(
      "df_frequency" -> Array("2"),
      "df_consistent" -> Array("2"),
      "df_consistent_other" -> Array("dunno"),
      "df_app" -> Array("2"),
      "df_app_change" -> Array("0"),
      "df_app_change_how" -> Array("4"),
      "df_malnourished" -> Array("0"),
      "df_underweight" -> Array("99"),
      "df_overrweight" -> Array("1"),
      "df_weight_change" -> Array("1"),
      "df_weight_change_how" -> Array("10")
    )
    val unknownAppChangeWeightChange = Map(
      "df_frequency" -> Array("3"),
      "df_consistent" -> Array("1"),
      "df_app" -> Array("2"),
      "df_app_change" -> Array("99"),
      "df_app_change_how" -> Array("1"),
      "df_malnourished" -> Array("99"),
      "df_underweight" -> Array("1"),
      "df_overrweight" -> Array("0"),
      "df_weight_change" -> Array("0"),
      "df_weight_change_how" -> Array("2")
    )

    val noAppChangeUnknownWeightChangeOut = DietTransformations.mapSummary(
      RawRecord(1, noAppChangeUnknownWeightChange),
      HlesDogDiet.init()
    )
    val appChangeNoWeightChangeOut = DietTransformations.mapSummary(
      RawRecord(1, appChangeNoWeightChange),
      HlesDogDiet.init()
    )
    val unknownAppChangeWeightChangeOut = DietTransformations.mapSummary(
      RawRecord(1, unknownAppChangeWeightChange),
      HlesDogDiet.init()
    )

    noAppChangeUnknownWeightChangeOut.dfFeedingsPerDay.value shouldBe 1L
    noAppChangeUnknownWeightChangeOut.dfDietConsistency.value shouldBe 98L
    noAppChangeUnknownWeightChangeOut.dfDietConsistencyOtherDescription.value shouldBe "dunno"
    noAppChangeUnknownWeightChangeOut.dfAppetite.value shouldBe 2L
    noAppChangeUnknownWeightChangeOut.dfAppetiteChangeLastYear.value shouldBe 0L
    noAppChangeUnknownWeightChangeOut.dfEverMalnourished.value shouldBe 0L
    noAppChangeUnknownWeightChangeOut.dfEverUnderweight.value shouldBe 1L
    noAppChangeUnknownWeightChangeOut.dfEverOverweight.value shouldBe 99L
    noAppChangeUnknownWeightChangeOut.dfWeightChangeLastYear.value shouldBe 99L

    appChangeNoWeightChangeOut.dfFeedingsPerDay.value shouldBe 2L
    appChangeNoWeightChangeOut.dfDietConsistency.value shouldBe 2L
    appChangeNoWeightChangeOut.dfDietConsistencyOtherDescription shouldBe None
    appChangeNoWeightChangeOut.dfAppetite.value shouldBe 2L
    appChangeNoWeightChangeOut.dfAppetiteChangeLastYear.value shouldBe 4L
    appChangeNoWeightChangeOut.dfEverMalnourished.value shouldBe 0L
    appChangeNoWeightChangeOut.dfEverUnderweight.value shouldBe 99L
    appChangeNoWeightChangeOut.dfEverOverweight.value shouldBe 1L
    appChangeNoWeightChangeOut.dfWeightChangeLastYear.value shouldBe 0L

    unknownAppChangeWeightChangeOut.dfFeedingsPerDay.value shouldBe 3L
    unknownAppChangeWeightChangeOut.dfDietConsistency.value shouldBe 1L
    unknownAppChangeWeightChangeOut.dfDietConsistencyOtherDescription shouldBe None
    unknownAppChangeWeightChangeOut.dfAppetite.value shouldBe 2L
    unknownAppChangeWeightChangeOut.dfAppetiteChangeLastYear.value shouldBe 99L
    unknownAppChangeWeightChangeOut.dfEverMalnourished.value shouldBe 99L
    unknownAppChangeWeightChangeOut.dfEverUnderweight.value shouldBe 1L
    unknownAppChangeWeightChangeOut.dfEverOverweight.value shouldBe 0L
    unknownAppChangeWeightChangeOut.dfWeightChangeLastYear.value shouldBe 2L
  }

  it should "map information about primary diet components" in {
    val example = Map(
      "df_prim" -> Array("98"),
      "df_prim_other" -> Array("Noms"),
      "df_prim_org" -> Array("0"),
      "df_prim_gf" -> Array("1"),
      "df_prim_gf_past" -> Array("0"),
      "df_prim_change_12m" -> Array("1"),
      "df_prim_change_12m_when" -> Array("5"),
      "df_prim_change_why" -> Array("1", "3", "5", "98"),
      "df_prim_change_why_other" -> Array("Felt like it")
    )

    val out = DietTransformations.mapPrimaryComponent(
      RawRecord(1, example),
      HlesDogDiet.init()
    )

    out.dfPrimaryDietComponent.value shouldBe 98L
    out.dfPrimaryDietComponentOtherDescription.value shouldBe "Noms"
    out.dfPrimaryDietComponentOrganic.value shouldBe false
    out.dfPrimaryDietComponentGrainFree.value shouldBe true
    out.dfPrimaryDietComponentGrainFreePast.value shouldBe false
    out.dfPrimaryDietComponentChangeRecent.value shouldBe true
    out.dfPrimaryDietComponentChangeMonthsAgo.value shouldBe 5L
    out.dfPrimaryDietComponentChangeAllergyRelated.value shouldBe true
    out.dfPrimaryDietComponentChangeDifferentLifeStage.value shouldBe false
    out.dfPrimaryDietComponentChangeStopGrainFree.value shouldBe true
    out.dfPrimaryDietComponentChangeHealthConditionSpecific.value shouldBe false
    out.dfPrimaryDietComponentChangeBrandChange.value shouldBe true
    out.dfPrimaryDietComponentChangeNewFoodSameBrand.value shouldBe false
    out.dfPrimaryDietComponentChangeOther.value shouldBe true
    out.dfPrimaryDietComponentChangeOtherDescription.value shouldBe "Felt like it"
  }

  it should "map information about secondary diet components" in {
    val no2nd = Map(
      "df_sec_yn" -> Array("0"),
      "df_sec" -> Array("2")
    )

    val with2nd = Map(
      "df_sec_yn" -> Array("1"),
      "df_sec" -> Array("98"),
      "df_sec_other" -> Array("Noms"),
      "df_sec_org" -> Array("0"),
      "df_sec_gf" -> Array("1"),
      "df_sec_gf_past" -> Array("0"),
      "df_sec_change_12m" -> Array("1"),
      "df_sec_change_12m_when" -> Array("5"),
      "df_sec_change_why" -> Array("1", "3", "5", "98"),
      "df_sec_change_why_other" -> Array("Felt like it")
    )

    val no2ndOut = DietTransformations.mapSecondaryComponent(
      RawRecord(1, no2nd),
      HlesDogDiet.init()
    )

    val with2ndOut = DietTransformations.mapSecondaryComponent(
      RawRecord(1, with2nd),
      HlesDogDiet.init()
    )

    no2ndOut.dfSecondaryDietComponentUsed.value shouldBe false
    no2ndOut.dfSecondaryDietComponent shouldBe None

    with2ndOut.dfSecondaryDietComponentUsed.value shouldBe true
    with2ndOut.dfSecondaryDietComponent.value shouldBe 98L
    with2ndOut.dfSecondaryDietComponentOtherDescription.value shouldBe "Noms"
    with2ndOut.dfSecondaryDietComponentOrganic.value shouldBe false
    with2ndOut.dfSecondaryDietComponentGrainFree.value shouldBe true
    with2ndOut.dfSecondaryDietComponentGrainFreePast.value shouldBe false
    with2ndOut.dfSecondaryDietComponentChangeRecent.value shouldBe true
    with2ndOut.dfSecondaryDietComponentChangeMonthsAgo.value shouldBe 5L
    with2ndOut.dfSecondaryDietComponentChangeAllergyRelated.value shouldBe true
    with2ndOut.dfSecondaryDietComponentChangeDifferentLifeStage.value shouldBe false
    with2ndOut.dfSecondaryDietComponentChangeStopGrainFree.value shouldBe true
    with2ndOut.dfSecondaryDietComponentChangeHealthConditionSpecific.value shouldBe false
    with2ndOut.dfSecondaryDietComponentChangeBrandChange.value shouldBe true
    with2ndOut.dfSecondaryDietComponentChangeNewFoodSameBrand.value shouldBe false
    with2ndOut.dfSecondaryDietComponentChangeOther.value shouldBe true
    with2ndOut.dfSecondaryDietComponentChangeOtherDescription.value shouldBe "Felt like it"
  }

  it should "map information about treats" in {
    val example = Map(
      "df_treat_freq" -> Array("2"),
      "df_t_biscuit" -> Array("1"),
      "df_t_rawhide" -> Array("0"),
      "df_t_bones" -> Array("1"),
      "df_t_table_meat" -> Array("0"),
      "df_t_table_carbs" -> Array("1"),
      "df_t_veg" -> Array("0"),
      "df_t_hm_prot" -> Array("1"),
      "df_t_hm_biscuit" -> Array("0"),
      "df_t_pumpkin" -> Array("1"),
      "df_t_pb" -> Array("0"),
      "df_t_other" -> Array("1"),
      "df_t_other_text" -> Array("foob")
    )

    val out = DietTransformations.mapTreats(
      RawRecord(1, example),
      HlesDogDiet.init()
    )

    out.dfTreatsFrequency.value shouldBe 2L
    out.dfTreatsCommercialBiscuits.value shouldBe true
    out.dfTreatsRawhide.value shouldBe false
    out.dfTreatsBones.value shouldBe true
    out.dfTreatsTableMeat.value shouldBe false
    out.dfTreatsTableCarbs.value shouldBe true
    out.dfTreatsVegetables.value shouldBe false
    out.dfTreatsHomemadeProtein.value shouldBe true
    out.dfTreatsHomemadeBiscuits.value shouldBe false
    out.dfTreatsPumpkin.value shouldBe true
    out.dfTreatsPeanutButter.value shouldBe false
    out.dfTreatsOther.value shouldBe true
    out.dfTreatsOtherDescription.value shouldBe "foob"
  }

  it should "map information about daily supplements" in {
    val example = Map(
      "df_s_bone_meal" -> Array("0"),
      "df_s_glucosamine" -> Array("1"),
      "df_s_chondroitin" -> Array("2"),
      "df_s_joint_other" -> Array("0"),
      "df_s_omega3" -> Array("1"),
      "df_s_skin" -> Array("2"),
      "df_s_vitamin" -> Array("0"),
      "df_s_enzyme" -> Array("1"),
      "df_s_probiotics" -> Array("2"),
      "df_s_fiber" -> Array("0"),
      "df_s_alkalinize" -> Array("1"),
      "df_s_acidify" -> Array("2"),
      "df_s_taurine" -> Array("0"),
      "df_s_antiox" -> Array("1"),
      "df_s_q10" -> Array("2"),
      "df_s_other" -> Array("0"),
      "df_s_other_text" -> Array("No passthrough")
    )

    val out = DietTransformations.mapDailySupplements(
      RawRecord(1, example),
      HlesDogDiet.init()
    )

    out.dfDailySupplementsBoneMeal shouldBe None
    out.dfDailySupplementsGlucosamine.value shouldBe 1L
    out.dfDailySupplementsChondroitin.value shouldBe 2L
    out.dfDailySupplementsOtherJoint shouldBe None
    out.dfDailySupplementsOmega3.value shouldBe 1L
    out.dfDailySupplementsNonOilSkin.value shouldBe 2L
    out.dfDailySupplementsVitamins shouldBe None
    out.dfDailySupplementsEnzyme.value shouldBe 1L
    out.dfDailySupplementsProbiotics.value shouldBe 2L
    out.dfDailySupplementsFiber shouldBe None
    out.dfDailySupplementsAlkalinize.value shouldBe 1L
    out.dfDailySupplementsAcidify.value shouldBe 2L
    out.dfDailySupplementsTaurine shouldBe None
    out.dfDailySupplementsAntiox.value shouldBe 1L
    out.dfDailySupplementsCoenzymeQ10.value shouldBe 2L
    out.dfDailySupplementsOther shouldBe None
    out.dfDailySupplementsOtherDescription shouldBe None
  }

  it should "map information about supplements taken less-than-daily" in {
    val example = Map(
      "df_s_bone_meal_ltd" -> Array("0"),
      "df_s_glucosamine_ltd" -> Array("1"),
      "df_s_chondroitin_ltd" -> Array("2"),
      "df_s_joint_other_ltd" -> Array("0"),
      "df_s_omega3_ltd" -> Array("1"),
      "df_s_skin_ltd" -> Array("2"),
      "df_s_vitamin_ltd" -> Array("0"),
      "df_s_enzyme_ltd" -> Array("1"),
      "df_s_probiotics_ltd" -> Array("2"),
      "df_s_fiber_ltd" -> Array("0"),
      "df_s_alkalinize_ltd" -> Array("1"),
      "df_s_acidify_ltd" -> Array("2"),
      "df_s_taurine_ltd" -> Array("0"),
      "df_s_antiox_ltd" -> Array("1"),
      "df_s_q10_ltd" -> Array("2"),
      "df_s_other_ltd" -> Array("0"),
      "df_s_other_ltd_text" -> Array("No passthrough")
    )

    val out = DietTransformations.mapInfrequentSupplements(
      RawRecord(1, example),
      HlesDogDiet.init()
    )

    out.dfInfrequentSupplementsBoneMeal shouldBe None
    out.dfInfrequentSupplementsGlucosamine.value shouldBe 1L
    out.dfInfrequentSupplementsChondroitin.value shouldBe 2L
    out.dfInfrequentSupplementsOtherJoint shouldBe None
    out.dfInfrequentSupplementsOmega3.value shouldBe 1L
    out.dfInfrequentSupplementsNonOilSkin.value shouldBe 2L
    out.dfInfrequentSupplementsVitamins shouldBe None
    out.dfInfrequentSupplementsEnzyme.value shouldBe 1L
    out.dfInfrequentSupplementsProbiotics.value shouldBe 2L
    out.dfInfrequentSupplementsFiber shouldBe None
    out.dfInfrequentSupplementsAlkalinize.value shouldBe 1L
    out.dfInfrequentSupplementsAcidify.value shouldBe 2L
    out.dfInfrequentSupplementsTaurine shouldBe None
    out.dfInfrequentSupplementsAntiox.value shouldBe 1L
    out.dfInfrequentSupplementsCoenzymeQ10.value shouldBe 2L
    out.dfInfrequentSupplementsOther shouldBe None
    out.dfInfrequentSupplementsOtherDescription shouldBe None
  }
}
