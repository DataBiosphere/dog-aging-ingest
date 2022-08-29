package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.AfusDogDora

object DoraTransformations {

  def mapDora(rawRecord: RawRecord): AfusDogDora = {
    val init = AfusDogDora.init()

    val transformations = List(
      mapDoraScores _
    )

    transformations.foldLeft(init)((acc, f) => f(rawRecord, acc))
  }

  def mapDoraScores(rawRecord: RawRecord, dog: AfusDogDora): AfusDogDora = {
    dog.copy(
      afusDora1ExcitedFood = rawRecord.getOptionalNumber("dora_1_excited_food"),
      afusDora2MostWalksOffLeash = rawRecord.getOptionalNumber("dora_2_most_walks_off_leash"),
      afusDora3HumanLeftoversInBowl = rawRecord.getOptionalNumber("dora_3_human_leftovers_in_bowl"),
      afusDora4WaitsForScraps = rawRecord.getOptionalNumber("dora_4_waits_for_scraps"),
      afusDora5ChoosyTreats = rawRecord.getOptionalNumber("dora_5_choosy_treats"),
      afusDora6WaitsDuringFoodPrep = rawRecord.getOptionalNumber("dora_6_waits_during_food_prep"),
      afusDora7TurnsDownFood = rawRecord.getOptionalNumber("dora_7_turns_down_food"),
      afusDora8FinishesMealQuickly = rawRecord.getOptionalNumber("dora_8_finishes_meal_quickly"),
      afusDora9InspectsUnfamilarFoods =
        rawRecord.getOptionalNumber("dora_9_inspects_unfamilar_foods"),
      afusDora10RunsAroundAlot = rawRecord.getOptionalNumber("dora_10_runs_around_alot"),
      afusDora11InterestedEatingAfterMeal =
        rawRecord.getOptionalNumber("dora_11_interested_eating_after_meal"),
      afusDora12SlowEater = rawRecord.getOptionalNumber("dora_12_slow_eater"),
      afusDora13EatsTreatsQuickly = rawRecord.getOptionalNumber("dora_13_eats_treats_quickly"),
      afusDora14HumanFoodDuringMeals =
        rawRecord.getOptionalNumber("dora_14_human_food_during_meals"),
      afusDora15EatAnything = rawRecord.getOptionalNumber("dora_15_eat_anything"),
      afusDora16VeryFit = rawRecord.getOptionalNumber("dora_16_very_fit"),
      afusDora17HumanFoodOften = rawRecord.getOptionalNumber("dora_17_human_food_often"),
      afusDora18UpsetStomachSomeFoods =
        rawRecord.getOptionalNumber("dora_18_upset_stomach_some_foods"),
      afusDora19ShouldLoseWeight = rawRecord.getOptionalNumber("dora_19_should_lose_weight"),
      afusDora20MostWalksOnLeash = rawRecord.getOptionalNumber("dora_20_most_walks_on_leash"),
      afusDora21RestrictExercise = rawRecord.getOptionalNumber("dora_21_restrict_exercise"),
      afusDora22DietToControlWeight = rawRecord.getOptionalNumber("dora_22_diet_to_control_weight"),
      afusDora23HungryAllTime = rawRecord.getOptionalNumber("dora_23_hungry_all_time"),
      afusDora24WalksHighEnergy = rawRecord.getOptionalNumber("dora_24_walks_high_energy"),
      afusDora25CarefulDogsWeight = rawRecord.getOptionalNumber("dora_25_careful_dogs_weight"),
      afusDora26SensitiveStomach = rawRecord.getOptionalNumber("dora_26_sensitive_stomach"),
      afusDora27VeryGreedy = rawRecord.getOptionalNumber("dora_27_very_greedy"),
      afusDora28VetOftenForHealthProblems =
        rawRecord.getOptionalNumber("dora_28_vet_often_for_health_problems"),
      afusDora29HappyDogsWeight = rawRecord.getOptionalNumber("dora_29_happy_dogs_weight"),
      afusDora30MeasureDogsFood = rawRecord.getOptionalNumber("dora_30_measure_dogs_food"),
      afusDora31RegulateExerciseForWeight =
        rawRecord.getOptionalNumber("dora_31_regulate_exercise_for_weight"),
      afusDora32GetsLotsOfExercise = rawRecord.getOptionalNumber("dora_32_gets_lots_of_exercise"),
      afusDora33UpsetStomachOften = rawRecord.getOptionalNumber("dora_33_upset_stomach_often"),
      afusDora34NoHumanFoodDuringMeals =
        rawRecord.getOptionalNumber("dora_34_no_human_food_during_meals"),
      afusDora35EatNonFoodObjects = rawRecord.getOptionalNumber("dora_35_eat_non_food_objects"),
      afusDoraFactorFoodResponsivenessSatiety =
        rawRecord.getOptionalFloat("dora_factor_food_responsiveness_satiety"),
      afusDoraFactorLackOfFussiness = rawRecord.getOptionalFloat("dora_factor_lack_of_fussiness"),
      afusDoraFactorInterestInFood = rawRecord.getOptionalFloat("dora_factor_interest_in_food"),
      afusDoraFactorOwnerPerception = rawRecord.getOptionalFloat("dora_factor_owner_perception"),
      afusDoraFactorOwnerInterventionControlWeight =
        rawRecord.getOptionalFloat("dora_factor_owner_intervention_control_weight"),
      afusDoraFactorRestrictionHumanFood =
        rawRecord.getOptionalFloat("dora_factor_restriction_human_food"),
      afusDoraFactorExerciseTaken = rawRecord.getOptionalFloat("dora_factor_exercise_taken"),
      afusDoraFactorSignsGiDisease = rawRecord.getOptionalFloat("dora_factor_signs_gi_disease"),
      afusDoraFactorCurrentDisease = rawRecord.getOptionalFloat("dora_factor_current_disease"),
      afusDoraScoreDogFoodMotivation = rawRecord.getOptionalFloat("dora_score_dog_food_motivation"),
      afusDoraScoreOwnerManagement = rawRecord.getOptionalFloat("dora_score_owner_management")
    )
  }
}
