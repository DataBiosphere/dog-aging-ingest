package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BehaviorTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "BehaviorTransformations"

  it should "map behavior fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "db_e_before_walk" -> Array("0"),
      "db_e_before_car" -> Array("1"),
      "db_a_approach_walk" -> Array("2"),
      "db_a_toy_family" -> Array("3"),
      "db_a_food_approach_family" -> Array("4"),
      "db_a_delivery" -> Array("0"),
      "db_a_food_take_family" -> Array("1"),
      "db_a_dog_approach" -> Array("2"),
      "db_a_stranger_yard" -> Array("3"),
      "db_a_dog_growled_at" -> Array("4"),
      "db_a_food_familiar_dog" -> Array("0"),
      "db_a_toy_familiar_dog" -> Array("1"),
      "db_f_approach_stranger" -> Array("2"),
      "db_f_noise" -> Array("3"),
      "db_f_unfamiliar_touch" -> Array("4"),
      "db_f_strange_objects" -> Array("0"),
      "db_f_strange_dog" -> Array("1"),
      "db_f_strange_situations" -> Array("2"),
      "db_f_strange_dog_growl" -> Array("3"),
      "db_f_nails_family" -> Array("4"),
      "db_f_bath_family" -> Array("0"),
      "db_s_agitation" -> Array("1"),
      "db_s_bark" -> Array("2"),
      "db_s_scratching" -> Array("3"),
      "db_at_follow" -> Array("4"),
      "db_at_sit_close" -> Array("0"),
      "db_td_sit" -> Array("1"),
      "db_td_stay" -> Array("2"),
      "db_td_distracted" -> Array("3"),
      "db_m_chase_bird" -> Array("4"),
      "db_m_chase_squirrel" -> Array("0"),
      "db_m_escape" -> Array("1"),
      "db_m_chew" -> Array("2"),
      "db_m_pull" -> Array("3"),
      "db_m_urine_object" -> Array("4"),
      "db_m_urine_alone" -> Array("0"),
      "db_m_defecate_alone" -> Array("1"),
      "db_m_hyperactive" -> Array("2"),
      "db_m_playful" -> Array("3"),
      "db_m_active" -> Array("4"),
      "db_m_chase_tail" -> Array("0"),
      "db_m_bark" -> Array("1")
    )
    val output = BehaviorTransformations.mapBehavior(
      RawRecord(id = 1, exampleDogFields),
      HlesDog.init(dogId = 1, ownerId = 1)
    )

    output.dbExcitementLevelBeforeWalk.value shouldBe 0
    output.dbExcitementLevelBeforeCarRide.value shouldBe 1
    output.dbAggressionLevelOnLeashUnknownHuman.value shouldBe 2
    output.dbAggressionLevelToysTakenAway.value shouldBe 3
    output.dbAggressionLevelApproachedWhileEating.value shouldBe 4
    output.dbAggressionLevelDeliveryWorkersAtHome.value shouldBe 0
    output.dbAggressionLevelFoodTakenAway.value shouldBe 1
    output.dbAggressionLevelOnLeashUnknownDog.value shouldBe 2
    output.dbAggressionLevelUnknownHumanNearYard.value shouldBe 3
    output.dbAggressionLevelUnknownAggressiveDog.value shouldBe 4
    output.dbAggressionLevelFamiliarDogWhileEating.value shouldBe 0
    output.dbAggressionLevelFamiliarDogWhilePlaying.value shouldBe 1
    output.dbFearLevelUnknownHumanAwayFromHome.value shouldBe 2
    output.dbFearLevelLoudNoises.value shouldBe 3
    output.dbFearLevelUnknownHumanTouch.value shouldBe 4
    output.dbFearLevelUnknownObjectsOutside.value shouldBe 0
    output.dbFearLevelUnknownDogs.value shouldBe 1
    output.dbFearLevelUnknownSituations.value shouldBe 2
    output.dbFearLevelUnknownAggressiveDog.value shouldBe 3
    output.dbFearLevelNailsClippedAtHome.value shouldBe 4
    output.dbFearLevelBathedAtHome.value shouldBe 0
    output.dbLeftAloneRestlessnessFrequency.value shouldBe 1
    output.dbLeftAloneBarkingFrequency.value shouldBe 2
    output.dbLeftAloneScratchingFrequency.value shouldBe 3
    output.dbAttentionSeekingFollowsHumansFrequency.value shouldBe 4
    output.dbAttentionSeekingSitsCloseToHumansFrequency.value shouldBe 0
    output.dbTrainingObeysSitCommandFrequency.value shouldBe 1
    output.dbTrainingObeysStayCommandFrequency.value shouldBe 2
    output.dbTrainingDistractionFrequency.value shouldBe 3
    output.dbChasesBirdsFrequency.value shouldBe 4
    output.dbChasesSquirrelsFrequency.value shouldBe 0
    output.dbEscapesHomeOrPropertyFrequency.value shouldBe 1
    output.dbChewsInappropriateObjectsFrequency.value shouldBe 2
    output.dbPullsLeashFrequency.value shouldBe 3
    output.dbUrinatesInHomeFrequency.value shouldBe 4
    output.dbUrinatesAloneFrequency.value shouldBe 0
    output.dbDefecatesAloneFrequency.value shouldBe 1
    output.dbHyperactiveFrequency.value shouldBe 2
    output.dbPlayfulFrequency.value shouldBe 3
    output.dbEnergeticFrequency.value shouldBe 4
    output.dbChasesTailFrequency.value shouldBe 0
    output.dbBarksFrequency.value shouldBe 1
  }
}
