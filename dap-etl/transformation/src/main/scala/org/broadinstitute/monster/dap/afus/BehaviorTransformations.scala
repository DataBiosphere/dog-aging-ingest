package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.AfusDogBehavior

object BehaviorTransformations {

  def mapBehavior(rawRecord: RawRecord): AfusDogBehavior = {
    val init = AfusDogBehavior.init()

    val transformations = List(
      mapBehaviors _
    )

    transformations.foldLeft(init)((acc, f) => f(rawRecord, acc))
  }

  def mapBehaviors(rawRecord: RawRecord, dog: AfusDogBehavior): AfusDogBehavior = {
    dog.copy(
      afusDbExcitementLevelBeforeWalk = rawRecord.getOptionalNumber("fu_db_e_before_walk"),
      afusDbExcitementLevelBeforeCarRide = rawRecord.getOptionalNumber("fu_db_e_before_car"),
      afusDbAggressionLevelOnLeashUnknownHuman =
        rawRecord.getOptionalNumber("fu_db_a_approach_walk"),
      afusDbAggressionLevelToysTakenAway = rawRecord.getOptionalNumber("fu_db_a_toy_family"),
      afusDbAggressionLevelApproachedWhileEating =
        rawRecord.getOptionalNumber("fu_db_a_food_approach_family"),
      afusDbAggressionLevelDeliveryWorkersAtHome = rawRecord.getOptionalNumber("fu_db_a_delivery"),
      afusDbAggressionLevelFoodTakenAway = rawRecord.getOptionalNumber("fu_db_a_food_take_family"),
      afusDbAggressionLevelOnLeashUnknownDog = rawRecord.getOptionalNumber("fu_db_a_dog_approach"),
      afusDbAggressionLevelUnknownHumanNearYard =
        rawRecord.getOptionalNumber("fu_db_a_stranger_yard"),
      afusDbAggressionLevelUnknownAggressiveDog =
        rawRecord.getOptionalNumber("fu_db_a_dog_growled_at"),
      afusDbAggressionLevelFamiliarDogWhileEating =
        rawRecord.getOptionalNumber("fu_db_a_food_familiar_dog"),
      afusDbAggressionLevelFamiliarDogWhilePlaying =
        rawRecord.getOptionalNumber("fu_db_a_toy_familiar_dog"),
      afusDbFearLevelUnknownHumanAwayFromHome =
        rawRecord.getOptionalNumber("fu_db_f_approach_stranger"),
      afusDbFearLevelLoudNoises = rawRecord.getOptionalNumber("fu_db_f_noise"),
      afusDbFearLevelUnknownHumanTouch = rawRecord.getOptionalNumber("fu_db_f_unfamiliar_touch"),
      afusDbFearLevelUnknownObjectsOutside = rawRecord.getOptionalNumber("fu_db_f_strange_objects"),
      afusDbFearLevelUnknownDogs = rawRecord.getOptionalNumber("fu_db_f_strange_dog"),
      afusDbFearLevelUnknownSituations = rawRecord.getOptionalNumber("fu_db_f_strange_situations"),
      afusDbFearLevelUnknownAggressiveDog =
        rawRecord.getOptionalNumber("fu_db_f_strange_dog_growl"),
      afusDbFearLevelNailsClippedAtHome = rawRecord.getOptionalNumber("fu_db_f_nails_family"),
      afusDbFearLevelBathedAtHome = rawRecord.getOptionalNumber("fu_db_f_bath_family"),
      afusDbLeftAloneRestlessnessFrequency = rawRecord.getOptionalNumber("fu_db_s_agitation"),
      afusDbLeftAloneBarkingFrequency = rawRecord.getOptionalNumber("fu_db_s_bark"),
      afusDbLeftAloneScratchingFrequency = rawRecord.getOptionalNumber("fu_db_s_scratching"),
      afusDbAttentionSeekingFollowsHumansFrequency = rawRecord.getOptionalNumber("fu_db_at_follow"),
      afusDbAttentionSeekingSitsCloseToHumansFrequency =
        rawRecord.getOptionalNumber("fu_db_at_sit_close"),
      afusDbTrainingObeysSitCommandFrequency = rawRecord.getOptionalNumber("fu_db_td_sit"),
      afusDbTrainingObeysStayCommandFrequency = rawRecord.getOptionalNumber("fu_db_td_stay"),
      afusDbTrainingDistractionFrequency = rawRecord.getOptionalNumber("fu_db_td_distracted"),
      afusDbChasesBirdsFrequency = rawRecord.getOptionalNumber("fu_db_m_chase_bird"),
      afusDbChasesSquirrelsFrequency = rawRecord.getOptionalNumber("fu_db_m_chase_squirrel"),
      afusDbEscapesHomeOrPropertyFrequency = rawRecord.getOptionalNumber("fu_db_m_escape"),
      afusDbChewsInappropriateObjectsFrequency = rawRecord.getOptionalNumber("fu_db_m_chew"),
      afusDbPullsLeashFrequency = rawRecord.getOptionalNumber("fu_db_m_pull"),
      afusDbUrinatesInHomeFrequency = rawRecord.getOptionalNumber("fu_db_m_urine_object"),
      afusDbUrinatesAloneFrequency = rawRecord.getOptionalNumber("fu_db_m_urine_alone"),
      afusDbDefecatesAloneFrequency = rawRecord.getOptionalNumber("fu_db_m_defecate_alone"),
      afusDbHyperactiveFrequency = rawRecord.getOptionalNumber("fu_db_m_hyperactive"),
      afusDbPlayfulFrequency = rawRecord.getOptionalNumber("fu_db_m_playful"),
      afusDbEnergeticFrequency = rawRecord.getOptionalNumber("fu_db_m_active"),
      afusDbChasesTailFrequency = rawRecord.getOptionalNumber("fu_db_m_chase_tail"),
      afusDbBarksFrequency = rawRecord.getOptionalNumber("fu_db_m_bark")
    )
  }
}
