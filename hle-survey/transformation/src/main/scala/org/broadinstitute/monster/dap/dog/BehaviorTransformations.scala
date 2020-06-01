package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog

object BehaviorTransformations {

  /**
    * Parse all behavior-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapBehavior(rawRecord: RawRecord, dog: HlesDog): HlesDog =
    dog.copy(
      dbExcitementLevelBeforeWalk = rawRecord.getOptionalNumber("db_e_before_walk"),
      dbExcitementLevelBeforeCarRide = rawRecord.getOptionalNumber("db_e_before_car"),
      dbAggressionLevelOnLeashUnknownHuman = rawRecord.getOptionalNumber("db_a_approach_walk"),
      dbAggressionLevelToysTakenAway = rawRecord.getOptionalNumber("db_a_toy_family"),
      dbAggressionLevelApproachedWhileEating = rawRecord.getOptionalNumber("db_a_food_approach_family"),
      dbAggressionLevelDeliveryWorkersAtHome = rawRecord.getOptionalNumber("db_a_delivery"),
      dbAggressionLevelFoodTakenAway = rawRecord.getOptionalNumber("db_a_food_take_family"),
      dbAggressionLevelOnLeashUnknownDog = rawRecord.getOptionalNumber("db_a_dog_approach"),
      dbAggressionLevelUnknownHumanNearYard = rawRecord.getOptionalNumber("db_a_stranger_yard"),
      dbAggressionLevelUnknownAggressiveDog = rawRecord.getOptionalNumber("db_a_dog_growled_at"),
      dbAggressionLevelFamiliarDogWhileEating = rawRecord.getOptionalNumber("db_a_food_familiar_dog"),
      dbAggressionLevelFamiliarDogWhilePlaying = rawRecord.getOptionalNumber("db_a_toy_familiar_dog"),
      dbFearLevelUnknownHumanAwayFromHome = rawRecord.getOptionalNumber("db_f_approach_stranger"),
      dbFearLevelLoudNoises = rawRecord.getOptionalNumber("db_f_noise"),
      dbFearLevelUnknownHumanTouch = rawRecord.getOptionalNumber("db_f_unfamiliar_touch"),
      dbFearLevelUnknownObjectsOutside = rawRecord.getOptionalNumber("db_f_strange_objects"),
      dbFearLevelUnknownDogs = rawRecord.getOptionalNumber("db_f_strange_dog"),
      dbFearLevelUnknownSituations = rawRecord.getOptionalNumber("db_f_strange_situations"),
      dbFearLevelUnknownAggressiveDog = rawRecord.getOptionalNumber("db_f_strange_dog_growl"),
      dbFearLevelNailsClippedAtHome = rawRecord.getOptionalNumber("db_f_nails_family"),
      dbFearLevelBathedAtHome = rawRecord.getOptionalNumber("db_f_bath_family"),
      dbLeftAloneRestlessnessFrequency = rawRecord.getOptionalNumber("db_s_agitation"),
      dbLeftAloneBarkingFrequency = rawRecord.getOptionalNumber("db_s_bark"),
      dbLeftAloneScratchingFrequency = rawRecord.getOptionalNumber("db_s_scratching"),
      dbAttentionSeekingFollowsHumansFrequency = rawRecord.getOptionalNumber("db_at_follow"),
      dbAttentionSeekingSitsCloseToHumansFrequency = rawRecord.getOptionalNumber("db_at_sit_close"),
      dbTrainingObeysSitCommandFrequency = rawRecord.getOptionalNumber("db_td_sit"),
      dbTrainingObeysStayCommandFrequency = rawRecord.getOptionalNumber("db_td_stay"),
      dbTrainingDistractionFrequency = rawRecord.getOptionalNumber("db_td_distracted"),
      dbChasesBirdsFrequency = rawRecord.getOptionalNumber("db_m_chase_bird"),
      dbChasesSquirrelsFrequency = rawRecord.getOptionalNumber("db_m_chase_squirrel"),
      dbEscapesHomeOrPropertyFrequency = rawRecord.getOptionalNumber("db_m_escape"),
      dbChewsInappropriateObjectsFrequency = rawRecord.getOptionalNumber("db_m_chew"),
      dbPullsLeashFrequency = rawRecord.getOptionalNumber("db_m_pull"),
      dbUrinatesInHomeFrequency = rawRecord.getOptionalNumber("db_m_urine_object"),
      dbUrinatesAloneFrequency = rawRecord.getOptionalNumber("db_m_urine_alone"),
      dbDefecatesAloneFrequency = rawRecord.getOptionalNumber("db_m_defecate_alone"),
      dbHyperactiveFrequency = rawRecord.getOptionalNumber("db_m_hyperactive"),
      dbPlayfulFrequency = rawRecord.getOptionalNumber("db_m_playful"),
      dbEnergeticFrequency = rawRecord.getOptionalNumber("db_m_active"),
      dbChasesTailFrequency = rawRecord.getOptionalNumber("db_m_chase_tail"),
      dbBarksFrequency = rawRecord.getOptionalNumber("db_m_bark")
    )
}