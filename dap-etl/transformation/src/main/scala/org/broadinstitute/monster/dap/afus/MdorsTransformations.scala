package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.AfusDogMdors

object MdorsTransformations {

  def mapMdors(rawRecord: RawRecord): AfusDogMdors = {
    val init = AfusDogMdors.init()

    val transformations = List(
      mapMdorsScores _
    )

    transformations.foldLeft(init)((acc, f) => f(rawRecord, acc))
  }

  def mapMdorsScores(rawRecord: RawRecord, dog: AfusDogMdors): AfusDogMdors = {
    dog.copy(
      afusMdors1PlayGames = rawRecord.getOptionalNumber("mdors_1_play_games"),
      afusMdors2Treats = rawRecord.getOptionalNumber("mdors_2_treats"),
      afusMdors3Kiss = rawRecord.getOptionalNumber("mdors_3_kiss"),
      afusMdors4DogInCar = rawRecord.getOptionalNumber("mdors_4_dog_in_car"),
      afusMdors5Hug = rawRecord.getOptionalNumber("mdors_5_hug"),
      afusMdors6RelaxingTogether = rawRecord.getOptionalNumber("mdors_6_relaxing_together"),
      afusMdors7GroomingFrequency = rawRecord.getOptionalNumber("mdors_7_grooming_frequency"),
      afusMdors8VisitingOtherPeople = rawRecord.getOptionalNumber("mdors_8_visiting_other_people"),
      afusMdors9BuyPresents = rawRecord.getOptionalNumber("mdors_9_buy_presents"),
      afusMdors10GetThroughToughTimes =
        rawRecord.getOptionalNumber("mdors_10_get_through_tough_times"),
      afusMdors11DogForComfort = rawRecord.getOptionalNumber("mdors_11_dog_for_comfort"),
      afusMdors12CloseProximity = rawRecord.getOptionalNumber("mdors_12_close_proximity"),
      afusMdors13ConstantCompanionship =
        rawRecord.getOptionalNumber("mdors_13_constant_companionship"),
      afusMdors14ThereForMe = rawRecord.getOptionalNumber("mdors_14_there_for_me"),
      afusMdors15ReasonToGetUp = rawRecord.getOptionalNumber("mdors_15_reason_to_get_up"),
      afusMdors16NeverApart = rawRecord.getOptionalNumber("mdors_16_never_apart"),
      afusMdors17ConstantlyAttentive = rawRecord.getOptionalNumber("mdors_17_constantly_attentive"),
      afusMdors18ShareSecrets = rawRecord.getOptionalNumber("mdors_18_share_secrets"),
      afusMdors19TraumaOfDeath = rawRecord.getOptionalNumber("mdors_19_trauma_of_death"),
      afusMdors20CareIsChore = rawRecord.getOptionalNumber("mdors_20_care_is_chore"),
      afusMdors21InterferesWithActivities =
        rawRecord.getOptionalNumber("mdors_21_interferes_with_activities"),
      afusMdors22MoreTroubleThanItsWorth =
        rawRecord.getOptionalNumber("mdors_22_more_trouble_than_its_worth"),
      afusMdors23AnnoyingToChangePlans =
        rawRecord.getOptionalNumber("mdors_23_annoying_to_change_plans"),
      afusMdors24StoppedEnjoyableActivities =
        rawRecord.getOptionalNumber("mdors_24_stopped_enjoyable_activities"),
      afusMdors25DontLikeOwnership = rawRecord.getOptionalNumber("mdors_25_dont_like_ownership"),
      afusMdors26MakesMess = rawRecord.getOptionalNumber("mdors_26_makes_mess"),
      afusMdors27CostsMoney = rawRecord.getOptionalNumber("mdors_27_costs_money"),
      afusMdors28HardToCareFor = rawRecord.getOptionalNumber("mdors_28_hard_to_care_for"),
      afusMdorsSubscaleDogOwnerInteraction =
        rawRecord.getOptionalNumber("mdors_subscale_dog_owner_interaction"),
      afusMdorsSubscalePerceivedEmotionalCloseness =
        rawRecord.getOptionalNumber("mdors_subscale_perceived_emotional_closeness"),
      afusMdorsSubscalePerceivedCosts = rawRecord.getOptionalNumber("mdors_subscale_perceived_costs")
    )
  }
}
