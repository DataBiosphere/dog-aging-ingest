package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogRoutineEnvironment

object RoutineEnvironmentTransformations {

  /**
    * Parse all routine environment fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapRoutineEnvironment(rawRecord: RawRecord): HlesDogRoutineEnvironment = {
    val init = HlesDogRoutineEnvironment.init()

    val transformations = List(
      mapDogparks _,
      mapRecreationalSpaces _,
      mapWork _,
      mapSitter _,
      mapEatsFeces _,
      mapToys _,
      mapSleepLocation _,
      mapToxinsIngested _,
      mapOtherAnimals _,
      mapGeneralRoutine _
    )

    transformations.foldLeft(init)((acc, f) => f(rawRecord, acc))
  }

  /* Dog Parks */
  def mapDogparks(
    rawRecord: RawRecord,
    dog: HlesDogRoutineEnvironment
  ): HlesDogRoutineEnvironment = {
    val dogPark = rawRecord.getOptionalBoolean("de_dogpark_yn")
    // ONLY if: de_dogpark_yn = 1
    if (dogPark.contains(true)) {
      val dogparkTravel = rawRecord.get("de_dogpark_get_to")
      val dogparkTravelOther = dogparkTravel.map(_.contains("98"))
      dog.copy(
        deDogpark = dogPark,
        deDogparkDaysPerMonth = rawRecord.getOptionalNumber("de_dogpark_freq"),
        deDogparkTravelWalk = dogparkTravel.map(_.contains("1")),
        deDogparkTravelDrive = dogparkTravel.map(_.contains("2")),
        deDogparkTravelBike = dogparkTravel.map(_.contains("3")),
        deDogparkTravelPublicTransportation = dogparkTravel.map(_.contains("4")),
        deDogparkTravelOther = dogparkTravelOther,
        //ONLY if: de_dogpark_get_to(98) = '1'
        deDogparkTravelOtherDescription =
          if (dogparkTravelOther.contains(true)) rawRecord.getOptional("de_dogpark_get_to_other")
          else None,
        // Compute using hour & min columns, only if de_dogpark_yn = Yes
        // Multiply hours by 60 to get minutes
        deDogparkTravelTimeMinutes = Some(
          (rawRecord.getRequired("de_dogpark_hr").toLong * 60)
            + rawRecord.getRequired("de_dogpark_min").toLong
        )
      )
    } else {
      dog.copy(
        deDogpark = dogPark
      )
    }
  }

  /* Recreational Spaces */
  def mapRecreationalSpaces(
    rawRecord: RawRecord,
    dog: HlesDogRoutineEnvironment
  ): HlesDogRoutineEnvironment = {
    val dogRecSpaces = rawRecord.getOptionalBoolean("de_spaces_yn")
    // ONLY if: de_spaces_yn = 1
    if (dogRecSpaces.contains(true)) {
      val dogRecSpacesTravel = rawRecord.get("de_spaces_get_to")
      val dogRecSpacesTravelOther = dogRecSpacesTravel.map(_.contains("98"))
      dog.copy(
        deRecreationalSpaces = dogRecSpaces,
        deRecreationalSpacesDaysPerMonth = rawRecord.getOptionalNumber("de_spaces_freq"),
        deRecreationalSpacesTravelWalk = dogRecSpacesTravel.map(_.contains("1")),
        deRecreationalSpacesTravelDrive = dogRecSpacesTravel.map(_.contains("2")),
        deRecreationalSpacesTravelBike = dogRecSpacesTravel.map(_.contains("3")),
        deRecreationalSpacesTravelPublicTransportation = dogRecSpacesTravel.map(_.contains("4")),
        deRecreationalSpacesTravelOther = dogRecSpacesTravelOther,
        //ONLY if: de_spaces_get_to(98) = '1'
        deRecreationalSpacesTravelOtherDescription =
          if (dogRecSpacesTravelOther.contains(true))
            rawRecord.getOptional("de_spaces_get_to_other")
          else None,
        // Compute using hour & min columns, only if de_spaces_yn = Yes
        // Multiply hours by 60 to get minutes
        deRecreationalSpacesTravelTimeMinutes = Some(
          (rawRecord.getRequired("de_spaces_hr").toLong * 60)
            + rawRecord.getRequired("de_spaces_min").toLong
        )
      )
    } else {
      dog.copy(
        deRecreationalSpaces = dogRecSpaces
      )
    }
  }

  /* Work */
  def mapWork(rawRecord: RawRecord, dog: HlesDogRoutineEnvironment): HlesDogRoutineEnvironment = {
    val dogWork = rawRecord.getOptionalBoolean("de_dog_to_work")
    // ONLY if: de_dog_to_work = 1
    if (dogWork.contains(true)) {
      val dogWorkTravel = rawRecord.get("de_dog_to_work_how")
      val dogWorkTravelOther = dogWorkTravel.map(_.contains("5"))
      dog.copy(
        deWork = dogWork,
        deWorkDaysPerMonth = rawRecord.getOptionalNumber("de_dog_to_work_freq"),
        deWorkTravelWalk = dogWorkTravel.map(_.contains("1")),
        deWorkTravelDrive = dogWorkTravel.map(_.contains("2")),
        deWorkTravelBike = dogWorkTravel.map(_.contains("3")),
        deWorkTravelPublicTransportation = dogWorkTravel.map(_.contains("4")),
        deWorkTravelOther = dogWorkTravelOther,
        // ONLY if: de_dog_to_work_how(98) = '1'
        deWorkTravelOtherDescription =
          if (dogWorkTravelOther.contains(true)) rawRecord.getOptional("de_dog_to_work_how_other")
          else None,
        // Compute using hour & min columns, only if de_dog_to_work = Yes
        // Multiply hours by 60 to get minutes
        deWorkTravelTimeMinutes = Some(
          (rawRecord.getRequired("de_dog_to_work_hr").toLong * 60)
            + rawRecord.getRequired("de_dog_to_work_min").toLong
        )
      )
    } else {
      dog.copy(
        deWork = dogWork
      )
    }
  }

  /* Sitter */
  def mapSitter(rawRecord: RawRecord, dog: HlesDogRoutineEnvironment): HlesDogRoutineEnvironment = {
    val dogSitter = rawRecord.getOptionalBoolean("de_sitter_yn")
    // ONLY if: de_sitter_yn = 1
    if (dogSitter.contains(true)) {
      val dogSitterTravel = rawRecord.get("de_sitter_how")
      val dogSitterTravelOther = dogSitterTravel.map(_.contains("98"))
      dog.copy(
        deSitterOrDaycare = dogSitter,
        deSitterOrDaycareDaysPerMonth = rawRecord.getOptionalNumber("de_sitter_freq"),
        deSitterOrDaycareTravelWalk = dogSitterTravel.map(_.contains("1")),
        deSitterOrDaycareTravelDrive = dogSitterTravel.map(_.contains("2")),
        deSitterOrDaycareTravelBike = dogSitterTravel.map(_.contains("3")),
        deSitterOrDaycareTravelPublicTransportation = dogSitterTravel.map(_.contains("4")),
        deSitterOrDaycareTravelOther = dogSitterTravelOther,
        // ONLY if: de_sitter_how(98) = '1'
        deSitterOrDaycareTravelOtherDescription =
          if (dogSitterTravelOther.contains(true)) rawRecord.getOptional("de_sitter_how_other")
          else None,
        //Compute using hour & min columns, Multiply hours by 60 to get minutes
        deSitterOrDaycareTravelTimeMinutes = Some(
          (rawRecord.getRequired("de_sitter_hr").toLong * 60)
            + rawRecord.getRequired("de_sitter_min").toLong
        )
      )
    } else {
      dog.copy(
        deSitterOrDaycare = dogSitter
      )
    }
  }

  /* Eats Gross Things */
  def mapEatsFeces(
    rawRecord: RawRecord,
    dog: HlesDogRoutineEnvironment
  ): HlesDogRoutineEnvironment = {
    val deEatsGrassFrequency = rawRecord.getOptionalNumber("de_eat_grass")
    val eatsFeces = rawRecord.getOptionalNumber("de_eat_feces_yn")
    val drinksOutdoorWater = rawRecord.getOptionalBoolean("de_water_outdoor_yn")
    // ONLY if: de_water_outdoor_yn = 1
    val drinksOutdoorWaterFrequency = drinksOutdoorWater.flatMap {
      if (_) rawRecord.getOptionalNumber("de_water_outdoor_freq") else None
    }
    // ONLY if: de_eat_feces_yn = 1
    if (eatsFeces.contains(1L)) {
      val fecesType = rawRecord.get("de_eat_feces_type")
      val eatsFecesOther = fecesType.map(_.contains("98"))
      dog.copy(
        deEatsGrassFrequency = deEatsGrassFrequency,
        deEatsFeces = eatsFeces,
        deEatsFecesOwnFeces = fecesType.map(_.contains("1")),
        deEatsFecesOtherDog = fecesType.map(_.contains("2")),
        deEatsFecesCat = fecesType.map(_.contains("3")),
        deEatsFecesHorse = fecesType.map(_.contains("4")),
        deEatsFecesCattle = fecesType.map(_.contains("5")),
        deEatsFecesWildlife = fecesType.map(_.contains("6")),
        deEatsFecesOther = eatsFecesOther,
        // ONLY if: de_eat_feces_type(98) = '1'
        deEatsFecesOtherDescription =
          if (eatsFecesOther.contains(true)) rawRecord.getOptional("de_eat_feces_type_other")
          else None,
        deDrinksOutdoorWater = drinksOutdoorWater,
        deDrinksOutdoorWaterFrequency = drinksOutdoorWaterFrequency
      )
    } else {
      dog.copy(
        deEatsGrassFrequency = deEatsGrassFrequency,
        deEatsFeces = eatsFeces,
        deDrinksOutdoorWater = drinksOutdoorWater,
        deDrinksOutdoorWaterFrequency = drinksOutdoorWaterFrequency
      )
    }
  }

  /* Toys */
  def mapToys(rawRecord: RawRecord, dog: HlesDogRoutineEnvironment): HlesDogRoutineEnvironment = {
    val toys = rawRecord.getOptionalBoolean("de_toys_yn")
    val nonToys = rawRecord.getOptionalBoolean("de_chew_other_yn")
    // ONLY if: de_toys_yn = 1
    if (toys.contains(true)) { //FIXME
      val otherToys = rawRecord.getOptionalNumber("de_toy_other_yn")
      dog.copy(
        deRoutineToys = toys,
        deRoutineToysIncludePlastic = rawRecord.getOptionalNumber("de_toy_plastic"),
        deRoutineToysIncludeStuffedFabric = rawRecord.getOptionalNumber("de_toy_stuffed_fabric"),
        deRoutineToysIncludeUnstuffedFabric =
          rawRecord.getOptionalNumber("de_toy_fabric_unstuffed"),
        deRoutineToysIncludeRubber = rawRecord.getOptionalNumber("de_toy_rubber"),
        deRoutineToysIncludeMetal = rawRecord.getOptionalNumber("de_toy_metal"),
        deRoutineToysIncludeAnimalProducts = rawRecord.getOptionalNumber("de_toy_animal"),
        deRoutineToysIncludeLatex = rawRecord.getOptionalNumber("de_toy_latex"),
        deRoutineToysIncludeRope = rawRecord.getOptionalNumber("de_toy_rope"),
        deRoutineToysIncludeTennisBalls = rawRecord.getOptionalNumber("de_toy_tennis_ball"),
        deRoutineToysIncludeSticks = rawRecord.getOptionalNumber("de_toy_sticks"),
        deRoutineToysIncludeOther = otherToys,
        // ONLY if: de_toy_other_yn = '1'
        deRoutineToysOtherDescription =
          if (otherToys.contains(true)) rawRecord.getOptional("de_toy_other") else None,
        deRoutineToysHoursPerDay = rawRecord.getOptionalNumber("de_toys_amt"),
        deLicksChewsOrPlaysWithNonToys = nonToys
      )
    } else {
      dog.copy(
        deRoutineToys = toys,
        deLicksChewsOrPlaysWithNonToys = nonToys
      )
    }
  }

  /* Sleep Location */
  def mapSleepLocation(
    rawRecord: RawRecord,
    dog: HlesDogRoutineEnvironment
  ): HlesDogRoutineEnvironment = {
    val nighttimeSleepLocation = rawRecord.getOptionalNumber("de_sleep_location")
    val daySleep = rawRecord.getOptionalBoolean("de_sleep_day_yn")
    // ONLY if: de_sleep_day_yn = 1
    val daytimeSleepLocation =
      if (daySleep.contains(true)) rawRecord.getOptionalNumber("de_sleep_location_day") else None
    dog.copy(
      deNighttimeSleepLocation = nighttimeSleepLocation,
      // ONLY if: de_sleep_location = '98'
      deNighttimeSleepLocationOtherDescription =
        if (nighttimeSleepLocation.contains(98L)) rawRecord.getOptional("de_primary_heat_other")
        else None,
      deNighttimeSleepAvgHours = rawRecord.getOptionalNumber("de_sleep_amt_night"),
      deDaytimeSleepLocationDifferent = daySleep,
      deDaytimeSleepLocation = daytimeSleepLocation,
      // ONLY if: de_sleep_location_day = 98
      deDaytimeSleepLocationOtherDescription =
        if (daytimeSleepLocation.contains(98L)) rawRecord.getOptional("de_sleep_loc_day_other")
        else None,
      deDaytimeSleepAvgHours = rawRecord.getOptionalNumber("de_sleep_amt_day")
    )
  }

  /* Toxins ingested */
  def mapToxinsIngested(
    rawRecord: RawRecord,
    dog: HlesDogRoutineEnvironment
  ): HlesDogRoutineEnvironment = {
    val toxinsAmount = rawRecord.getOptionalNumber("de_ingest_bad_amt")
    //ONLY if: de_ingest_bad_amt = 1 OR de_ingest_bad_amt = 2
    if (toxinsAmount.get > 0) {
      val toxinsDescription = rawRecord.getOptional("de_ingest_bad")
      val toxinsIngested = rawRecord.get("de_ingest_bad_what")
      val recentToxinsOrHazardsIngestedOther = toxinsIngested.map(_.contains("98"))
      dog.copy(
        deRecentToxinsOrHazardsIngestedFrequency = toxinsAmount,
        deRecentToxinsOrHazardsIngestedChocolate = toxinsIngested.map(_.contains("1")),
        deRecentToxinsOrHazardsIngestedPoison = toxinsIngested.map(_.contains("2")),
        deRecentToxinsOrHazardsIngestedHumanMedication = toxinsIngested.map(_.contains("3")),
        deRecentToxinsOrHazardsIngestedPetMedication = toxinsIngested.map(_.contains("4")),
        deRecentToxinsOrHazardsIngestedGarbageOrFood = toxinsIngested.map(_.contains("5")),
        deRecentToxinsOrHazardsIngestedDeadAnimal = toxinsIngested.map(_.contains("6")),
        deRecentToxinsOrHazardsIngestedToys = toxinsIngested.map(_.contains("7")),
        deRecentToxinsOrHazardsIngestedClothing = toxinsIngested.map(_.contains("8")),
        deRecentToxinsOrHazardsIngestedOther = recentToxinsOrHazardsIngestedOther,
        //de_ingest_bad is deprecated, check it first, then check de_ingest_bad_what_other
        deRecentToxinsOrHazardsIngestedOtherDescription =
          if (toxinsDescription.exists(_.trim.nonEmpty)) toxinsDescription
          // ONLY if: de_ingest_bad_what(98) = 1
          else if (recentToxinsOrHazardsIngestedOther.get)
            rawRecord.getOptional("de_ingest_bad_what_other")
          else None, //FIXME CAN THIS BE REFACTORED NEATLY ... USE GETORELSE?
        deRecentToxinsOrHazardsIngestedRequiredVet =
          rawRecord.getOptionalBoolean("de_ingest_bad_er_yn")
      )
    } else {
      dog.copy(
        deRecentToxinsOrHazardsIngestedFrequency = toxinsAmount
      )
    }
  }

  /* Other Animals */
  def mapOtherAnimals(
    rawRecord: RawRecord,
    dog: HlesDogRoutineEnvironment
  ): HlesDogRoutineEnvironment = {
    val otherAnimals = rawRecord.getOptionalBoolean("de_other_animals_yn")
    //ONLY if: de_other_animals_yn = 1
    if (otherAnimals.contains(true)) {
      val otherPresentAnimalsOther = rawRecord.getOptionalBoolean("de_other_other_yn")
      dog.copy(
        deOtherPresentAnimals = otherAnimals,
        deOtherPresentAnimalsDogs = rawRecord.getOptionalBoolean("de_other_dogs"),
        deOtherPresentAnimalsCats = rawRecord.getOptionalBoolean("de_other_cats"),
        deOtherPresentAnimalsBirds = rawRecord.getOptionalBoolean("de_other_birds"),
        deOtherPresentAnimalsReptiles = rawRecord.getOptionalBoolean("de_other_reptiles"),
        deOtherPresentAnimalsLivestock = rawRecord.getOptionalBoolean("de_other_livestock"),
        deOtherPresentAnimalsHorses = rawRecord.getOptionalBoolean("de_other_horses"),
        deOtherPresentAnimalsRodents = rawRecord.getOptionalBoolean("de_other_rodents"),
        deOtherPresentAnimalsFish = rawRecord.getOptionalBoolean("de_other_fish"),
        deOtherPresentAnimalsWildlife = rawRecord.getOptionalBoolean("de_other_wildlife"),
        deOtherPresentAnimalsOther = otherPresentAnimalsOther,
        deOtherPresentAnimalsOtherDescription =
          if (otherPresentAnimalsOther.contains(true)) rawRecord.getOptional("de_other_other")
          else None,
        deOtherPresentAnimalsIndoorCount = rawRecord.getOptionalNumber("de_other_inside_nbr"),
        deOtherPresentAnimalsOutdoorCount = rawRecord.getOptionalNumber("de_other_outside_nbr"),
        deOtherPresentAnimalsInteractWithDog = rawRecord.getOptionalBoolean("de_other_interact_yn")
      )
    } else {
      dog.copy(
        deOtherPresentAnimals = otherAnimals
      )
    }
  }

  /* General Routine */
  def mapGeneralRoutine(
    rawRecord: RawRecord,
    dog: HlesDogRoutineEnvironment
  ): HlesDogRoutineEnvironment =
    dog.copy(
      deRoutineConsistency = rawRecord.getOptionalNumber("de_routine_consistent"),
      deRoutineHoursPerDayInCrate = rawRecord.getOptionalNumber("de_amt_crate"),
      deRoutineHoursPerDayRoamingHouse = rawRecord.getOptionalNumber("de_amt_roam_house"),
      deRoutineHoursPerDayInGarage = rawRecord.getOptionalNumber("de_amt_garage"),
      deRoutineHoursPerDayInOutdoorKennel = rawRecord.getOptionalNumber("de_amt_kennel"),
      deRoutineHoursPerDayInYard = rawRecord.getOptionalNumber("de_amt_yard"),
      deRoutineHoursPerDayRoamingOutside = rawRecord.getOptionalNumber("de_amt_roam_outside"),
      deRoutineHoursPerDayChainedOutside = rawRecord.getOptionalNumber("de_amt_chain_outside"),
      deRoutineHoursPerDayAwayFromHome = rawRecord.getOptionalNumber("de_amt_diff_location"),
      deRoutineHoursPerDayWithOtherAnimals = rawRecord.getOptionalNumber("de_amt_other_animals"),
      deRoutineHoursPerDayWithAdults = rawRecord.getOptionalNumber("de_amt_adults"),
      deRoutineHoursPerDayWithTeens = rawRecord.getOptionalNumber("de_amt_teens"),
      deRoutineHoursPerDayWithChildren = rawRecord.getOptionalNumber("de_amt_children")
    )
}
