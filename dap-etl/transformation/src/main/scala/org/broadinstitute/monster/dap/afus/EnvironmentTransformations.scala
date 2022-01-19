package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.AfusDogEnvironment

object EnvironmentTransformations {

  def mapEnvironment(rawRecord: RawRecord): AfusDogEnvironment = {
    val init = AfusDogEnvironment.init()

    val transformations = List(
      mapHouse _,
      mapHeating _,
      mapDrinkingWater _,
      mapToxinExposure _,
      mapFlooring _,
      mapProperty _,
      mapNeighborhood _,
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

  def mapHouse(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    dog.copy(
      afusDePrimaryAddressMoveYn = rawRecord.getOptionalBoolean("fu_de_move"),
      afusDePrimaryAddressMoveCount = rawRecord.getOptionalNumber("fu_de_home_nbr_last_year"),
      afusDeHomeAreaType = rawRecord.getOptionalNumber("fu_de_type_area"),
      afusDeHomeType = rawRecord.getOptionalNumber("fu_de_type_home"),
      afusDeHomeTypeOtherDescription = rawRecord.getOptionalStripped("fu_de_type_home_other"),
      afusDeHomeConstructionDecade = rawRecord.getOptionalNumber("fu_de_home_age"),
      //todo: should we use getOptionalNumber with the truncatedDecimals arg?
      afusDeHomeSquareFootage = rawRecord.getOptional("fu_de_home_area")
    )
  }

  def mapHeating(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val primaryHeat = rawRecord.getOptionalNumber("fu_de_primary_heat")
    val secondaryHeatUsed = rawRecord.getOptionalNumber("fu_de_secondary_heat_yn")
    val secondaryHeat =
      if (secondaryHeatUsed.contains(1L)) rawRecord.getOptionalNumber("fu_de_secondary_heat")
      else None
    val primaryStove = rawRecord.getOptionalNumber("fu_de_primary_stove")
    val secondaryStoveUsed = rawRecord.getOptionalNumber("fu_de_secondary_stove_yn")
    val secondaryStove = if (secondaryStoveUsed.contains(1L)) {
      rawRecord.getOptionalNumber("fu_de_secondary_stove")
    } else {
      None
    }
    dog.copy(
      afusDePrimaryHeatFuel = primaryHeat,
      afusDePrimaryHeatFuelOtherDescription =
        if (primaryHeat.contains(98L)) rawRecord.getOptionalStripped("fu_de_primary_heat_other")
        else None,
      afusDeSecondaryHeatFuelUsed = secondaryHeatUsed,
      afusDeSecondaryHeatFuel = secondaryHeat,
      afusDeSecondaryHeatFuelOtherDescription =
        if (secondaryHeat.contains(98L)) rawRecord.getOptionalStripped("fu_de_secondary_heat_other")
        else None,
      afusDePrimaryStoveFuel = primaryStove,
      afusDePrimaryStoveFuelOtherDescription =
        rawRecord.getOptionalStripped("fu_de_primary_stove_other"),
      afusDeSecondaryStoveFuelUsed = secondaryStoveUsed,
      afusDeSecondaryStoveFuel = secondaryStove,
      afusDeSecondaryStoveFuelOtherDescription =
        if (secondaryStove.contains(98L))
          rawRecord.getOptionalStripped("fu_de_secondary_stove_other")
        else None
    )
  }

  def mapDrinkingWater(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val source = rawRecord.getOptionalNumber("fu_de_water_source")
    val knownPipes = rawRecord.getOptionalBoolean("fu_de_pipe_yn")
    val pipeType = knownPipes.flatMap {
      case true  => rawRecord.getOptionalNumber("fu_de_pipe_type")
      case false => Some(99L) // unknown type
    }
    dog.copy(
      afusDeWaterSource = source,
      afusDeWaterSourceOtherDescription =
        if (source.contains(98L)) rawRecord.getOptionalStripped("fu_de_water_source_other")
        else None,
      afusDeWaterFilterYn = rawRecord.getOptionalNumber("fu_de_water_filter_yn"),
      afusDePipeType = pipeType,
      afusDePipeTypeOtherDescription =
        if (pipeType.contains(98L)) rawRecord.getOptionalStripped("fu_de_pipe_type_other") else None
    )
  }

  def mapToxinExposure(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val woodFireplace = rawRecord.getOptionalNumber("fu_de_wood_burning")
    val gasFireplace = rawRecord.getOptionalNumber("fu_de_gas_fireplace")
    dog.copy(
      afusDeCentralAirConditioningPresent = rawRecord.getOptionalNumber("fu_de_central_ac"),
      afusDeRoomOrWindowAirConditioningPresent = rawRecord.getOptionalNumber("fu_de_room_ac"),
      afusDeCentralHeatPresent = rawRecord.getOptionalNumber("fu_de_central_heat"),
      afusDeAsbestosPresent = rawRecord.getOptionalNumber("fu_de_asbestos"),
      afusDeRadonPresent = rawRecord.getOptionalNumber("fu_de_radon"),
      afusDeLeadPresent = rawRecord.getOptionalNumber("fu_de_lead"),
      afusDeMothballPresent = rawRecord.getOptionalNumber("fu_de_mothball"),
      afusDeIncensePresent = rawRecord.getOptionalNumber("fu_de_incense"),
      afusDeAirFreshenerPresent = rawRecord.getOptionalNumber("fu_de_air_freshener"),
      afusDeAirCleanerPresent = rawRecord.getOptionalNumber("fu_de_air_cleaner"),
      afusDeHepaPresent = rawRecord.getOptionalNumber("fu_de_hepa"),
      afusDeWoodFireplacePresent = woodFireplace,
      afusDeGasFireplacePresent = gasFireplace,
      afusDeSecondHandSmokeHoursPerDay = rawRecord.getOptionalNumber("fu_de_2nd_hand_smoke_amt"),
      afusDeWoodFireplaceLightingsPerWeek =
        if (woodFireplace.contains(1L)) rawRecord.getOptionalNumber("fu_de_wood_fireplace_lit")
        else None,
      afusDeGasFireplaceLightingsPerWeek =
        if (gasFireplace.contains(1L)) rawRecord.getOptionalNumber("fu_de_gas_fireplace_lit")
        else None
    )
  }

  def mapFlooring(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val hasWood = rawRecord.getOptionalBoolean("fu_de_floor_wood_yn")
    val hasCarpet = rawRecord.getOptionalBoolean("fu_de_floor_carpet_yn")
    val hasConcrete = rawRecord.getOptionalBoolean("fu_de_floor_concrete_yn")
    val hasTile = rawRecord.getOptionalBoolean("fu_de_floor_tile_yn")
    val hasLinoleum = rawRecord.getOptionalBoolean("fu_de_floor_linoleum_yn")
    val hasLaminate = rawRecord.getOptionalBoolean("fu_de_floor_laminate_yn")
    val hasOther = rawRecord.getOptionalBoolean("fu_de_floor_other_yn")
    val hasStairs = rawRecord.getOptionalBoolean("fu_de_stairs")
    dog.copy(
      afusDeFloorTypeChange = rawRecord.getOptionalBoolean("fu_de_floor_change"),
      afusDeFloorTypesWood = hasWood,
      afusDeFloorFrequencyOnWood =
        if (hasWood.contains(true)) rawRecord.getOptionalNumber("fu_de_floor_wood_freq") else None,
      afusDeFloorTypesCarpet = hasCarpet,
      afusDeFloorFrequencyOnCarpet =
        if (hasCarpet.contains(true)) rawRecord.getOptionalNumber("fu_de_floor_carpet_freq")
        else None,
      afusDeFloorTypesConcrete = hasConcrete,
      afusDeFloorFrequencyOnConcrete =
        if (hasConcrete.contains(true)) rawRecord.getOptionalNumber("fu_de_floor_concrete_freq")
        else None,
      afusDeFloorTypesTile = hasTile,
      afusDeFloorFrequencyOnTile =
        if (hasTile.contains(true)) rawRecord.getOptionalNumber("fu_de_floor_tile_freq") else None,
      afusDeFloorTypesLinoleum = hasLinoleum,
      afusDeFloorFrequencyOnLinoleum =
        if (hasLinoleum.contains(true)) rawRecord.getOptionalNumber("fu_de_floor_linoleum_freq")
        else None,
      afusDeFloorTypesLaminate = hasLaminate,
      afusDeFloorFrequencyOnLaminate =
        if (hasLaminate.contains(true)) rawRecord.getOptionalNumber("fu_de_floor_laminate_freq")
        else None,
      afusDeFloorTypesOther = hasOther,
      afusDeFloorTypesOtherDescription =
        if (hasOther.contains(true)) rawRecord.getOptionalStripped("fu_de_floor_other") else None,
      afusDeFloorFrequencyOnOther =
        if (hasOther.contains(true)) rawRecord.getOptionalNumber("fu_de_floor_other_freq")
        else None,
      afusDeStairsInHome = hasStairs,
      afusDeStairsAvgFlightsPerDay =
        if (hasStairs.contains(true)) rawRecord.getOptionalNumber("fu_de_stairs_nbr") else None
    )
  }

  def mapProperty(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val propertyAccess = rawRecord.getOptionalBoolean("fu_de_property_access_yn")
    val containmentType = if (propertyAccess.contains(true)) {
      rawRecord.getOptionalNumber("fu_de_property_fence_yn")
    } else {
      None
    }
    val drinkingWaterSources = rawRecord.get("fu_de_outside_water")
    val drinkingWaterOther = drinkingWaterSources.map(_.contains("98"))
    val weedControl = rawRecord.getOptionalNumber("fu_de_yard_weed_ctl_yn")
    val pestControl = rawRecord.getOptionalNumber("fu_de_yard_pest_ctl_yn")
    dog.copy(
      afusDePropertyArea = rawRecord.getOptionalNumber("fu_de_property_size"),
      afusDePropertyAccessible = propertyAccess,
      afusDePropertyAreaAccessible = rawRecord.getOptionalNumber("fu_de_property_access"),
      afusDePropertyContainmentType = containmentType,
      afusDePropertyContainmentTypeOtherDescription =
        rawRecord.getOptionalStripped("fu_de_property_fence_other"),
      afusDePropertyDrinkingWaterNone = drinkingWaterSources.map(_.contains("0")),
      afusDePropertyDrinkingWaterBowl = drinkingWaterSources.map(_.contains("1")),
      afusDePropertyDrinkingWaterHose = drinkingWaterSources.map(_.contains("2")),
      afusDePropertyDrinkingWaterPuddles = drinkingWaterSources.map(_.contains("3")),
      afusDePropertyDrinkingWaterOther = drinkingWaterOther,
      afusDePropertyDrinkingWaterUnknown = drinkingWaterSources.map(_.contains("99")),
      afusDePropertyDrinkingWaterOtherDescription =
        if (drinkingWaterOther.contains(true))
          rawRecord.getOptionalStripped("fu_de_outside_water_other")
        else None,
      afusDePropertyWeedControl = weedControl,
      //todo: else weedControl/PestControl?? - this was incorrect
      afusDePropertyWeedControlFrequency =
        if (weedControl.contains(1L)) rawRecord.getOptionalNumber("fu_de_yard_weed_ctl_freq")
        else None,
      afusDePropertyPestControl = pestControl,
      afusDePropertyPestControlFrequency =
        if (pestControl.contains(1L)) rawRecord.getOptionalNumber("fu_de_yard_pest_ctl_freq")
        else None
    )
  }

  def mapNeighborhood(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val animalsInteract = rawRecord.getOptionalBoolean("fu_de_animal_interact")
    val humansInteract = rawRecord.getOptionalBoolean("fu_de_human_interact")
    dog.copy(
      afusDeTrafficNoiseInHomeFrequency = rawRecord.getOptionalNumber("fu_de_traffic_noise_house"),
      afusDeTrafficNoiseInPropertyFrequency =
        rawRecord.getOptionalNumber("fu_de_traffic_noise_yard"),
      afusDeNeighborhoodHasSidewalks = rawRecord.getOptionalNumber("fu_de_sidewalk_yn"),
      afusDeNeighborhoodHasParks = rawRecord.getOptionalBoolean("fu_de_parks_near"),
      afusDeDogInteractsWithNeighborhoodAnimals = animalsInteract,
      afusDeDogInteractsWithNeighborhoodAnimalsWithOwner =
        if (animalsInteract.contains(true))
          rawRecord.getOptionalBoolean("fu_de_animal_interact_present")
        else None,
      afusDeDogInteractsWithNeighborhoodHumans = humansInteract,
      afusDeDogInteractsWithNeighborhoodHumansWithOwner =
        if (humansInteract.contains(true))
          rawRecord.getOptionalBoolean("fu_de_human_interact_present")
        else None
    )
  }

  def mapDogparks(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val dogPark = rawRecord.getOptionalBoolean("fu_de_dogpark_yn")
    if (dogPark.contains(true)) {
      val dogparkTravel = rawRecord.getOptionalNumber("fu_de_dogpark_get_to")
      dog.copy(
        afusDeDogpark = dogPark,
        afusDeDogparkDaysPerMonth = rawRecord.getOptionalNumber("fu_de_dogpark_freq"),
        afusDeDogparkTravelHow = dogparkTravel,
        afusDeDogparkTravelMethodOtherDescription = if (dogparkTravel.equals(98)) {
          rawRecord.getOptionalStripped("fu_de_dogpark_get_to_other")
        } else None,
        afusDeDogparkTravelTimeHours = rawRecord.getOptionalNumber("fu_de_dogpark_hr"),
        afusDeDogparkTravelTimeMinutes = rawRecord.getOptionalNumber("fu_de_dogpark_min")
      )
    } else {
      dog.copy(
        afusDeDogpark = dogPark
      )
    }
  }

  def mapRecreationalSpaces(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val dogRecSpaces = rawRecord.getOptionalBoolean("fu_de_spaces_yn")
    if (dogRecSpaces.contains(true)) {
      val dogRecSpacesTravel = rawRecord.getOptionalNumber("fu_de_spaces_get_to")
      dog.copy(
        afusDeRecreationalSpaces = dogRecSpaces,
        afusDeRecreationalSpacesDaysPerMonth = rawRecord.getOptionalNumber("fu_de_spaces_freq"),
        afusDeRecreationalSpacesTravelHow = dogRecSpacesTravel,
        afusDeRecreationalSpacesTravelOtherDescription = if (dogRecSpacesTravel.equals(98)) {
          rawRecord.getOptionalStripped("fu_de_spaces_get_to_other")
        } else None,
        afusDeRecreationalSpacesTravelTimeHours = rawRecord.getOptionalNumber("fu_de_spaces_hr"),
        afusDeRecreationalSpacesTravelTimeMinutes = rawRecord.getOptionalNumber("fu_de_spaces_min")
      )
    } else {
      dog.copy(
        afusDeRecreationalSpaces = dogRecSpaces
      )
    }
  }

  def mapWork(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val dogWork = rawRecord.getOptionalBoolean("fu_de_dog_to_work")
    // ONLY if: de_dog_to_work = 1
    if (dogWork.contains(true)) {
      val dogWorkTravel = rawRecord.getOptionalNumber("fu_de_dog_to_work_how")
      dog.copy(
        afusDeWork = dogWork,
        afusDeWorkDaysPerMonth = rawRecord.getOptionalNumber("fu_de_dog_to_work_freq"),
        afusDeWorkTravelHow = dogWorkTravel,
        afusDeWorkTravelMethodOtherDescription = if (dogWorkTravel.contains(98)) {
          rawRecord.getOptionalStripped("fu_de_dog_to_work_how_other")
        } else None,
        afusDeWorkTravelTimeHours = rawRecord.getOptionalNumber("fu_de_dog_to_work_hr"),
        afusDeWorkTravelTimeMinutes = rawRecord.getOptionalNumber("fu_de_dog_to_work_min")
      )
    } else {
      dog.copy(
        afusDeWork = dogWork
      )
    }
  }

  def mapSitter(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val dogSitter = rawRecord.getOptionalBoolean("fu_de_sitter_yn")
    // ONLY if: de_sitter_yn = 1
    if (dogSitter.contains(true)) {
      val dogSitterTravel = rawRecord.getOptionalNumber("fu_de_sitter_how")
      dog.copy(
        afusDeSitterOrDaycare = dogSitter,
        afusDeSitterOrDaycareFrequency = rawRecord.getOptionalNumber("fu_de_sitter_freq"),
        afusDeSitterOrDaycareTravelHow = dogSitterTravel,
        afusDeSitterOrDaycareTravelMethodOtherDescription = if (dogSitterTravel.contains(98)) {
          rawRecord.getOptionalBoolean("fu_de_sitter_how_other")
        } else None,
        afusDeSitterOrDaycareTravelTimeHours = rawRecord.getOptionalNumber("fu_de_sitter_hr"),
        afusDeSitterOrDaycareTravelTimeMinutes = rawRecord.getOptionalNumber("fu_de_sitter_min")
      )
    } else {
      dog.copy(
        afusDeSitterOrDaycare = dogSitter
      )
    }
  }

  def mapEatsFeces(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val deEatsGrassFrequency = rawRecord.getOptionalNumber("fu_de_eat_grass")
    val eatsFeces = rawRecord.getOptionalNumber("fu_de_eat_feces_yn")
    val drinksOutdoorWater = rawRecord.getOptionalBoolean("fu_de_water_outdoor_yn")
    // ONLY if: de_water_outdoor_yn = 1
    val drinksOutdoorWaterFrequency = drinksOutdoorWater.flatMap {
      if (_) {
        rawRecord.getOptionalNumber("fu_de_water_outdoor_freq")
      } else {
        None
      }
    }
    // ONLY if: de_eat_feces_yn = 1
    if (eatsFeces.contains(1L)) {
      val fecesType = rawRecord.get("fu_de_eat_feces_type")
      val eatsFecesOther = fecesType.map(_.contains("98"))
      dog.copy(
        afusDeEatsGrassFrequency = deEatsGrassFrequency,
        afusDeEatsFeces = eatsFeces,
        afusDeEatsFecesOwn = fecesType.map(_.contains("1")),
        afusDeEatsFecesOtherDog = fecesType.map(_.contains("2")),
        afusDeEatsFecesCat = fecesType.map(_.contains("3")),
        afusDeEatsFecesHorse = fecesType.map(_.contains("4")),
        afusDeEatsFecesCattle = fecesType.map(_.contains("5")),
        afusDeEatsFecesWildlife = fecesType.map(_.contains("6")),
        afusDeEatsFecesOther = eatsFecesOther,
        afusDeEatsFecesOtherDescription = if (eatsFecesOther.contains(true)) {
          rawRecord.getOptionalStripped("fu_de_eat_feces_type_other")
        } else None,
        afusDeDogDrinksOutdoorWater = drinksOutdoorWater,
        afusDeDogDrinksOutdoorWaterFrequency = drinksOutdoorWaterFrequency
      )
    } else {
      dog.copy(
        afusDeEatsGrassFrequency = deEatsGrassFrequency,
        afusDeEatsFeces = eatsFeces,
        afusDeDogDrinksOutdoorWater = drinksOutdoorWater,
        afusDeDogDrinksOutdoorWaterFrequency = drinksOutdoorWaterFrequency
      )
    }
  }

  def mapToys(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val toys = rawRecord.getOptionalBoolean("fu_de_toys_yn")
    val nonToys = rawRecord.getOptionalBoolean("fu_de_chew_other_yn")
    // ONLY if: de_toys_yn = 1
    if (toys.contains(true)) {
      val otherToys = rawRecord.getOptionalNumber("fu_de_toy_other_yn")
      dog.copy(
        afusDeRoutineToys = toys,
        afusDeRoutineToysIncludePlastic = rawRecord.getOptionalNumber("fu_de_toy_plastic"),
        afusDeRoutineToysIncludeStuffedFabric =
          rawRecord.getOptionalNumber("fu_de_toy_fabric_stuffed"),
        afusDeRoutineToysIncludeUnstuffedFabric =
          rawRecord.getOptionalNumber("fu_de_toy_fabric_unstuffed"),
        afusDeRoutineToysIncludeRubber = rawRecord.getOptionalNumber("fu_de_toy_rubber"),
        afusDeRoutineToysIncludeMetal = rawRecord.getOptionalNumber("fu_de_toy_metal"),
        afusDeRoutineToysIncludeAnimalProducts = rawRecord.getOptionalNumber("fu_de_toy_animal"),
        afusDeRoutineToysIncludeLatex = rawRecord.getOptionalNumber("fu_de_toy_latex"),
        afusDeRoutineToysIncludeRope = rawRecord.getOptionalNumber("fu_de_toy_rope"),
        afusDeRoutineToysIncludeTennisBalls = rawRecord.getOptionalNumber("fu_de_toy_tennis_ball"),
        afusDeRoutineToysIncludeSticks = rawRecord.getOptionalNumber("fu_de_toy_sticks"),
        afusDeRoutineToysIncludeOther = otherToys,
        afusDeRoutineToysOtherDescription = if (otherToys.contains(1L)) {
          rawRecord.getOptionalStripped("fu_de_toy_other")
        } else None,
        afusDeRoutineToysHoursPerDay = rawRecord.getOptionalNumber("fu_de_toys_amt"),
        afusDeLicksChewsOrPlaysWithNonToys = nonToys
      )
    } else {
      dog.copy(
        afusDeRoutineToys = toys,
        afusDeLicksChewsOrPlaysWithNonToys = nonToys
      )
    }
  }

  def mapSleepLocation(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val nighttimeSleepLocation = rawRecord.getOptionalNumber("fu_de_sleep_location")
    val daySleep = rawRecord.getOptionalBoolean("fu_de_sleep_day_yn")
    val daytimeSleepLocation =
      if (daySleep.contains(true)) {
        rawRecord.getOptionalNumber("fu_de_sleep_location_day")
      } else {
        None
      }
    val daytimeSleepLocationOtherDescription =
      if (daytimeSleepLocation.contains(98L)) {
        rawRecord.getOptionalStripped("fu_de_sleep_loc_day_other")
      } else {
        None
      }
    dog.copy(
      afusDeNighttimeSleepLocation = nighttimeSleepLocation,
      afusDeNighttimeSleepLocationOtherDescription = if (nighttimeSleepLocation.contains(98L)) {
        rawRecord.getOptionalStripped("fu_de_sleep_location_other")
      } else None,
      afusDeNighttimeSleepAvgHours = rawRecord.getOptionalNumber("fu_de_sleep_amt_night"),
      afusDeDaytimeSleepLocationDifferent = daySleep,
      afusDeDaytimeSleepLocation = daytimeSleepLocation,
      afusDeDaytimeSleepLocationOtherDescription = daytimeSleepLocationOtherDescription,
      afusDeDaytimeSleepAvgHours = rawRecord.getOptionalNumber("fu_de_sleep_amt_day")
    )
  }

  def mapToxinsIngested(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val toxinsAmount = rawRecord.getOptionalNumber("fu_de_ingest_bad_amt")
    if (toxinsAmount.exists(_ > 0)) {
      val toxinsDescription =
        rawRecord.getOptionalStripped("fu_de_ingest_bad").map(_.trim).filter(_.nonEmpty)
      val toxinsIngested = rawRecord.get("fu_de_ingest_bad_what")
      val recentToxinsOrHazardsIngestedOther = toxinsIngested.map(_.contains("98"))
      dog.copy(
        afusDeRecentToxinsOrHazardsIngestedFrequency = toxinsAmount,
        afusDeRecentToxinsOrHazardsIngestedChocolate = toxinsIngested.map(_.contains("1")),
        afusDeRecentToxinsOrHazardsIngestedPoison = toxinsIngested.map(_.contains("2")),
        afusDeRecentToxinsOrHazardsIngestedHumanMedication = toxinsIngested.map(_.contains("3")),
        afusDeRecentToxinsOrHazardsIngestedPetMedication = toxinsIngested.map(_.contains("4")),
        afusDeRecentToxinsOrHazardsIngestedGarbageOrFood = toxinsIngested.map(_.contains("5")),
        afusDeRecentToxinsOrHazardsIngestedDeadAnimal = toxinsIngested.map(_.contains("6")),
        afusDeRecentToxinsOrHazardsIngestedToys = toxinsIngested.map(_.contains("7")),
        afusDeRecentToxinsOrHazardsIngestedClothing = toxinsIngested.map(_.contains("8")),
        afusDeRecentToxinsOrHazardsIngestedOther = recentToxinsOrHazardsIngestedOther,
        afusDeIngestBadOtherDescription =
          recentToxinsOrHazardsIngestedOther.fold(toxinsDescription) {
            if (_) {
              rawRecord.getOptionalStripped("fu_de_ingest_bad_what_other")
            } else {
              toxinsDescription
            }
          },
        afusDeIngestBadRequiredVet = rawRecord.getOptionalBoolean("fu_de_ingest_bad_er_yn")
      )
    } else {
      dog.copy(
        afusDeRecentToxinsOrHazardsIngestedFrequency = toxinsAmount
      )
    }
  }

  def mapOtherAnimals(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    val otherAnimals = rawRecord.getOptionalBoolean("fu_de_other_animals_yn")
    //ONLY if: de_other_animals_yn = 1
    if (otherAnimals.contains(true)) {
      val otherPresentAnimalsOther = rawRecord.getOptionalBoolean("fu_de_other_other_yn")
      dog.copy(
        afusDeOtherPresentAnimals = otherAnimals,
        afusDeOtherPresentAnimalsDogs = rawRecord.getOptionalBoolean("fu_de_other_dogs"),
        afusDeOtherPresentAnimalsCats = rawRecord.getOptionalBoolean("fu_de_other_cats"),
        afusDeOtherPresentAnimalsBirds = rawRecord.getOptionalBoolean("fu_de_other_birds"),
        afusDeOtherPresentAnimalsReptiles = rawRecord.getOptionalBoolean("fu_de_other_reptiles"),
        afusDeOtherPresentAnimalsLivestock = rawRecord.getOptionalBoolean("fu_de_other_livestock"),
        afusDeOtherPresentAnimalsHorses = rawRecord.getOptionalBoolean("fu_de_other_horses"),
        afusDeOtherPresentAnimalsRodents = rawRecord.getOptionalBoolean("fu_de_other_rodents"),
        afusDeOtherPresentAnimalsFish = rawRecord.getOptionalBoolean("fu_de_other_fish"),
        afusDeOtherPresentAnimalsWildlife = rawRecord.getOptionalBoolean("fu_de_other_wildlife"),
        afusDeOtherPresentAnimalsOther = otherPresentAnimalsOther,
        afusDeOtherPresentAnimalsOtherDescription = if (otherPresentAnimalsOther.contains(true)) {
          rawRecord.getOptionalStripped("fu_de_other_other")
        } else None,
        afusDeOtherPresentAnimalsIndoorCount =
          rawRecord.getOptionalNumber("fu_de_other_inside_nbr"),
        afusDeOtherPresentAnimalsOutdoorCount =
          rawRecord.getOptionalNumber("fu_de_other_outside_nbr"),
        afusDeOtherPresentAnimalsInteractWithDog =
          rawRecord.getOptionalBoolean("fu_de_other_interact_yn")
      )
    } else {
      dog.copy(
        afusDeOtherPresentAnimals = otherAnimals
      )
    }
  }

  def mapGeneralRoutine(rawRecord: RawRecord, dog: AfusDogEnvironment): AfusDogEnvironment = {
    dog.copy(
      afusDeRoutineConsistency = rawRecord.getOptionalNumber("fu_de_routine_consistent"),
      afusDeRoutineHoursPerDayInCrate = rawRecord.getOptionalNumber("fu_de_amt_crate"),
      afusDeRoutineHoursPerDayRoamingHouse = rawRecord.getOptionalNumber("fu_de_amt_roam_house"),
      afusDeRoutineHoursPerDayInGarage = rawRecord.getOptionalNumber("fu_de_amt_garage"),
      afusDeRoutineHoursPerDayInOutdoorKennel = rawRecord.getOptionalNumber("fu_de_amt_kennel"),
      afusDeRoutineHoursPerDayInYard = rawRecord.getOptionalNumber("fu_de_amt_yard"),
      afusDeRoutineHoursPerDayRoamingOutside =
        rawRecord.getOptionalNumber("fu_de_amt_roam_outside"),
      afusDeRoutineHoursPerDayChainedOutside =
        rawRecord.getOptionalNumber("fu_de_amt_chain_outside"),
      afusDeRoutineHoursPerDayAwayFromHome = rawRecord.getOptionalNumber("fu_de_amt_diff_location"),
      afusDeRoutineHoursPerDayWithOtherAnimals =
        rawRecord.getOptionalNumber("fu_de_amt_other_animals"),
      afusDeRoutineHoursPerDayWithAdults = rawRecord.getOptionalNumber("fu_de_amt_adults"),
      afusDeRoutineHoursPerDayWithTeens = rawRecord.getOptionalNumber("fu_de_amt_teens"),
      afusDeRoutineHoursPerDayWithChildren = rawRecord.getOptionalNumber("fu_de_amt_children")
    )
  }
}
