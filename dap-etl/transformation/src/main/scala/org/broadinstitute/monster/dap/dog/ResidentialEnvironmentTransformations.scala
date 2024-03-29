package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogResidentialEnvironment

object ResidentialEnvironmentTransformations {

  /** Map all residential environment fields out of a raw RedCap record into a partial Dog model. */
  def mapResidentialEnvironment(rawRecord: RawRecord): HlesDogResidentialEnvironment = {
    val init = HlesDogResidentialEnvironment.init()

    val transformations = List(
      mapPastResidences _,
      mapHouse _,
      mapHeating _,
      mapDrinkingWater _,
      mapToxinExposure _,
      mapFlooring _,
      mapProperty _,
      mapNeighborhood _
    )

    transformations.foldLeft(init)((acc, f) => f(rawRecord, acc))
  }

  /**
    * Checks for a given alternate residence country, gated with a check on residence count
    * pastCountryCount arg is the RC variable "de_country_nbr"
    * countryCounter arg is the sequence number for the supplied RC variable (next arg)
    * countryField arg is the RC past residence country we are currently checking
    */
  def getPastResidenceCountry(
    rawRecord: RawRecord,
    pastCountryCount: Long,
    countryCounter: Long,
    countryField: String
  ): Option[String] = {
    if (pastCountryCount > countryCounter) rawRecord.getOptional(countryField) else None
  }

  /**
    * Parse all past-residence-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    * de_country_[n] is the deprecated user entry free text field
    * de_country_[n]_dd is the updated drop down option list
    */
  def mapPastResidences(
    rawRecord: RawRecord,
    dog: HlesDogResidentialEnvironment
  ): HlesDogResidentialEnvironment = {
    val currentResidenceCount = rawRecord.getOptionalBoolean("oc_address2_yn").map(if (_) 2L else 1L)

    rawRecord
      .getOptionalNumber("de_home_nbr")
      .fold(dog.copy(deLifetimeResidenceCount = currentResidenceCount)) { pastResidenceCount =>
        val pastZipCount =
          if (pastResidenceCount > 0) rawRecord.getRequired("de_zip_nbr").toLong else 0L
        val pastCountryCount =
          if (pastResidenceCount > 0) rawRecord.getRequired("de_country_nbr").toLong else 0L

        dog.copy(
          deLifetimeResidenceCount = Some(pastResidenceCount + currentResidenceCount.getOrElse(0L)),
          dePastResidenceZipCount = Some(pastZipCount),
          dePastResidenceCountryCount = Some(pastCountryCount),
          // there are two separate fields to check for "First country"
          dePastResidenceCountry1 =
            if (pastCountryCount == 1)
              rawRecord.getOptional("de_country_01_only_dd")
            else getPastResidenceCountry(rawRecord, pastCountryCount, 1L, "de_country_01_dd"),
          dePastResidenceCountry2 =
            getPastResidenceCountry(rawRecord, pastCountryCount, 1L, "de_country_02_dd"),
          dePastResidenceCountry3 =
            getPastResidenceCountry(rawRecord, pastCountryCount, 2L, "de_country_03_dd"),
          dePastResidenceCountry4 =
            getPastResidenceCountry(rawRecord, pastCountryCount, 3L, "de_country_04_dd"),
          dePastResidenceCountry5 =
            getPastResidenceCountry(rawRecord, pastCountryCount, 4L, "de_country_05_dd"),
          dePastResidenceCountry6 =
            getPastResidenceCountry(rawRecord, pastCountryCount, 5L, "de_country_06_dd"),
          dePastResidenceCountry7 =
            getPastResidenceCountry(rawRecord, pastCountryCount, 6L, "de_country_07_dd"),
          dePastResidenceCountry8 =
            getPastResidenceCountry(rawRecord, pastCountryCount, 7L, "de_country_08_dd"),
          dePastResidenceCountry9 =
            getPastResidenceCountry(rawRecord, pastCountryCount, 8L, "de_country_09_dd"),
          dePastResidenceCountry10 =
            getPastResidenceCountry(rawRecord, pastCountryCount, 9L, "de_country_10_dd"),
          // there are two separate fields to check for "First country"
          dePastResidenceCountry1Text =
            if (pastCountryCount == 1)
              rawRecord.getOptional("de_country_01_only")
            else getPastResidenceCountry(rawRecord, pastCountryCount, 1L, "de_country_01"),
          dePastResidenceCountry2Text =
            getPastResidenceCountry(rawRecord, pastCountryCount, 1L, "de_country_02")
        )
      }
  }

  /**
    * Parse high-level house-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapHouse(
    rawRecord: RawRecord,
    dog: HlesDogResidentialEnvironment
  ): HlesDogResidentialEnvironment =
    dog.copy(
      deHomeAreaType = rawRecord.getOptionalNumber("de_type_area"),
      deHomeType = rawRecord.getOptionalNumber("de_type_home"),
      deHomeTypeOtherDescription = rawRecord.getOptionalStripped("de_type_home_other"),
      deHomeConstructionDecade = rawRecord.getOptionalNumber("de_home_age"),
      deHomeYearsLivedIn = rawRecord.getOptionalNumber("de_home_lived_years"),
      deHomeSquareFootage = rawRecord.getOptionalNumber("de_home_area", truncateDecimals = true)
    )

  /**
    * Parse heating-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapHeating(
    rawRecord: RawRecord,
    dog: HlesDogResidentialEnvironment
  ): HlesDogResidentialEnvironment = {
    val primaryHeat = rawRecord.getOptionalNumber("de_primary_heat")
    val secondaryHeatUsed = rawRecord.getOptionalNumber("de_secondary_heat_yn")
    val secondaryHeat =
      if (secondaryHeatUsed.contains(1L)) rawRecord.getOptionalNumber("de_secondary_heat") else None
    val primaryStove = rawRecord.getOptionalNumber("de_primary_stove")
    val secondaryStoveUsed = rawRecord.getOptionalNumber("de_secondary_stove_yn")
    val secondaryStove = if (secondaryStoveUsed.contains(1L)) {
      rawRecord.getOptionalNumber("de_secondary_stove")
    } else {
      None
    }
    dog.copy(
      dePrimaryHeatFuel = primaryHeat,
      dePrimaryHeatFuelOtherDescription =
        if (primaryHeat.contains(98L)) rawRecord.getOptionalStripped("de_primary_heat_other")
        else None,
      deSecondaryHeatFuelUsed = secondaryHeatUsed,
      deSecondaryHeatFuel = secondaryHeat,
      deSecondaryHeatFuelOtherDescription =
        if (secondaryHeat.contains(98L)) rawRecord.getOptionalStripped("de_secondary_heat_other")
        else None,
      dePrimaryStoveFuel = primaryStove,
      dePrimaryStoveFuelOtherDescription =
        if (primaryStove.contains(98L)) rawRecord.getOptionalStripped("de_primary_stove_other")
        else None,
      deSecondaryStoveFuelUsed = secondaryStoveUsed,
      deSecondaryStoveFuel = secondaryStove,
      deSecondaryStoveFuelOtherDescription = if (secondaryStove.contains(98L)) {
        rawRecord.getOptionalStripped("de_secondary_stove_other")
      } else {
        None
      }
    )
  }

  /**
    * Parse drinking-water-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapDrinkingWater(
    rawRecord: RawRecord,
    dog: HlesDogResidentialEnvironment
  ): HlesDogResidentialEnvironment = {
    val source = rawRecord.getOptionalNumber("de_water_source")
    val knownPipes = rawRecord.getOptionalBoolean("de_pipe_yn")
    val pipeType = knownPipes.flatMap {
      case true  => rawRecord.getOptionalNumber("de_pipe_type")
      case false => Some(99L) // unknown type
    }
    dog.copy(
      deDrinkingWaterSource = source,
      deDrinkingWaterSourceOtherDescription =
        if (source.contains(98L)) rawRecord.getOptional("de_water_source_other") else None,
      deDrinkingWaterIsFiltered = rawRecord.getOptionalNumber("de_water_filter_yn"),
      dePipeType = pipeType,
      dePipeTypeOtherDescription =
        if (pipeType.contains(98L)) rawRecord.getOptional("de_pipe_type_other") else None
    )
  }

  /**
    * Parse toxin-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapToxinExposure(
    rawRecord: RawRecord,
    dog: HlesDogResidentialEnvironment
  ): HlesDogResidentialEnvironment = {
    val woodFireplace = rawRecord.getOptionalNumber("de_wood_burning")
    val gasFireplace = rawRecord.getOptionalNumber("de_gas_fireplace")
    dog.copy(
      deSecondHandSmokeHoursPerDay = rawRecord.getOptionalNumber("de_2nd_hand_smoke_amt"),
      deCentralAirConditioningPresent = rawRecord.getOptionalNumber("de_central_ac"),
      deRoomOrWindowAirConditioningPresent = rawRecord.getOptionalNumber("de_room_ac"),
      deCentralHeatPresent = rawRecord.getOptionalNumber("de_central_heat"),
      deAsbestosPresent = rawRecord.getOptionalNumber("de_asbestos"),
      deRadonPresent = rawRecord.getOptionalNumber("de_radon"),
      deLeadPresent = rawRecord.getOptionalNumber("de_lead"),
      deMothballPresent = rawRecord.getOptionalNumber("de_mothball"),
      deIncensePresent = rawRecord.getOptionalNumber("de_incense"),
      deAirFreshenerPresent = rawRecord.getOptionalNumber("de_air_freshener"),
      deAirCleanerPresent = rawRecord.getOptionalNumber("de_air_cleaner"),
      deHepaPresent = rawRecord.getOptionalNumber("de_hepa"),
      deWoodFireplacePresent = woodFireplace,
      deGasFireplacePresent = gasFireplace,
      deWoodFireplaceLightingsPerWeek = if (woodFireplace.contains(1L)) {
        rawRecord.getOptionalNumber("de_wood_fireplace_lit")
      } else {
        None
      },
      deGasFireplaceLightingsPerWeek =
        if (gasFireplace.contains(1L)) rawRecord.getOptionalNumber("de_gas_fireplace_lit") else None
    )
  }

  /**
    * Parse flooring-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapFlooring(
    rawRecord: RawRecord,
    dog: HlesDogResidentialEnvironment
  ): HlesDogResidentialEnvironment = {
    val hasWood = rawRecord.getOptionalBoolean("de_floor_wood_yn")
    val hasCarpet = rawRecord.getOptionalBoolean("de_floor_carpet_yn")
    val hasConcrete = rawRecord.getOptionalBoolean("de_floor_concrete_yn")
    val hasTile = rawRecord.getOptionalBoolean("de_floor_tile_yn")
    val hasLinoleum = rawRecord.getOptionalBoolean("de_floor_linoleum_yn")
    val hasLaminate = rawRecord.getOptionalBoolean("de_floor_laminate_yn")
    val hasOther = rawRecord.getOptionalBoolean("de_floor_other_yn")
    val hasStairs = rawRecord.getOptionalBoolean("de_stairs")

    dog.copy(
      deFloorTypesWood = hasWood,
      deFloorFrequencyOnWood =
        if (hasWood.contains(true)) rawRecord.getOptionalNumber("de_floor_wood_freq") else None,
      deFloorTypesCarpet = hasCarpet,
      deFloorFrequencyOnCarpet =
        if (hasCarpet.contains(true)) rawRecord.getOptionalNumber("de_floor_carpet_freq") else None,
      deFloorTypesConcrete = hasConcrete,
      deFloorFrequencyOnConcrete = if (hasConcrete.contains(true)) {
        rawRecord.getOptionalNumber("de_floor_concrete_freq")
      } else {
        None
      },
      deFloorTypesTile = hasTile,
      deFloorFrequencyOnTile =
        if (hasTile.contains(true)) rawRecord.getOptionalNumber("de_floor_tile_freq") else None,
      deFloorTypesLinoleum = hasLinoleum,
      deFloorFrequencyOnLinoleum = if (hasLinoleum.contains(true)) {
        rawRecord.getOptionalNumber("de_floor_linoleum_freq")
      } else {
        None
      },
      deFloorTypesLaminate = hasLaminate,
      deFloorFrequencyOnLaminate = if (hasLaminate.contains(true)) {
        rawRecord.getOptionalNumber("de_floor_laminate_freq")
      } else {
        None
      },
      deFloorTypesOther = hasOther,
      deFloorTypesOtherDescription =
        if (hasOther.contains(true)) rawRecord.getOptional("de_floor_other") else None,
      deFloorFrequencyOnOther =
        if (hasOther.contains(true)) rawRecord.getOptionalNumber("de_floor_other_freq") else None,
      deStairsInHome = hasStairs,
      deStairsAvgFlightsPerDay =
        if (hasStairs.contains(true)) rawRecord.getOptionalNumber("de_stairs_nbr") else None
    )
  }

  /**
    * Parse property-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapProperty(
    rawRecord: RawRecord,
    dog: HlesDogResidentialEnvironment
  ): HlesDogResidentialEnvironment = {
    val propertyAccess = rawRecord.getOptionalBoolean("de_property_access_yn")
    val containmentType = if (propertyAccess.contains(true)) {
      rawRecord.getOptionalNumber("de_property_fence_yn")
    } else {
      None
    }
    val drinkingWaterSources = rawRecord.get("de_outside_water")
    val drinkingWaterOther = drinkingWaterSources.map(_.contains("98"))
    val weedControl = rawRecord.getOptionalNumber("de_yard_weed_ctl_yn")
    val pestControl = rawRecord.getOptionalNumber("de_yard_pest_ctl_yn")

    dog.copy(
      dePropertyArea = rawRecord.getOptionalNumber("de_property_size"),
      dePropertyAccessible = propertyAccess,
      dePropertyAreaAccessible = if (propertyAccess.contains(true)) {
        rawRecord.getOptionalNumber("de_property_access")
      } else {
        None
      },
      dePropertyContainmentType = containmentType,
      dePropertyContainmentTypeOtherDescription = if (containmentType.contains(4L)) {
        rawRecord.getOptional("de_property_fence_other")
      } else {
        None
      },
      dePropertyDrinkingWaterNone = drinkingWaterSources.map(_.contains("0")),
      dePropertyDrinkingWaterBowl = drinkingWaterSources.map(_.contains("1")),
      dePropertyDrinkingWaterHose = drinkingWaterSources.map(_.contains("2")),
      dePropertyDrinkingWaterPuddles = drinkingWaterSources.map(_.contains("3")),
      dePropertyDrinkingWaterUnknown = drinkingWaterSources.map(_.contains("99")),
      dePropertyDrinkingWaterOther = drinkingWaterOther,
      dePropertyDrinkingWaterOtherDescription = if (drinkingWaterOther.contains(true)) {
        rawRecord.getOptional("de_outside_water_other")
      } else {
        None
      },
      dePropertyWeedControlFrequency = if (weedControl.contains(1L)) {
        rawRecord.getOptionalNumber("de_yard_weed_ctl_freq")
      } else {
        weedControl
      },
      dePropertyPestControlFrequency = if (pestControl.contains(1L)) {
        rawRecord.getOptionalNumber("de_yard_pest_ctl_freq")
      } else {
        pestControl
      }
    )
  }

  /**
    * Parse neighborhood-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapNeighborhood(
    rawRecord: RawRecord,
    dog: HlesDogResidentialEnvironment
  ): HlesDogResidentialEnvironment = {
    val animalsInteract = rawRecord.getOptionalBoolean("de_animal_interact")
    val humansInteract = rawRecord.getOptionalBoolean("de_human_interact")
    dog.copy(
      deTrafficNoiseInHomeFrequency = rawRecord.getOptionalNumber("de_traffic_noise_house"),
      deTrafficNoiseInPropertyFrequency = rawRecord.getOptionalNumber("de_traffic_noise_yard"),
      deNeighborhoodHasSidewalks = rawRecord.getOptionalNumber("de_sidewalk_yn"),
      deNeighborhoodHasParks = rawRecord.getOptionalBoolean("de_parks_near"),
      deInteractsWithNeighborhoodAnimals = animalsInteract,
      deInteractsWithNeighborhoodAnimalsWithOwner = if (animalsInteract.contains(true)) {
        rawRecord.getOptionalBoolean("de_animal_interact_present")
      } else {
        None
      },
      deInteractsWithNeighborhoodHumans = humansInteract,
      deInteractsWithNeighborhoodHumansWithOwner = if (humansInteract.contains(true)) {
        rawRecord.getOptionalBoolean("de_human_interact_present")
      } else {
        None
      }
    )
  }
}
