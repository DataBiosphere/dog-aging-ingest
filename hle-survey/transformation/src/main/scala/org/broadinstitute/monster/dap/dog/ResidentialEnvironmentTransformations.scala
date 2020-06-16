package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
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
    )

    transformations.foldLeft(init)((acc, f) => f(rawRecord, acc))
  }

  /**
    * Parse all past-residence-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapPastResidences(
    rawRecord: RawRecord,
    dog: HlesDogResidentialEnvironment
  ): HlesDogResidentialEnvironment = {
    val secondaryHome = rawRecord.getOptionalBoolean("oc_address2_yn")
    val pastResidenceCount = rawRecord.getOptionalNumber("de_home_nbr")
    if (secondaryHome.isDefined && pastResidenceCount.isDefined) {
      val currentResidenceCount = if (secondaryHome.head) 2 else 1
      val totalResidenceCount = Some(pastResidenceCount.head + currentResidenceCount)
      val pastZipCount =
        if (totalResidenceCount > 0) rawRecord.getRequired("de_zip_nbr").toLong else 0L
      val pastCountryCount =
        if (totalResidenceCount > 0) rawRecord.getRequired("de_country_nbr").toLong else 0L

      dog.copy(
        deLifetimeResidenceCount = totalResidenceCount,
        dePastResidenceZipCount = Some(pastZipCount),
        dePastResidenceZip1 =
          if (pastZipCount > 1) rawRecord.getOptional("de_zip_01")
          else rawRecord.getOptional("de_zip_01_only"),
        dePastResidenceZip2 = if (pastZipCount > 1) rawRecord.getOptional("de_zip_02") else None,
        dePastResidenceZip3 = if (pastZipCount > 2) rawRecord.getOptional("de_zip_03") else None,
        dePastResidenceZip4 = if (pastZipCount > 3) rawRecord.getOptional("de_zip_04") else None,
        dePastResidenceZip5 = if (pastZipCount > 4) rawRecord.getOptional("de_zip_05") else None,
        dePastResidenceZip6 = if (pastZipCount > 5) rawRecord.getOptional("de_zip_06") else None,
        dePastResidenceZip7 = if (pastZipCount > 6) rawRecord.getOptional("de_zip_07") else None,
        dePastResidenceZip8 = if (pastZipCount > 7) rawRecord.getOptional("de_zip_08") else None,
        dePastResidenceZip9 = if (pastZipCount > 8) rawRecord.getOptional("de_zip_09") else None,
        dePastResidenceZip10 = if (pastZipCount > 9) rawRecord.getOptional("de_zip_10") else None,
        dePastResidenceCountryCount = Some(pastCountryCount),
        dePastResidenceCountry1 =
          if (pastCountryCount > 1) rawRecord.getOptional("de_country_01")
          else rawRecord.getOptional("de_country_01_only"),
        dePastResidenceCountry2 = if (pastCountryCount > 1) rawRecord.getOptional("de_country_02") else None,
        dePastResidenceCountry3 = if (pastCountryCount > 2) rawRecord.getOptional("de_country_03") else None,
        dePastResidenceCountry4 = if (pastCountryCount > 3) rawRecord.getOptional("de_country_04") else None,
        dePastResidenceCountry5 = if (pastCountryCount > 4) rawRecord.getOptional("de_country_05") else None,
        dePastResidenceCountry6 = if (pastCountryCount > 5) rawRecord.getOptional("de_country_06") else None,
        dePastResidenceCountry7 = if (pastCountryCount > 6) rawRecord.getOptional("de_country_07") else None,
        dePastResidenceCountry8 = if (pastCountryCount > 7) rawRecord.getOptional("de_country_08") else None,
        dePastResidenceCountry9 = if (pastCountryCount > 8) rawRecord.getOptional("de_country_09") else None,
        dePastResidenceCountry10 = if (pastCountryCount > 9) rawRecord.getOptional("de_country_10") else None
      )
    }
  }

  /**
    * Parse high-level house-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapHouse(rawRecord: RawRecord, dog: HlesDogResidentialEnvironment): HlesDogResidentialEnvironment = {
    dog.copy(
      deHomeAreaType = rawRecord.getOptionalNumber("de_type_area"),
      deHomeType = rawRecord.getOptionalNumber("de_type_home"),
      deHomeTypeOtherDescription = rawRecord.getOptional("he_type_home_other"),
      deHomeConstructionDecade = rawRecord.getOptionalNumber("de_home_age"),
      deHomeYearsLivedIn = rawRecord.getOptionalNumber("de_home_lived_years"),
      deHomeSquareFootage = rawRecord.getOptionalNumber("de_home_area")
    )
  }


  /**
    * Parse heating-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapHeating(rawRecord: RawRecord, dog: HlesDogResidentialEnvironment): HlesDogResidentialEnvironment = {
    val primaryHeat = rawRecord.getOptionalNumber("de_primary_heat")
    val secondaryHeatUsed = rawRecord.getOptionalNumber("de_secondary_heat_yn")
    val secondaryHeat = if (secondaryHeatUsed.contains(1L)) rawRecord.getOptionalNumber("de_secondary_heat") else None
    val primaryStove = rawRecord.getOptionalNumber("de_primary_stove")
    val secondaryStoveUsed = rawRecord.getOptionalNumber("de_secondary_stove_yn")
    val secondaryStove = if (secondaryStoveUsed.contains(1L)) rawRecord.getOptionalNumber("de_secondary_stove") else None
    dog.copy(
      dePrimaryHeatFuel = primaryHeat,
      dePrimaryHeatFuelOtherDescription = if (primaryHeat.contains(98L)) rawRecord.getOptional("de_primary_heat_other") else None,
      deSecondaryHeatFuelUsed = secondaryHeatUsed,
      deSecondaryHeatFuel = secondaryHeat,
      deSecondaryHeatFuelOtherDescription = if (secondaryHeat.contains(98L)) rawRecord.getOptional("de_secondary_heat_other") else None,
      dePrimaryStoveFuel = primaryStove,
      dePrimaryStoveFuelOtherDescription = if (primaryStove.contains(98L)) rawRecord.getOptional("de_primary_stove_other") else None,
      deSecondaryStoveFuelUsed = secondaryStoveUsed,
      deSecondaryStoveFuel = secondaryStove,
      deSecondaryStoveFuelOtherDescription = if (secondaryStove.contains(98L)) rawRecord.getOptional("de_secondary_stove_other") else None
    )
  }

  /**
    * Parse drinking-water-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapDrinkingWater(rawRecord: RawRecord, dog: HlesDogResidentialEnvironment): HlesDogResidentialEnvironment = {
    val source = rawRecord.getOptionalNumber("de_water_source")
    val knownPipes = rawRecord.getOptionalBoolean("de_pipe_yn")
    val pipeType = knownPipes match {
      case Some(false) => Some(99L) // unknown type
      case Some(true) => rawRecord.getOptionalNumber("de_pipe_type")
      case None => None
    }
    dog.copy(
      deDrinkingWaterSource = source,
      deDrinkingWaterSourceOtherDescription = if (source.contains(98L)) rawRecord.getOptional("de_water_source_other") else None,
      deDrinkingWaterIsFiltered = rawRecord.getOptionalNumber("de_water_filter_yn"),
      dePipeType = pipeType,
      dePipeTypeOtherDescription = if (pipeType.contains(98L)) rawRecord.getOptional("de_pipe_type_other") else None
    )
  }

  /**
    * Parse toxin-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapToxinExposure(rawRecord: RawRecord, dog: HlesDogResidentialEnvironment): HlesDogResidentialEnvironment = {
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
      deWoodFireplaceLightingsPerWeek = if (woodFireplace.contains(1L)) rawRecord.getOptionalNumber("de_wood_fireplace_lit") else None,
      deGasFireplaceLightingsPerWeek = if (gasFireplace.contains(1L)) rawRecord.getOptionalNumber("de_gas_fireplace_lit") else None,
    )
  }

  /* Flooring */
  //deFloorTypesWood,
  //deFloorFrequencyOnWood,
  //deFloorTypesCarpet,
  //deFloorFrequencyOnCarpet,
  //deFloorTypesConcrete,
  //deFloorFrequencyOnConcrete,
  //deFloorTypesTile,
  //deFloorFrequencyOnTile,
  //deFloorTypesLinoleum,
  //deFloorFrequencyOnLinoleum,
  //deFloorTypesLaminate,
  //deFloorFrequencyOnLaminate,
  //deFloorTypesOther,
  //deFloorTypesOtherDescription,
  //deFloorFrequencyOnOther,
  //deStairsInHome,
  //deStairsAvgFlightsPerDay,

  /* Outdoor Property */
  //dePropertyArea,
  //dePropertyAccessible,
  //dePropertyAreaAccessible,
  //dePropertyContainmentType,
  //dePropertyContainmentTypeOtherDescription,
  //dePropertyDrinkingWaterBowl,
  //dePropertyDrinkingWaterHose,
  //dePropertyDrinkingWaterPuddles,
  //dePropertyDrinkingWaterUnknown,
  //dePropertyDrinkingWaterOther,
  //dePropertyDrinkingWaterOtherDescription,
  //dePropertyWeedControlFrequency,
  //dePropertyPestControlFrequency,

  /* Neighborhood */
  //deTrafficNoiseInHomeFrequency,
  //deTrafficNoiseInPropertyFrequency,
  //deNeighborhoodHasSidewalks,
  //deNeighborhoodHasParks,
  //deInteractsWithNeighborhoodAnimals,
  //deInteractsWithNeighborhoodAnimalsWithoutOwner,
  //deInteractsWithNeighborhoodHumans,
  //deInteractsWithNeighborhoodHumansWithoutOwner,
}
