package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogResidentialEnvironment

object ResidentialEnvironmentTransformations {

  /**
    * Parse all residential environment fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapResidentialEnvironment(rawRecord: RawRecord): HlesDogResidentialEnvironment =
    HlesDogResidentialEnvironment.init()

  /* Past Residences */
  //deLifetimeResidenceCount,
  //dePastResidenceZipCount,
  //dePastResidenceZip1,
  //dePastResidenceZip2,
  //dePastResidenceZip3,
  //dePastResidenceZip4,
  //dePastResidenceZip5,
  //dePastResidenceZip6,
  //dePastResidenceZip7,
  //dePastResidenceZip8,
  //dePastResidenceZip9,
  //dePastResidenceZip10,
  //dePastResidenceCountryCount,
  //dePastResidenceCountry1,
  //dePastResidenceCountry2,
  //dePastResidenceCountry3,
  //dePastResidenceCountry4,
  //dePastResidenceCountry5,
  //dePastResidenceCountry6,
  //dePastResidenceCountry7,
  //dePastResidenceCountry8,
  //dePastResidenceCountry9,
  //dePastResidenceCountry10,

  /* Home Info */
  //deHomeAreaType,
  //deHomeType,
  //deHomeTypeOtherDescription,
  //deHomeConstructionDecade,
  //deHomeYearsLivedIn,
  //deHomeSquareFootage,

  /* Heating */
  //dePrimaryHeatFuel,
  //dePrimaryHeatFuelOtherDescription,
  //deSecondaryHeatFuelUsed,
  //deSecondaryHeatFuel,
  //deSecondaryHeatFuelOtherDescription,
  //dePrimaryStoveFuel,
  //dePrimaryStoveFuelOtherDescription,
  //deSecondaryStoveFuelUsed,
  //deSecondaryStoveFuel,
  //deSecondaryStoveFuelOtherDescription,

  /* Drinking Water */
  //deDrinkingWaterSource,
  //deDrinkingWaterSourceOtherDescription,
  //deDrinkingWaterIsFiltered,
  //dePipeType,
  //dePipeTypeOtherDescription,

  /* Toxin Exposure */
  //deSecondHandSmokeHoursPerDay,
  //deCentralAirConditioningPresent,
  //deRoomOrWindowAirConditioningPresent,
  //deCentralHeatPresent,
  //deAsbestosPresent,
  //deRadonPresent,
  //deLeadPresent,
  //deMothballPresent,
  //deIncensePresent,
  //deAirFreshenerPresent,
  //deAirCleanerPresent,
  //deHepaPresent,
  //deWoodFireplacePresent,
  //deGasFireplacePresent,
  //deWoodFireplaceLightingsPerWeek,
  //deGasFireplaceLightingsPerWeek,

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
