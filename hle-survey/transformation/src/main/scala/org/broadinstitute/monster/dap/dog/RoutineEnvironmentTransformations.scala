package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogRoutineEnvironment

object RoutineEnvironmentTransformations {

  /**
    * Parse all routine environment fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapRoutineEnvironment(rawRecord: RawRecord): HlesDogRoutineEnvironment =
    HlesDogRoutineEnvironment.init()

  /* Recreational Spaces */
  //deDogpark,
  //deDogparkDaysPerMonth,
  //deDogparkTravelWalk,
  //deDogparkTravelDrive,
  //deDogparkTravelBike,
  //deDogparkTravelPublicTransportation,
  //deDogparkTravelOther,
  //deDogparkTravelOtherDescription,
  //deDogparkTravelTimeMinutes,
  //deRecreationalSpaces,
  //deRecreationalSpacesTravelWalk,
  //deRecreationalSpacesTravelDrive,
  //deRecreationalSpacesTravelBike,
  //deRecreationalSpacesTravelPublicTransportation,
  //deRecreationalSpacesTravelOther,
  //deRecreationalSpacesTravelOtherDescription,
  //deRecreationalSpacesTravelTimeMinutes,

  /* Work */
  //deWork,
  //deWorkDaysPerMonth,
  //deWorkTravelWalk,
  //deWorkTravelDrive,
  //deWorkTravelBike,
  //deWorkTravelPublicTransportation,
  //deWorkTravelOther,
  //deWorkTravelOtherDescription,
  //deWorkTravelTimeMinutes,

  /* Sitter */
  //deSitterOrDaycare,
  //deSitterOrDaycareDaysPerMonth,
  //deSitterOrDaycareTravelWalk,
  //deSitterOrDaycareTravelDrive,
  //deSitterOrDaycareTravelBike,
  //deSitterOrDaycareTravelPublicTransportation,
  //deSitterOrDaycareTravelOther,
  //deSitterOrDaycareTravelOtherDescription,
  //deSitterOrDaycareTravelTimeMinutes,

  /* Eats Gross Things */
  //deEatsGrassFrequency,
  //deEatsFeces,
  //deEatsFecesOwnFeces,
  //deEatsFecesOtherDog,
  //deEatsFecesCat,
  //deEatsFecesHorse,
  //deEatsFecesCattle,
  //deEatsFecesWildlife,
  //deEatsFecesOther,
  //deEatsFecesOtherDescription,
  //deDrinksOutdoorWater,
  //deDrinksOutdoorWaterFrequency,

  /* Toys */
  //deRoutineToys,
  //deRoutineToysIncludePlastic,
  //deRoutineToysIncludeStuffedFabric,
  //deRoutineToysIncludeUnstuffedFabric,
  //deRoutineToysIncludeRubber,
  //deRoutineToysIncludeMetal,
  //deRoutineToysIncludeAnimalProducts,
  //deRoutineToysIncludeLatex,
  //deRoutineToysIncludeTennisBalls,
  //deRoutineToysIncludeSticks,
  //deRoutineToysIncludeOther,
  //deRoutineToysOtherDescription,
  //deRoutineToysHoursPerDay,
  //deNighttimeSleepLocation,
  //deNighttimeSleepLocationOtherDescription,
  //deNighttimeSleepAvgHours,
  //deDaytimeSleepLocationDifferent,
  //deDaytimeSleepLocation,
  //deDaytimeSleepLocationOtherDescription,
  //deDaytimeSleepAvgHours,
  //deLicksChewsOrPlaysWithNonToys,

  /* Toxins ingested */
  //deRecentToxinsOrHazardsIngestedFrequency,
  //deRecentToxinsOrHazardsIngestedChocolate,
  //deRecentToxinsOrHazardsIngestedPoison,
  //deRecentToxinsOrHazardsIngestedHumanMedication,
  //deRecentToxinsOrHazardsIngestedPetMedication,
  //deRecentToxinsOrHazardsIngestedGarbageOrFood,
  //deRecentToxinsOrHazardsIngestedDeadAnimal,
  //deRecentToxinsOrHazardsIngestedToys,
  //deRecentToxinsOrHazardsIngestedClothing,
  //deRecentToxinsOrHazardsIngestedOther,
  //deRecentToxinsOrHazardsIngestedOtherDescription,
  //deRecentToxinsOrHazardsIngestedRequiredVet,

  /* Other Animals */
  //deOtherPresentAnimals,
  //deOtherPresentAnimalsDogs,
  //deOtherPresentAnimalsCats,
  //deOtherPresentAnimalsBirds,
  //deOtherPresentAnimalsReptiles,
  //deOtherPresentAnimalsLivestock,
  //deOtherPresentAnimalsHorses,
  //deOtherPresentAnimalsRodents,
  //deOtherPresentAnimalsFish,
  //deOtherPresentAnimalsWildlife,
  //deOtherPresentAnimalsOther,
  //deOtherPresentAnimalsOtherDescription,
  //deOtherPresentAnimalsIndoorCount,
  //deOtherPresentAnimalsOutdoorCount,
  //deOtherPresentAnimalsInteractWithDog,

  /* General Routine */
  //deRoutineConsistency,
  //deRoutineHoursPerDayInCrate,
  //deRoutineHoursPerDayRoamingHouse,
  //deRoutineHoursPerDayInGarage,
  //deRoutineHoursPerDayInOutdoorKennel,
  //deRoutineHoursPerDayInYard,
  //deRoutineHoursPerDayRoamingOutside,
  //deRoutineHoursPerDayChainedOutside,
  //deRoutineHoursPerDayAwayFromHome,
  //deRoutineHoursPerDayWithOtherAnimals,
  //deRoutineHoursPerDayWithAdults,
  //deRoutineHoursPerDayWithTeens,
  //deRoutineHoursPerDayWithChildren,
}
