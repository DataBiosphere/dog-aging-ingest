package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogPhysicalActivity

object PhysicalActivityTransformations {

  /**
    * Parse all physical-activity-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapPhysicalActivity(rawRecord: RawRecord): HlesDogPhysicalActivity = {
    val transformations = List(
      mapHighLevelFields _,
      mapWeather _,
      mapWalks _,
      mapSwims _
    )

    transformations.foldLeft(HlesDogPhysicalActivity.init())((acc, f) => f(rawRecord, acc))
  }

  /**
    * Parse all high-level physical activity fields out of a raw RedCap record,
    * injecting the data into a partially-modeled dog record.
    */
  def mapHighLevelFields(rawRecord: RawRecord, dog: HlesDogPhysicalActivity): HlesDogPhysicalActivity = {
    dog.copy()

    /* HighLevelFields */
    //paActivityLevel,
    //paAvgDailyActiveMinutes,
    //paAvgActivityIntensity,
    //paPhysicalGamesFrequency,
    //paOtherAerobicActivityFrequency,
    //paOtherAerobicActivityAvgMinutes,
    //paOtherAerobicActivityAvgIntensity
  }

  /**
    * Parse weather and outdoor surface fields out of a raw RedCap record,
    * injecting the data into a partially-modeled dog record.
    */
  def mapWeather(rawRecord: RawRecord, dog: HlesDogPhysicalActivity): HlesDogPhysicalActivity = {
    dog.copy()

    /* Weather */
    //paModerateWeatherMonthsPerYear,
    //paModerateWeatherDailyHoursOutside,
    //paModerateWeatherOutdoorConcrete,
    //paModerateWeatherOutdoorWood,
    //paModerateWeatherOutdoorOtherHardSurface,
    //paModerateWeatherOutdoorGrassOrDirt,
    //paModerateWeatherOutdoorGravel,
    //paModerateWeatherOutdoorSand,
    //paModerateWeatherOutdoorAstroturf,
    //paModerateWeatherOutdoorOtherSurface,
    //paModerateWeatherOutdoorOtherSurfaceDescription,
    //paModerateWeatherSunExposureLevel,
    //paHotWeatherMonthsPerYear,
    //paHotWeatherDailyHoursOutside,
    //paHotWeatherOutdoorConcrete,
    //paHotWeatherOutdoorWood,
    //paHotWeatherOutdoorOtherHardSurface,
    //paHotWeatherOutdoorGrassOrDirt,
    //paHotWeatherOutdoorGravel,
    //paHotWeatherOutdoorSand,
    //paHotWeatherOutdoorAstroturf,
    //paHotWeatherOutdoorOtherSurface,
    //paHotWeatherOutdoorOtherSurfaceDescription,
    //paHotWeatherSunExposureLevel,
    //paColdWeatherMonthsPerYear,
    //paColdWeatherDailyHoursOutside,
    //paColdWeatherOutdoorConcrete,
    //paColdWeatherOutdoorWood,
    //paColdWeatherOutdoorOtherHardSurface,
    //paColdWeatherOutdoorGrassOrDirt,
    //paColdWeatherOutdoorGravel,
    //paColdWeatherOutdoorSand,
    //paColdWeatherOutdoorAstroturf,
    //paColdWeatherOutdoorOtherSurface,
    //paColdWeatherOutdoorOtherSurfaceDescription,
    //paColdWeatherSunExposureLevel,
  }

  /**
    * Parse walk-related physical activity fields out of a raw RedCap record,
    * injecting the data into a partially-modeled dog record as ***.
    */
  def mapWalks(rawRecord: RawRecord, dog: HlesDogPhysicalActivity): HlesDogPhysicalActivity = {
    dog.copy()

    /* Walks */
    //paOnLeashOffLeashWalk,
    //paOnLeashWalkFrequency,
    //paOnLeashWalkAvgMinutes,
    //paOnLeashWalkSlowPacePct,
    //paOnLeashWalkAveragePacePct,
    //paOnLeashWalkBriskPacePct,
    //paOnLeashWalkJogPacePct,
    //paOnLeashWalkRunPacePct,
    //paOnLeashWalkReasonsDogRelieveItself,
    //paOnLeashWalkReasonsActivityAndEnjoyment,
    //paOnLeashWalkReasonsExerciseForDog,
    //paOnLeashWalkReasonsExerciseForOwner,
    //paOnLeashWalkReasonsTrainingObedience,
    //paOnLeashWalkReasonsOther,
    //paOnLeashWalkReasonsOtherDescription,
    //paOffLeashWalkFrequency,
    //paOffLeashWalkAvgMinutes,
    //paOffLeashWalkSlowPacePct,
    //paOffLeashWalkAveragePacePct,
    //paOffLeashWalkBriskPacePct,
    //paOffLeashWalkJogPacePct,
    //paOffLeashWalkRunPacePct,
    //paOffLeashWalkReasonsDogRelieveItself,
    //paOffLeashWalkReasonsActivityAndEnjoyment,
    //paOffLeashWalkReasonsExerciseForDog,
    //paOffLeashWalkReasonsExerciseForOwner,
    //paOffLeashWalkReasonsTrainingObedience,
    //paOffLeashWalkReasonsOther,
    //paOffLeashWalkReasonsOtherDescription,
    //paOffLeashWalkInEnclosedArea,
    //paOffLeashWalkInOpenArea,
    //paOffLeashWalkReturnsWhenCalledFrequency,
  }

  /**
    * Parse swimming-related physical activity fields out of a raw RedCap record,
    * injecting the data into a partially-modeled dog record.
    */
  def mapSwims(rawRecord: RawRecord, dog: HlesDogPhysicalActivity): HlesDogPhysicalActivity = {
    dog.copy()

    /* Swims */
    //paSwim,
    //paSwimModerateWeatherFrequency,
    //paSwimHotWeatherFrequency,
    //paSwimColdWeatherFrequency,
    //paSwimLocationsSwimmingPool,
    //paSwimLocationsPondOrLake,
    //paSwimLocationsRiverStreamOrCreek,
    //paSwimLocationsAgriculturalDitch,
    //paSwimLocationsOcean,
    //paSwimLocationsOther,
    //paSwimLocationsOtherDescription,
  }
}







