package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.common.RawRecord
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
  def mapHighLevelFields(
    rawRecord: RawRecord,
    dog: HlesDogPhysicalActivity
  ): HlesDogPhysicalActivity =
    dog.copy(
      paActivityLevel = rawRecord.getOptionalNumber("pa_lifestyle"),
      paAvgDailyActiveHours = rawRecord.getOptionalNumber("pa_active_hours"),
      paAvgDailyActiveMinutes = rawRecord.getOptionalNumber("pa_active_minutes"),
      paAvgActivityIntensity = rawRecord.getOptionalNumber("pa_intensity"),
      paPhysicalGamesFrequency = rawRecord.getOptionalNumber("pa_play_yn"),
      paOtherAerobicActivityFrequency = rawRecord.getOptionalNumber("pa_aerobic_freq"),
      paOtherAerobicActivityAvgHours = rawRecord.getOptionalNumber("pa_aerobic_hours"),
      paOtherAerobicActivityAvgMinutes = rawRecord.getOptionalNumber("pa_aerobic_minutes"),
      paOtherAerobicActivityAvgIntensity = rawRecord.getOptionalNumber("pa_walk_aerobic_level")
    )

  /**
    * Parse weather and outdoor surface fields out of a raw RedCap record,
    * injecting the data into a partially-modeled dog record.
    */
  def mapWeather(rawRecord: RawRecord, dog: HlesDogPhysicalActivity): HlesDogPhysicalActivity = {
    // moderate weather
    val moderateMonths = rawRecord.getOptionalNumber("pa_warm_months")
    val moderateDaily =
      if (moderateMonths.exists(_ > 0)) rawRecord.getOptionalNumber("pa_warm_outdoors") else None
    val hasModerateWeather = moderateDaily.exists(_ != 5L)
    val moderateWeatherOtherSurface =
      if (hasModerateWeather) rawRecord.getOptionalBoolean("pa_w_other_yn") else None

    // hot weather
    val hotMonths = rawRecord.getOptionalNumber("pa_hot_months")
    val hotDaily =
      if (hotMonths.exists(_ > 0)) rawRecord.getOptionalNumber("pa_hot_outdoors") else None
    val hasHotWeather = hotDaily.exists(_ != 5L)
    val hotWeatherOtherSurface =
      if (hasHotWeather) rawRecord.getOptionalBoolean("pa_h_other_yn") else None

    // cold weather
    val coldMonths = rawRecord.getOptionalNumber("pa_cold_months")
    val coldDaily =
      if (coldMonths.exists(_ > 0))
        rawRecord.getOptionalNumber("pa_cold_outdoors")
      else None
    val hasColdWeather = coldDaily.exists(_ != 5L)
    val coldWeatherOtherSurface =
      if (hasColdWeather) rawRecord.getOptionalBoolean("pa_c_other_yn") else None

    dog.copy(
      // moderate weather
      paModerateWeatherMonthsPerYear = moderateMonths,
      paModerateWeatherDailyHoursOutside = moderateDaily,
      paModerateWeatherOutdoorConcrete =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("pa_w_concrete") else None,
      paModerateWeatherOutdoorWood =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("pa_w_wood") else None,
      paModerateWeatherOutdoorOtherHardSurface =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("pa_w_other_hard") else None,
      paModerateWeatherOutdoorGrassOrDirt =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("pa_w_grass_dirt") else None,
      paModerateWeatherOutdoorGravel =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("pa_w_gravel") else None,
      paModerateWeatherOutdoorSand =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("pa_w_sand") else None,
      paModerateWeatherOutdoorAstroturf =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("pa_w_astro") else None,
      paModerateWeatherOutdoorOtherSurface = moderateWeatherOtherSurface,
      paModerateWeatherOutdoorOtherSurfaceDescription =
        if (moderateWeatherOtherSurface.getOrElse(false))
          rawRecord.getOptionalStripped("pa_w_other")
        else None,
      paModerateWeatherSunExposureLevel = rawRecord.getOptionalNumber("pa_w_sun"),
      // hot weather
      paHotWeatherMonthsPerYear = hotMonths,
      paHotWeatherDailyHoursOutside = hotDaily,
      paHotWeatherOutdoorConcrete =
        if (hasHotWeather) rawRecord.getOptionalBoolean("pa_h_concrete") else None,
      paHotWeatherOutdoorWood =
        if (hasHotWeather) rawRecord.getOptionalBoolean("pa_h_wood") else None,
      paHotWeatherOutdoorOtherHardSurface =
        if (hasHotWeather) rawRecord.getOptionalBoolean("pa_h_other_hard") else None,
      paHotWeatherOutdoorGrassOrDirt =
        if (hasHotWeather) rawRecord.getOptionalBoolean("pa_h_grass_dirt") else None,
      paHotWeatherOutdoorGravel =
        if (hasHotWeather) rawRecord.getOptionalBoolean("pa_h_gravel") else None,
      paHotWeatherOutdoorSand =
        if (hasHotWeather) rawRecord.getOptionalBoolean("pa_h_sand") else None,
      paHotWeatherOutdoorAstroturf =
        if (hasHotWeather) rawRecord.getOptionalBoolean("pa_h_astro") else None,
      paHotWeatherOutdoorOtherSurface = hotWeatherOtherSurface,
      paHotWeatherOutdoorOtherSurfaceDescription =
        if (hotWeatherOtherSurface.getOrElse(false)) rawRecord.getOptionalStripped("pa_h_other")
        else None,
      paHotWeatherSunExposureLevel = rawRecord.getOptionalNumber("pa_h_sun"),
      // cold weather
      paColdWeatherMonthsPerYear = coldMonths,
      paColdWeatherDailyHoursOutside = coldDaily,
      paColdWeatherOutdoorConcrete =
        if (hasColdWeather) rawRecord.getOptionalBoolean("pa_c_concrete") else None,
      paColdWeatherOutdoorWood =
        if (hasColdWeather) rawRecord.getOptionalBoolean("pa_c_wood") else None,
      paColdWeatherOutdoorOtherHardSurface =
        if (hasColdWeather) rawRecord.getOptionalBoolean("pa_c_other_hard") else None,
      paColdWeatherOutdoorGrassOrDirt =
        if (hasColdWeather) rawRecord.getOptionalBoolean("pa_c_grass_dirt") else None,
      paColdWeatherOutdoorGravel =
        if (hasColdWeather) rawRecord.getOptionalBoolean("pa_c_gravel") else None,
      paColdWeatherOutdoorSand =
        if (hasColdWeather) rawRecord.getOptionalBoolean("pa_c_sand") else None,
      paColdWeatherOutdoorAstroturf =
        if (hasColdWeather) rawRecord.getOptionalBoolean("pa_c_astro") else None,
      paColdWeatherOutdoorOtherSurface = coldWeatherOtherSurface,
      paColdWeatherOutdoorOtherSurfaceDescription =
        if (coldWeatherOtherSurface.getOrElse(false)) rawRecord.getOptionalStripped("pa_c_other")
        else None,
      paColdWeatherSunExposureLevel = rawRecord.getOptionalNumber("pa_c_sun")
    )
  }

  /**
    * Calculate the percentage of time a dog walks at a given pace.
    * @param selectedPaceTypes The set of all paces a dog walks at.
    * @param paceType The pace for which we want to calculate a percent value
    * @param pacePercent The proportion of time a dog walks at the given pace (only defined when the pace was one of multiple values selected).
    * @return A decimal value representing the percentage of time a dog walks at the given pace.
    */
  def transformPace(
    selectedPaceTypes: Array[String],
    paceType: String,
    pacePercent: Option[Long]
  ): Option[Double] =
    if (!selectedPaceTypes.contains(paceType)) Some(0.0) // pace was not selected
    else if (selectedPaceTypes.length == 1) Some(1.0) // pace was only value selected
    else pacePercent.map(_.toDouble / 100) // pace was one of multiple values selected

  /**
    * Parse walk-related physical activity fields out of a raw RedCap record,
    * injecting the data into a partially-modeled dog record.
    */
  def mapWalks(rawRecord: RawRecord, dog: HlesDogPhysicalActivity): HlesDogPhysicalActivity = {
    val leashType = rawRecord.getOptionalNumber("pa_walk_how")
    val includesOnLeash = leashType.exists(l => l == 1L || l == 3L)
    val includesOffLeash = leashType.exists(l => l == 2L || l == 3L)
    val dogWithBasicLeashInfo = dog.copy(paOnLeashOffLeashWalk = leashType)

    val dogWithOnLeashInfo = if (includesOnLeash) {
      val paceTypes = rawRecord.getArray("pa_walk_leash_pace")
      val walkReasons = rawRecord.get("pa_walk_leash_why")
      val otherWalkReason = walkReasons.map(_.contains("98"))

      dogWithBasicLeashInfo.copy(
        paOnLeashWalkFrequency = rawRecord.getOptionalNumber("pa_walk_leash_freq"),
        paOnLeashWalkAvgHours = rawRecord.getOptionalNumber("pa_walk_leash_hours"),
        paOnLeashWalkAvgMinutes = rawRecord.getOptionalNumber("pa_walk_leash_minutes"),
        paOnLeashWalkSlowPacePct =
          transformPace(paceTypes, "1", rawRecord.getOptionalNumber("pa_walk_leash_pace_slow")),
        paOnLeashWalkAveragePacePct =
          transformPace(paceTypes, "2", rawRecord.getOptionalNumber("pa_walk_leash_pace_avg")),
        paOnLeashWalkBriskPacePct =
          transformPace(paceTypes, "3", rawRecord.getOptionalNumber("pa_walk_leash_pace_brisk")),
        paOnLeashWalkJogPacePct =
          transformPace(paceTypes, "4", rawRecord.getOptionalNumber("pa_walk_leash_pace_jog")),
        paOnLeashWalkRunPacePct =
          transformPace(paceTypes, "5", rawRecord.getOptionalNumber("pa_walk_leash_pace_run")),
        paOnLeashWalkReasonsDogRelieveItself = walkReasons.map(_.contains("1")),
        paOnLeashWalkReasonsActivityAndEnjoyment = walkReasons.map(_.contains("2")),
        paOnLeashWalkReasonsExerciseForDog = walkReasons.map(_.contains("3")),
        paOnLeashWalkReasonsExerciseForOwner = walkReasons.map(_.contains("4")),
        paOnLeashWalkReasonsTrainingObedience = walkReasons.map(_.contains("5")),
        paOnLeashWalkReasonsOther = otherWalkReason,
        paOnLeashWalkReasonsOtherDescription =
          if (otherWalkReason.contains(true))
            rawRecord.getOptionalStripped("pa_walk_leash_why_other")
          else None
      )
    } else dogWithBasicLeashInfo

    if (includesOffLeash) {
      val paceTypes = rawRecord.getArray("pa_walk_unleash_pace")
      val walkReasons = rawRecord.get("pa_walk_unleash_why")
      val otherWalkReason = walkReasons.map(_.contains("98"))

      dogWithOnLeashInfo.copy(
        paOffLeashWalkFrequency = rawRecord.getOptionalNumber("pa_walk_unleash_freq"),
        paOffLeashWalkAvgHours = rawRecord.getOptionalNumber("pa_walk_unleash_hours"),
        paOffLeashWalkAvgMinutes = rawRecord.getOptionalNumber("pa_walk_unleash_minutes"),
        paOffLeashWalkSlowPacePct =
          transformPace(paceTypes, "1", rawRecord.getOptionalNumber("pa_walk_unleash_pace_slow")),
        paOffLeashWalkAveragePacePct =
          transformPace(paceTypes, "2", rawRecord.getOptionalNumber("pa_walk_unleash_pace_avg")),
        paOffLeashWalkBriskPacePct =
          transformPace(paceTypes, "3", rawRecord.getOptionalNumber("pa_walk_unleash_pace_brisk")),
        paOffLeashWalkJogPacePct =
          transformPace(paceTypes, "4", rawRecord.getOptionalNumber("pa_walk_unleash_pace_jog")),
        paOffLeashWalkRunPacePct =
          transformPace(paceTypes, "5", rawRecord.getOptionalNumber("pa_walk_unleash_pace_run")),
        paOffLeashWalkReasonsDogRelieveItself = walkReasons.map(_.contains("1")),
        paOffLeashWalkReasonsActivityAndEnjoyment = walkReasons.map(_.contains("2")),
        paOffLeashWalkReasonsExerciseForDog = walkReasons.map(_.contains("3")),
        paOffLeashWalkReasonsExerciseForOwner = walkReasons.map(_.contains("4")),
        paOffLeashWalkReasonsTrainingObedience = walkReasons.map(_.contains("5")),
        paOffLeashWalkReasonsOther = otherWalkReason,
        paOffLeashWalkReasonsOtherDescription =
          if (otherWalkReason.contains(true))
            rawRecord.getOptionalStripped("pa_walk_unleash_why_other")
          else None,
        paOffLeashWalkInEnclosedArea = rawRecord.getOptionalBoolean("pa_walk_unleash_contain_yn"),
        paOffLeashWalkInOpenArea = rawRecord.getOptionalBoolean("pa_walk_unleash_open"),
        paOffLeashWalkReturnsWhenCalledFrequency =
          rawRecord.getOptionalNumber("pa_walk_unleash_voice_yn") // not actually a y/n question
      )
    } else dogWithOnLeashInfo
  }

  /**
    * Parse swimming-related physical activity fields out of a raw RedCap record,
    * injecting the data into a partially-modeled dog record.
    */
  def mapSwims(rawRecord: RawRecord, dog: HlesDogPhysicalActivity): HlesDogPhysicalActivity =
    rawRecord.getOptionalBoolean("pa_swim_yn").fold(dog) { swims =>
      if (swims) {
        val swimLocations = rawRecord.get("pa_swim_location")
        val otherSwimLocation = swimLocations.map(_.contains("98"))
        dog.copy(
          paSwim = Some(swims),
          paSwimModerateWeatherFrequency = rawRecord.getOptionalNumber("pa_swim_warm_freq"),
          paSwimHotWeatherFrequency = rawRecord.getOptionalNumber("pa_swim_hot_freq"),
          paSwimColdWeatherFrequency = rawRecord.getOptionalNumber("pa_swim_cold_freq"),
          paSwimLocationsSwimmingPool = swimLocations.map(_.contains("1")),
          paSwimLocationsPondOrLake = swimLocations.map(_.contains("2")),
          paSwimLocationsRiverStreamOrCreek = swimLocations.map(_.contains("3")),
          paSwimLocationsAgriculturalDitch = swimLocations.map(_.contains("4")),
          paSwimLocationsOcean = swimLocations.map(_.contains("5")),
          paSwimLocationsOther = otherSwimLocation,
          paSwimLocationsOtherDescription =
            if (otherSwimLocation.contains(true))
              rawRecord.getOptionalStripped("pa_swim_location_other")
            else None
        )
      } else {
        dog.copy(paSwim = Some(swims))
      }
    }
}
