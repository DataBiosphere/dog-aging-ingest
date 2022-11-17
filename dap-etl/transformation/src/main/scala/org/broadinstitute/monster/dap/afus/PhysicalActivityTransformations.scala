package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.AfusDogPhysicalActivity

object PhysicalActivityTransformations {

  def mapPhysicalActivities(rawRecord: RawRecord): AfusDogPhysicalActivity = {
    val init = AfusDogPhysicalActivity.init()

    val transformations = List(
      mapWeather _,
      mapWalks _,
      mapSwims _,
      mapOtherActivities _
    )

    transformations.foldLeft(init)((acc, f) => f(rawRecord, acc))
  }

  def mapWeather(rawRecord: RawRecord, dog: AfusDogPhysicalActivity): AfusDogPhysicalActivity = {
    // moderate weather
    val moderateMonths = rawRecord.getOptionalNumber("fu_pa_warm_months")
    val moderateDaily =
      if (moderateMonths.exists(_ > 0)) rawRecord.getOptionalNumber("fu_pa_warm_outdoors") else None
    val hasModerateWeather = moderateDaily.exists(_ != 5L)
    val moderateWeatherOtherSurface =
      if (hasModerateWeather) rawRecord.getOptionalBoolean("fu_pa_w_other_yn") else None

    // hot weather
    val hotMonths = rawRecord.getOptionalNumber("fu_pa_hot_months")
    val hotDaily =
      if (hotMonths.exists(_ > 0)) rawRecord.getOptionalNumber("fu_pa_hot_outdoors") else None
    val hasHotWeather = hotDaily.exists(_ != 5L)
    val hotWeatherOtherSurface =
      if (hasHotWeather) rawRecord.getOptionalBoolean("fu_pa_h_other_yn") else None

    // cold weather
    val coldMonths = rawRecord.getOptionalNumber("fu_pa_cold_months")
    val coldDaily =
      if (coldMonths.exists(_ > 0))
        rawRecord.getOptionalNumber("fu_pa_cold_outdoors")
      else None
    val hasColdWeather = coldDaily.exists(_ != 5L)
    val coldWeatherOtherSurface =
      if (hasColdWeather) rawRecord.getOptionalBoolean("fu_pa_c_other_yn") else None

    dog.copy(
      afusPaActivityLevel = rawRecord.getOptionalNumber("fu_pa_lifestyle"),
      afusPaAvgDailyActiveHours = rawRecord.getOptionalNumber("fu_pa_active_hours"),
      afusPaAvgDailyActiveMinutes = rawRecord.getOptionalNumber("fu_pa_active_minutes"),
      afusPaAvgIntensity = rawRecord.getOptionalNumber("fu_pa_intensity"),
      // moderate weather
      afusPaModerateWeatherMonthsPerYear = moderateMonths,
      afusPaModerateWeatherDailyHoursOutside = moderateDaily,
      afusPaModerateWeatherConcrete =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("fu_pa_w_concrete") else None,
      afusPaModerateWeatherWood =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("fu_pa_w_wood") else None,
      afusPaModerateWeatherOtherHardSurface =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("fu_pa_w_other_hard") else None,
      afusPaModerateWeatherGrassDirt =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("fu_pa_w_grass_dirt") else None,
      afusPaModerateWeatherGravel =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("fu_pa_w_gravel") else None,
      afusPaModerateWeatherSand =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("fu_pa_w_sand") else None,
      afusPaModerateWeatherArtificialTurf =
        if (hasModerateWeather) rawRecord.getOptionalBoolean("fu_pa_w_astro") else None,
      afusPaModerateWeatherOtherSurface = moderateWeatherOtherSurface,
      afusPaModerateWeatherOtherSurfaceDescription =
        if (moderateWeatherOtherSurface.getOrElse(false))
          rawRecord.getOptionalStripped("fu_pa_w_other")
        else None,
      afusPaModerateWeatherSunExposureLevel =
        if (hasModerateWeather) rawRecord.getOptionalNumber("fu_pa_w_sun") else None,
      // hot weather
      afusPaHotWeatherMonthsPerYear = hotMonths,
      afusPaHotWeatherDailyHoursOutside = hotDaily,
      afusPaHotWeatherConcrete =
        if (hasHotWeather) rawRecord.getOptionalBoolean("fu_pa_h_concrete") else None,
      afusPaHotWeatherWood =
        if (hasHotWeather) rawRecord.getOptionalBoolean("fu_pa_h_wood") else None,
      afusPaHotWeatherOtherHardSurface =
        if (hasHotWeather) rawRecord.getOptionalBoolean("fu_pa_h_other_hard") else None,
      afusPaHotWeatherGrassDirt =
        if (hasHotWeather) rawRecord.getOptionalBoolean("fu_pa_h_grass_dirt") else None,
      afusPaHotWeatherGravel =
        if (hasHotWeather) rawRecord.getOptionalBoolean("fu_pa_h_gravel") else None,
      afusPaHotWeatherSand =
        if (hasHotWeather) rawRecord.getOptionalBoolean("fu_pa_h_sand") else None,
      afusPaHotWeatherArtificialTurf =
        if (hasHotWeather) rawRecord.getOptionalBoolean("fu_pa_h_astro") else None,
      afusPaHotWeatherOtherSurface = hotWeatherOtherSurface,
      afusPaHotWeatherOtherSurfaceDescription =
        if (hotWeatherOtherSurface.getOrElse(false)) rawRecord.getOptionalStripped("fu_pa_h_other")
        else None,
      afusPaHotWeatherSunExposureLevel =
        if (hasHotWeather) rawRecord.getOptionalNumber("fu_pa_h_sun") else None,
      // cold weather
      afusPaColdWeatherMonthsPerYear = coldMonths,
      afusPaColdWeatherDailyHoursOutside = coldDaily,
      afusPaColdWeatherConcrete =
        if (hasColdWeather) rawRecord.getOptionalBoolean("fu_pa_c_concrete") else None,
      afusPaColdWeatherWood =
        if (hasColdWeather) rawRecord.getOptionalBoolean("fu_pa_c_wood") else None,
      afusPaColdWeatherOtherHardSurface =
        if (hasColdWeather) rawRecord.getOptionalBoolean("fu_pa_c_other_hard") else None,
      afusPaColdWeatherGrassDirt =
        if (hasColdWeather) rawRecord.getOptionalBoolean("fu_pa_c_grass_dirt") else None,
      afusPaColdWeatherGravel =
        if (hasColdWeather) rawRecord.getOptionalBoolean("fu_pa_c_gravel") else None,
      afusPaColdWeatherSand =
        if (hasColdWeather) rawRecord.getOptionalBoolean("fu_pa_c_sand") else None,
      afusPaColdWeatherArtificialTurf =
        if (hasColdWeather) rawRecord.getOptionalBoolean("fu_pa_c_astro") else None,
      afusPaColdWeatherOtherSurface = coldWeatherOtherSurface,
      afusPaColdWeatherOtherSurfaceDescription =
        if (coldWeatherOtherSurface.getOrElse(false)) rawRecord.getOptionalStripped("fu_pa_c_other")
        else None,
      afusPaColdWeatherSunExposureLevel =
        if (hasColdWeather) rawRecord.getOptionalNumber("fu_pa_c_sun") else None
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

  def mapWalks(rawRecord: RawRecord, dog: AfusDogPhysicalActivity): AfusDogPhysicalActivity = {
    val leashType = rawRecord.getOptionalNumber("fu_pa_walk_how")
    val includesOnLeash = leashType.exists(l => l == 1L || l == 3L)
    val includesOffLeash = leashType.exists(l => l == 2L || l == 3L)
    val dogWithBasicLeashInfo = dog.copy(afusPaOnLeashOffLeashWalk = leashType)

    val dogWithOnLeashInfo = if (includesOnLeash) {
      val paceTypes = rawRecord.getArray("fu_pa_walk_leash_pace")
      val walkReasons = rawRecord.get("fu_pa_walk_leash_why")
      val otherWalkReason = walkReasons.map(_.contains("98"))

      dogWithBasicLeashInfo.copy(
        afusPaOnLeashWalkFrequency = rawRecord.getOptionalNumber("fu_pa_walk_leash_freq"),
        afusPaOnLeashWalkAvgHours = rawRecord.getOptionalNumber("fu_pa_walk_leash_hours"),
        afusPaOnLeashWalkAvgMinutes = rawRecord.getOptionalNumber("fu_pa_walk_leash_minutes"),
        afusPaOnLeashWalkSlowPacePct =
          transformPace(paceTypes, "1", rawRecord.getOptionalNumber("fu_pa_walk_leash_pace_slow")),
        afusPaOnLeashWalkAveragePacePct =
          transformPace(paceTypes, "2", rawRecord.getOptionalNumber("fu_pa_walk_leash_pace_avg")),
        afusPaOnLeashWalkBriskPacePct =
          transformPace(paceTypes, "3", rawRecord.getOptionalNumber("fu_pa_walk_leash_pace_brisk")),
        afusPaOnLeashWalkJogPacePct =
          transformPace(paceTypes, "4", rawRecord.getOptionalNumber("fu_pa_walk_leash_pace_jog")),
        afusPaOnLeashWalkRunPacePct =
          transformPace(paceTypes, "5", rawRecord.getOptionalNumber("fu_pa_walk_leash_pace_run")),
        afusPaOnLeashWalkReasonsDogRelieveItself = walkReasons.map(_.contains("1")),
        afusPaOnLeashWalkReasonsActivityAndEnjoyment = walkReasons.map(_.contains("2")),
        afusPaOnLeashWalkReasonsExerciseForDog = walkReasons.map(_.contains("3")),
        afusPaOnLeashWalkReasonsExerciseForOwner = walkReasons.map(_.contains("4")),
        afusPaOnLeashWalkReasonsTrainingObedience = walkReasons.map(_.contains("5")),
        afusPaOnLeashWalkReasonsOther = otherWalkReason,
        afusPaOnLeashWalkReasonsOtherDescription =
          if (otherWalkReason.contains(true))
            rawRecord.getOptionalStripped("fu_pa_walk_leash_why_other")
          else None
      )
    } else dogWithBasicLeashInfo

    if (includesOffLeash) {
      val paceTypes = rawRecord.getArray("fu_pa_walk_unleash_pace")
      val walkReasons = rawRecord.get("fu_pa_walk_unleash_why")
      val otherWalkReason = walkReasons.map(_.contains("98"))

      dogWithOnLeashInfo.copy(
        afusPaOffLeashWalkFrequency = rawRecord.getOptionalNumber("fu_pa_walk_unleash_freq"),
        afusPaOffLeashWalkAvgHours = rawRecord.getOptionalNumber("fu_pa_walk_unleash_hours"),
        afusPaOffLeashWalkAvgMinutes = rawRecord.getOptionalNumber("fu_pa_walk_unleash_minutes"),
        afusPaOffLeashWalkSlowPacePct =
          transformPace(paceTypes, "1", rawRecord.getOptionalNumber("fu_pa_walk_unleash_pace_slow")),
        afusPaOffLeashWalkAveragePacePct =
          transformPace(paceTypes, "2", rawRecord.getOptionalNumber("fu_pa_walk_unleash_pace_avg")),
        afusPaOffLeashWalkBriskPacePct =
          transformPace(paceTypes, "3", rawRecord.getOptionalNumber("fu_pa_walk_unleash_pace_brisk")),
        afusPaOffLeashWalkJogPacePct =
          transformPace(paceTypes, "4", rawRecord.getOptionalNumber("fu_pa_walk_unleash_pace_jog")),
        afusPaOffLeashWalkRunPacePct =
          transformPace(paceTypes, "5", rawRecord.getOptionalNumber("fu_pa_walk_unleash_pace_run")),
        afusPaOffLeashWalkReasonsDogRelieveItself = walkReasons.map(_.contains("1")),
        afusPaOffLeashWalkReasonsActivityAndEnjoyment = walkReasons.map(_.contains("2")),
        afusPaOffLeashWalkReasonsExerciseForDog = walkReasons.map(_.contains("3")),
        afusPaOffLeashWalkReasonsExerciseForOwner = walkReasons.map(_.contains("4")),
        afusPaOffLeashWalkReasonsTrainingObedience = walkReasons.map(_.contains("5")),
        afusPaOffLeashWalkReasonsOther = otherWalkReason,
        afusPaOffLeashWalkReasonsOtherDescription =
          if (otherWalkReason.contains(true))
            rawRecord.getOptionalStripped("fu_pa_walk_unleash_why_other")
          else None,
        afusPaOffLeashWalkInEnclosedArea =
          rawRecord.getOptionalBoolean("fu_pa_walk_unleash_contain_yn"),
        afusPaOffLeashWalkInOpenArea = rawRecord.getOptionalBoolean("fu_pa_walk_unleash_open"),
        afusPaOffLeashWalkReturnsWhenCalledFrequency =
          rawRecord.getOptionalNumber("fu_pa_walk_unleash_voice_yn")
      )
    } else dogWithOnLeashInfo
  }

  def mapSwims(rawRecord: RawRecord, dog: AfusDogPhysicalActivity): AfusDogPhysicalActivity = {
    rawRecord.getOptionalBoolean("fu_pa_swim_yn").fold(dog) { swims =>
      if (swims) {
        val swimLocations = rawRecord.get("fu_pa_swim_location")
        val otherSwimLocation = swimLocations.map(_.contains("98"))
        dog.copy(
          afusPaSwim = Some(swims),
          afusPaSwimModerateWeatherFrequency = rawRecord.getOptionalNumber("fu_pa_swim_warm_freq"),
          afusPaSwimHotWeatherFrequency = rawRecord.getOptionalNumber("fu_pa_swim_hot_freq"),
          afusPaSwimColdWeatherFrequency = rawRecord.getOptionalNumber("fu_pa_swim_cold_freq"),
          afusPaSwimLocationSwimmingPool = swimLocations.map(_.contains("1")),
          afusPaSwimLocationPondLake = swimLocations.map(_.contains("2")),
          afusPaSwimLocationRiverStreamCreek = swimLocations.map(_.contains("3")),
          afusPaSwimLocationAgriculturalDitch = swimLocations.map(_.contains("4")),
          afusPaSwimLocationOcean = swimLocations.map(_.contains("5")),
          afusPaSwimLocationOther = otherSwimLocation,
          afusPaSwimLocationsOtherDescription =
            if (otherSwimLocation.contains(true))
              rawRecord.getOptionalStripped("fu_pa_swim_location_other")
            else None
        )
      } else {
        dog.copy(afusPaSwim = Some(swims))
      }
    }
  }

  def mapOtherActivities(
    rawRecord: RawRecord,
    dog: AfusDogPhysicalActivity
  ): AfusDogPhysicalActivity = {
    dog.copy(
      afusPaPhysicalGamesFrequency = rawRecord.getOptionalNumber("fu_pa_play_yn"),
      afusPaOtherAerobicActivityFrequency = rawRecord.getOptionalNumber("fu_pa_aerobic_freq"),
      afusPaOtherAerobicActivityAvgHours = rawRecord.getOptionalNumber("fu_pa_aerobic_hours"),
      afusPaOtherAerobicActivityAvgMinutes = rawRecord.getOptionalNumber("fu_pa_aerobic_minutes"),
      afusPaOtherAerobicActivityAvgIntensity =
        rawRecord.getOptionalNumber("fu_pa_walk_aerobic_level")
    )
  }
}
