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
  def mapHighLevelFields(
    rawRecord: RawRecord,
    dog: HlesDogPhysicalActivity
  ): HlesDogPhysicalActivity =
    dog.copy()

  /* HighLevelFields */
  //paActivityLevel,
  //paAvgDailyActiveMinutes,
  //paAvgActivityIntensity,
  //paPhysicalGamesFrequency,
  //paOtherAerobicActivityFrequency,
  //paOtherAerobicActivityAvgMinutes,
  //paOtherAerobicActivityAvgIntensity

  /**
    * Parse weather and outdoor surface fields out of a raw RedCap record,
    * injecting the data into a partially-modeled dog record.
    */
  def mapWeather(rawRecord: RawRecord, dog: HlesDogPhysicalActivity): HlesDogPhysicalActivity =
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

  def transformPace(
    selectedPaceTypes: Option[Array[String]],
    paceType: String,
    pacePercent: Option[Long]
  ): Option[Double] =
    if (selectedPaceTypes.isEmpty) None
    else if (!selectedPaceTypes.contains(paceType)) Some(0.0) // pace was not selected
    else if (selectedPaceTypes.size == 1) Some(1.0) // pace was only value selected
    else pacePercent.map(_ / 100) // pace was one of multiple values selected

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
      val walkHours = rawRecord.getOptionalNumber("pa_walk_leash_hours")
      val walkMinutes = rawRecord.getOptionalNumber("pa_walk_leash_minutes")
      val paceTypes = rawRecord.get("pa_walk_leash_pace")
      val walkReasons = rawRecord.get("pa_walk_leash_why")
      val otherWalkReason = walkReasons.map(_.contains("98"))

      dogWithBasicLeashInfo.copy(
        paOnLeashWalkFrequency = rawRecord.getOptionalNumber("pa_walk_leash_freq"),
        paOnLeashWalkAvgMinutes =
          if (walkHours.isEmpty || walkMinutes.isEmpty) None
          else Some(walkHours.head * 60 + walkMinutes.head),
        paOnLeashWalkSlowPacePct =
          transformPace(paceTypes, "1", rawRecord.getOptionalNumber("pa_walk_leash_pace_slow")),
        paOnLeashWalkAveragePacePct =
          transformPace(paceTypes, "2", rawRecord.getOptionalNumber("pa_walk_leash_pace_average")),
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
          if (otherWalkReason.contains(true)) rawRecord.getOptional("pa_walk_leash_why_other")
          else None
      )
    } else dogWithBasicLeashInfo

    if (includesOffLeash) {
      val walkHours = rawRecord.getOptionalNumber("pa_walk_unleash_hours")
      val walkMinutes = rawRecord.getOptionalNumber("pa_walk_unleash_minutes")
      val paceTypes = rawRecord.get("pa_walk_unleash_pace")
      val walkReasons = rawRecord.get("pa_walk_unleash_why")
      val otherWalkReason = walkReasons.map(_.contains("98"))

      dogWithOnLeashInfo.copy(
        paOffLeashWalkFrequency = rawRecord.getOptionalNumber("pa_walk_unleash_freq"),
        paOffLeashWalkAvgMinutes =
          if (walkHours.isEmpty || walkMinutes.isEmpty) None
          else Some(walkHours.head * 60 + walkMinutes.head),
        paOffLeashWalkSlowPacePct =
          transformPace(paceTypes, "1", rawRecord.getOptionalNumber("pa_walk_unleash_pace_slow")),
        paOffLeashWalkAveragePacePct = transformPace(
          paceTypes,
          "2",
          rawRecord.getOptionalNumber("pa_walk_unleash_pace_average")
        ),
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
          if (otherWalkReason.contains(true)) rawRecord.getOptional("pa_walk_unleash_why_other")
          else None,
        paOffLeashWalkInEnclosedArea = rawRecord.getOptionalBoolean("pa_walk_unleash_contain_yn"),
        paOffLeashWalkInOpenArea = rawRecord.getOptionalBoolean("pa_walk_unleash_open"),
        paOffLeashWalkReturnsWhenCalledFrequency =
          rawRecord.getOptionalNumber("pa_walk_unleash_voice_yn")
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
            if (otherSwimLocation.contains(true)) rawRecord.getOptional("pa_swim_location_other")
            else None
        )
      } else {
        dog.copy(paSwim = Some(swims))
      }
    }
}
