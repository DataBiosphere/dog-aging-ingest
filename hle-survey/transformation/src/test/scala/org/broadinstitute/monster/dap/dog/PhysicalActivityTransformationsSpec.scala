package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PhysicalActivityTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "PhysicalActivityTransformations"

  it should "correctly handle null hour and minute values" in {}

  it should "map high level fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "pa_lifestyle" -> Array("3"),
      "pa_active_hours" -> Array("1"),
      "pa_active_minutes" -> Array("40"),
      "pa_intensity" -> Array("3"),
      "pa_play_yn" -> Array("3"),
      "pa_aerobic_freq" -> Array("3"),
      "pa_aerobic_hours" -> Array("2"),
      "pa_aerobic_minutes" -> Array("30"),
      "pa_walk_aerobic_level" -> Array("3")
    )
    val output =
      PhysicalActivityTransformations.mapPhysicalActivity(RawRecord(id = 1, exampleDogFields))

    output.paActivityLevel shouldBe Some(3)
    output.paAvgDailyActiveMinutes shouldBe Some(100)
    output.paAvgActivityIntensity shouldBe Some(3)
    output.paPhysicalGamesFrequency shouldBe Some(3)
    output.paOtherAerobicActivityFrequency shouldBe Some(3)
    output.paOtherAerobicActivityAvgMinutes shouldBe Some(150)
    output.paOtherAerobicActivityAvgIntensity shouldBe Some(3)
  }

  it should "map weather/surface-related fields" in {
//    val exampleDogFields = Map[String, Array[String]](
//      "" -> Array("")
//    )
//    val output =
//      PhysicalActivityTransformations.mapPhysicalActivity(RawRecord(id = 1, exampleDogFields))

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

  it should "map walking-related fields" in {
//    val exampleDogFields = Map[String, Array[String]](
//      "" -> Array("")
//    )
//    val output =
//      PhysicalActivityTransformations.mapPhysicalActivity(RawRecord(id = 1, exampleDogFields))

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

  it should "map swimming-related fields" in {
//    val exampleDogFields = Map[String, Array[String]](
//      "" -> Array("")
//    )
//    val output =
//      PhysicalActivityTransformations.mapPhysicalActivity(RawRecord(id = 1, exampleDogFields))

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
