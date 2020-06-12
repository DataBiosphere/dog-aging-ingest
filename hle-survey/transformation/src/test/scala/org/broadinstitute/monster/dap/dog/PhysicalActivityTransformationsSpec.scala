package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PhysicalActivityTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "PhysicalActivityTransformations"

  it should "map all high level fields" in {
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
    output.paAvgDailyActiveHours shouldBe Some(1)
    output.paAvgDailyActiveMinutes shouldBe Some(40)
    output.paAvgActivityIntensity shouldBe Some(3)
    output.paPhysicalGamesFrequency shouldBe Some(3)
    output.paOtherAerobicActivityFrequency shouldBe Some(3)
    output.paOtherAerobicActivityAvgHours shouldBe Some(2)
    output.paOtherAerobicActivityAvgMinutes shouldBe Some(30)
    output.paOtherAerobicActivityAvgIntensity shouldBe Some(3)
  }

  it should "map weather/surface-related fields when all fields are used" in {
    val exampleDogFields = Map[String, Array[String]](
      // Moderate
      "pa_warm_months" -> Array("2"),
      "pa_warm_outdoors" -> Array("2"),
      "pa_w_concrete" -> Array("1"),
      "pa_w_wood" -> Array("0"),
      "pa_w_other_hard" -> Array("1"),
      "pa_w_grass_dirt" -> Array("0"),
      "pa_w_gravel" -> Array("1"),
      "pa_w_sand" -> Array("0"),
      "pa_w_astro" -> Array("1"),
      "pa_w_other_yn" -> Array("1"),
      "pa_w_other" -> Array("some other thing"),
      "pa_w_sun" -> Array("2"),
      // Hot
      "pa_hot_months" -> Array("3"),
      "pa_hot_outdoors" -> Array("3"),
      "pa_h_concrete" -> Array("0"),
      "pa_h_wood" -> Array("1"),
      "pa_h_other_hard" -> Array("0"),
      "pa_h_grass_dirt" -> Array("1"),
      "pa_h_gravel" -> Array("0"),
      "pa_h_sand" -> Array("1"),
      "pa_h_astro" -> Array("0"),
      "pa_h_other_yn" -> Array("1"),
      "pa_h_other" -> Array("another thing"),
      "pa_h_sun" -> Array("3"),
      // Cold
      "pa_cold_months" -> Array("4"),
      "pa_cold_outdoors" -> Array("4"),
      "pa_c_concrete" -> Array("1"),
      "pa_c_wood" -> Array("0"),
      "pa_c_other_hard" -> Array("1"),
      "pa_c_grass_dirt" -> Array("0"),
      "pa_c_gravel" -> Array("1"),
      "pa_c_sand" -> Array("0"),
      "pa_c_astro" -> Array("1"),
      "pa_c_other_yn" -> Array("1"),
      "pa_c_other" -> Array("a different thing"),
      "pa_c_sun" -> Array("4")
    )
    val output =
      PhysicalActivityTransformations.mapPhysicalActivity(RawRecord(id = 1, exampleDogFields))

    // Moderate
    output.paModerateWeatherMonthsPerYear shouldBe Some(2)
    output.paModerateWeatherDailyHoursOutside shouldBe Some(2)
    output.paModerateWeatherOutdoorConcrete shouldBe Some(true)
    output.paModerateWeatherOutdoorWood shouldBe Some(false)
    output.paModerateWeatherOutdoorOtherHardSurface shouldBe Some(true)
    output.paModerateWeatherOutdoorGrassOrDirt shouldBe Some(false)
    output.paModerateWeatherOutdoorGravel shouldBe Some(true)
    output.paModerateWeatherOutdoorSand shouldBe Some(false)
    output.paModerateWeatherOutdoorAstroturf shouldBe Some(true)
    output.paModerateWeatherOutdoorOtherSurface shouldBe Some(true)
    output.paModerateWeatherOutdoorOtherSurfaceDescription shouldBe Some("some other thing")
    output.paModerateWeatherSunExposureLevel shouldBe Some(2)
    // Hot
    output.paHotWeatherMonthsPerYear shouldBe Some(3)
    output.paHotWeatherDailyHoursOutside shouldBe Some(3)
    output.paHotWeatherOutdoorConcrete shouldBe Some(false)
    output.paHotWeatherOutdoorWood shouldBe Some(true)
    output.paHotWeatherOutdoorOtherHardSurface shouldBe Some(false)
    output.paHotWeatherOutdoorGrassOrDirt shouldBe Some(true)
    output.paHotWeatherOutdoorGravel shouldBe Some(false)
    output.paHotWeatherOutdoorSand shouldBe Some(true)
    output.paHotWeatherOutdoorAstroturf shouldBe Some(false)
    output.paHotWeatherOutdoorOtherSurface shouldBe Some(true)
    output.paHotWeatherOutdoorOtherSurfaceDescription shouldBe Some("another thing")
    output.paHotWeatherSunExposureLevel shouldBe Some(3)
    // Cold
    output.paColdWeatherMonthsPerYear shouldBe Some(4)
    output.paColdWeatherDailyHoursOutside shouldBe Some(4)
    output.paColdWeatherOutdoorConcrete shouldBe Some(true)
    output.paColdWeatherOutdoorWood shouldBe Some(false)
    output.paColdWeatherOutdoorOtherHardSurface shouldBe Some(true)
    output.paColdWeatherOutdoorGrassOrDirt shouldBe Some(false)
    output.paColdWeatherOutdoorGravel shouldBe Some(true)
    output.paColdWeatherOutdoorSand shouldBe Some(false)
    output.paColdWeatherOutdoorAstroturf shouldBe Some(true)
    output.paColdWeatherOutdoorOtherSurface shouldBe Some(true)
    output.paColdWeatherOutdoorOtherSurfaceDescription shouldBe Some("a different thing")
    output.paColdWeatherSunExposureLevel shouldBe Some(4)
  }

  it should "map weather/surface-related fields when none of the climates are applicable" in {
    val exampleDogFields = Map[String, Array[String]](
      "pa_warm_months" -> Array("0"),
      "pa_hot_months" -> Array("0"),
      "pa_cold_months" -> Array("0"),
      // these other fields should be ignored
      "pa_warm_outdoors" -> Array("2"),
      "pa_hot_outdoors" -> Array("2"),
      "pa_cold_outdoors" -> Array("2"),
      "pa_w_other_hard" -> Array("1"),
      "pa_h_grass_dirt" -> Array("1"),
      "pa_c_wood" -> Array("1")
    )
    val output =
      PhysicalActivityTransformations.mapPhysicalActivity(RawRecord(id = 1, exampleDogFields))

    // month values should all be 0
    output.paModerateWeatherMonthsPerYear shouldBe Some(0)
    output.paHotWeatherMonthsPerYear shouldBe Some(0)
    output.paColdWeatherMonthsPerYear shouldBe Some(0)
    // other values should be None
    output.paModerateWeatherDailyHoursOutside shouldBe None
    output.paHotWeatherDailyHoursOutside shouldBe None
    output.paColdWeatherDailyHoursOutside shouldBe None
    output.paModerateWeatherOutdoorOtherHardSurface shouldBe None
    output.paHotWeatherOutdoorGrassOrDirt shouldBe None
    output.paColdWeatherOutdoorWood shouldBe None
  }

  it should "map weather/surface-related fields when the dog spends no time outside" in {
    val exampleDogFields = Map[String, Array[String]](
      "pa_warm_months" -> Array("1"),
      "pa_hot_months" -> Array("1"),
      "pa_cold_months" -> Array("1"),
      "pa_warm_outdoors" -> Array("5"),
      "pa_hot_outdoors" -> Array("5"),
      "pa_cold_outdoors" -> Array("5"),
      // these other fields should be ignored
      "pa_w_grass_dirt" -> Array("1"),
      "pa_h_wood" -> Array("1"),
      "pa_c_other_hard" -> Array("1")
    )
    val output =
      PhysicalActivityTransformations.mapPhysicalActivity(RawRecord(id = 1, exampleDogFields))

    output.paModerateWeatherDailyHoursOutside shouldBe Some(5)
    output.paHotWeatherDailyHoursOutside shouldBe Some(5)
    output.paColdWeatherDailyHoursOutside shouldBe Some(5)
    // other values should be None
    output.paModerateWeatherOutdoorGrassOrDirt shouldBe None
    output.paHotWeatherOutdoorWood shouldBe None
    output.paColdWeatherOutdoorOtherHardSurface shouldBe None
  }

  it should "map walking-related fields when all fields are used" in {
    val exampleDogFields = Map[String, Array[String]](
      "pa_walk_how" -> Array("3"), // both on and off leash
      // on leash
      "pa_walk_leash_freq" -> Array("1"),
      "pa_walk_leash_hours" -> Array("2"),
      "pa_walk_leash_minutes" -> Array("30"),
      "pa_walk_leash_pace" -> Array("1", "2", "3", "4", "5"),
      "pa_walk_leash_pace_slow" -> Array("10"),
      "pa_walk_leash_pace_avg" -> Array("20"),
      "pa_walk_leash_pace_brisk" -> Array("30"),
      "pa_walk_leash_pace_jog" -> Array("20"),
      "pa_walk_leash_pace_run" -> Array("20"),
      "pa_walk_leash_why" -> Array("1", "3", "5", "98"),
      "pa_walk_leash_why_other" -> Array("some other reason"),
      // off leash
      "pa_walk_unleash_freq" -> Array("2"),
      "pa_walk_unleash_hours" -> Array("1"),
      "pa_walk_unleash_minutes" -> Array("40"),
      "pa_walk_unleash_pace" -> Array("1", "2", "3", "4", "5"),
      "pa_walk_unleash_pace_slow" -> Array("10"),
      "pa_walk_unleash_pace_avg" -> Array("30"),
      "pa_walk_unleash_pace_brisk" -> Array("20"),
      "pa_walk_unleash_pace_jog" -> Array("30"),
      "pa_walk_unleash_pace_run" -> Array("10"),
      "pa_walk_unleash_why" -> Array("2", "4", "98"),
      "pa_walk_unleash_why_other" -> Array("another reason"),
      "pa_walk_unleash_contain_yn" -> Array("1"),
      "pa_walk_unleash_open" -> Array("1"),
      "pa_walk_unleash_voice_yn" -> Array("3") // not actually a y/n question
    )
    val output =
      PhysicalActivityTransformations.mapPhysicalActivity(RawRecord(id = 1, exampleDogFields))

    output.paOnLeashOffLeashWalk shouldBe Some(3)
    // on leash
    output.paOnLeashWalkFrequency shouldBe Some(1)
    output.paOnLeashWalkAvgHours shouldBe Some(2)
    output.paOnLeashWalkAvgMinutes shouldBe Some(30)
    output.paOnLeashWalkSlowPacePct shouldBe Some(0.1)
    output.paOnLeashWalkAveragePacePct shouldBe Some(0.2)
    output.paOnLeashWalkBriskPacePct shouldBe Some(0.3)
    output.paOnLeashWalkJogPacePct shouldBe Some(0.2)
    output.paOnLeashWalkRunPacePct shouldBe Some(0.2)
    output.paOnLeashWalkReasonsDogRelieveItself shouldBe Some(true)
    output.paOnLeashWalkReasonsActivityAndEnjoyment shouldBe Some(false)
    output.paOnLeashWalkReasonsExerciseForDog shouldBe Some(true)
    output.paOnLeashWalkReasonsExerciseForOwner shouldBe Some(false)
    output.paOnLeashWalkReasonsTrainingObedience shouldBe Some(true)
    output.paOnLeashWalkReasonsOther shouldBe Some(true)
    output.paOnLeashWalkReasonsOtherDescription shouldBe Some("some other reason")
    // off leash
    output.paOffLeashWalkFrequency shouldBe Some(2)
    output.paOffLeashWalkAvgHours shouldBe Some(1)
    output.paOffLeashWalkAvgMinutes shouldBe Some(40)
    output.paOffLeashWalkSlowPacePct shouldBe Some(0.1)
    output.paOffLeashWalkAveragePacePct shouldBe Some(0.3)
    output.paOffLeashWalkBriskPacePct shouldBe Some(0.2)
    output.paOffLeashWalkJogPacePct shouldBe Some(0.3)
    output.paOffLeashWalkRunPacePct shouldBe Some(0.1)
    output.paOffLeashWalkReasonsDogRelieveItself shouldBe Some(false)
    output.paOffLeashWalkReasonsActivityAndEnjoyment shouldBe Some(true)
    output.paOffLeashWalkReasonsExerciseForDog shouldBe Some(false)
    output.paOffLeashWalkReasonsExerciseForOwner shouldBe Some(true)
    output.paOffLeashWalkReasonsTrainingObedience shouldBe Some(false)
    output.paOffLeashWalkReasonsOther shouldBe Some(true)
    output.paOffLeashWalkReasonsOtherDescription shouldBe Some("another reason")
    output.paOffLeashWalkInEnclosedArea shouldBe Some(true)
    output.paOffLeashWalkInOpenArea shouldBe Some(true)
    output.paOffLeashWalkReturnsWhenCalledFrequency shouldBe Some(3)
  }

  it should "map walking-related fields when single pace chosen" in {
    val exampleDogFields = Map[String, Array[String]](
      "pa_walk_how" -> Array("3"), // both on and off leash
      "pa_walk_leash_pace" -> Array("2"), // only avg pace
      "pa_walk_unleash_pace" -> Array("3"), // only brisk pace
      // should be ignored
      "pa_walk_leash_pace_slow" -> Array("10"),
      "pa_walk_leash_pace_avg" -> Array("20"),
      "pa_walk_unleash_pace_avg" -> Array("30"),
      "pa_walk_unleash_pace_brisk" -> Array("40")
    )
    val output =
      PhysicalActivityTransformations.mapPhysicalActivity(RawRecord(id = 1, exampleDogFields))

    output.paOnLeashOffLeashWalk shouldBe Some(3)
    output.paOnLeashWalkSlowPacePct shouldBe Some(0.0)
    output.paOnLeashWalkAveragePacePct shouldBe Some(1.0)
    output.paOffLeashWalkAveragePacePct shouldBe Some(0.0)
    output.paOffLeashWalkBriskPacePct shouldBe Some(1.0)

  }

  it should "map walking-related fields when dog does not walk" in {
    val exampleDogFields = Map[String, Array[String]](
      "pa_walk_how" -> Array("4"), // no walks
      "pa_walk_leash_freq" -> Array("1"),
      "pa_walk_unleash_freq" -> Array("2")
    )
    val output =
      PhysicalActivityTransformations.mapPhysicalActivity(RawRecord(id = 1, exampleDogFields))

    output.paOnLeashOffLeashWalk shouldBe Some(4)
    output.paOnLeashWalkFrequency shouldBe None
    output.paOffLeashWalkFrequency shouldBe None
  }

  it should "map swimming-related fields when all fields are used" in {
    val exampleDogFields = Map[String, Array[String]](
      "pa_swim_yn" -> Array("1"),
      "pa_swim_warm_freq" -> Array("1"),
      "pa_swim_hot_freq" -> Array("2"),
      "pa_swim_cold_freq" -> Array("3"),
      "pa_swim_location" -> Array("1", "3", "5", "98"),
      "pa_swim_location_other" -> Array("something else")
    )
    val output =
      PhysicalActivityTransformations.mapPhysicalActivity(RawRecord(id = 1, exampleDogFields))

    output.paSwim shouldBe Some(true)
    output.paSwimModerateWeatherFrequency shouldBe Some(1)
    output.paSwimHotWeatherFrequency shouldBe Some(2)
    output.paSwimColdWeatherFrequency shouldBe Some(3)
    output.paSwimLocationsSwimmingPool shouldBe Some(true)
    output.paSwimLocationsPondOrLake shouldBe Some(false)
    output.paSwimLocationsRiverStreamOrCreek shouldBe Some(true)
    output.paSwimLocationsAgriculturalDitch shouldBe Some(false)
    output.paSwimLocationsOcean shouldBe Some(true)
    output.paSwimLocationsOther shouldBe Some(true)
    output.paSwimLocationsOtherDescription shouldBe Some("something else")
  }

  it should "map swimming-related fields when the dog does not swim" in {
    val exampleDogFields = Map[String, Array[String]](
      "pa_swim_yn" -> Array("0"),
      // everything below should be ignored
      "pa_swim_warm_freq" -> Array("1"),
      "pa_swim_hot_freq" -> Array("2"),
      "pa_swim_cold_freq" -> Array("3"),
      "pa_swim_location" -> Array("98"),
      "pa_swim_location_other" -> Array("some other swimming spot")
    )
    val output =
      PhysicalActivityTransformations.mapPhysicalActivity(RawRecord(id = 1, exampleDogFields))

    output.paSwim shouldBe Some(false)
    output.paSwimModerateWeatherFrequency shouldBe None
    output.paSwimHotWeatherFrequency shouldBe None
    output.paSwimColdWeatherFrequency shouldBe None
    output.paSwimLocationsOther shouldBe None
    output.paSwimLocationsOtherDescription shouldBe None
  }
}
