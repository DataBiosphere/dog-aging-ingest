package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ResidentialEnvironmentTransformationsSpec
    extends AnyFlatSpec
    with Matchers
    with OptionValues {
  behavior of "ResidentialEnvironmentTransformations"

  it should "map all past-residence-related fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_home_nbr" -> Array("10"),
      "de_zip_nbr" -> Array("10"),
      "oc_address2_yn" -> Array("1"),
      "de_country_nbr" -> Array("11"),
      "de_country_01_only" -> Array("this should be ignored too"),
      "de_country_01" -> Array("country1"),
      "de_country_02_dd" -> Array("country2"),
      "de_country_03_dd" -> Array("country3"),
      "de_country_04" -> Array("country4"),
      "de_country_05" -> Array("country5"),
      "de_country_06" -> Array("country6"),
      "de_country_07" -> Array("country7"),
      "de_country_08_dd" -> Array("country8"),
      "de_country_09" -> Array("country9"),
      "de_country_10_dd" -> Array("country10")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.deLifetimeResidenceCount.value shouldBe 12
    output.dePastResidenceZipCount.value shouldBe 10
    output.dePastResidenceCountryCount.value shouldBe 11
    output.dePastResidenceCountry1Text.value shouldBe "country1"
    output.dePastResidenceCountry2.value shouldBe "country2"
    output.dePastResidenceCountry3.value shouldBe "country3"
    output.dePastResidenceCountry4Text.value shouldBe "country4"
    output.dePastResidenceCountry5Text.value shouldBe "country5"
    output.dePastResidenceCountry6Text.value shouldBe "country6"
    output.dePastResidenceCountry7Text.value shouldBe "country7"
    output.dePastResidenceCountry8.value shouldBe "country8"
    output.dePastResidenceCountry9Text.value shouldBe "country9"
    output.dePastResidenceCountry10.value shouldBe "country10"
  }

  it should "map past-residence-related fields where there is a single past and current home" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_home_nbr" -> Array("1"),
      "de_zip_nbr" -> Array("1"),
      "oc_address2_yn" -> Array("0"),
      "de_country_nbr" -> Array("2"),
      "de_country_01_only" -> Array("this should be ignored"),
      "de_country_01" -> Array("USA!"),
      "de_country_02" -> Array("IgnoredCountry")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.deLifetimeResidenceCount.value shouldBe 2
    output.dePastResidenceZipCount.value shouldBe 1
    output.dePastResidenceCountryCount.value shouldBe 2
    output.dePastResidenceCountry1Text.value shouldBe "USA!"
    output.dePastResidenceCountry2 shouldBe None
  }

  it should "map past-residence-related fields where there is a single past and current home (dropdown)" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_home_nbr" -> Array("1"),
      "de_zip_nbr" -> Array("1"),
      "oc_address2_yn" -> Array("0"),
      "de_country_nbr" -> Array("2"),
      "de_country_01_only_dd" -> Array("US"),
      "de_country_01" -> Array("this should be ignored"),
      "de_country_02" -> Array("IgnoredCountry")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.deLifetimeResidenceCount.value shouldBe 2
    output.dePastResidenceZipCount.value shouldBe 1
    output.dePastResidenceCountryCount.value shouldBe 2
    output.dePastResidenceCountry1.value shouldBe "US"
    output.dePastResidenceCountry2 shouldBe None
  }

  it should "map all home-related fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_type_area" -> Array("2000"),
      "de_type_home" -> Array("2"),
      "de_type_home_other" -> Array("something else"),
      "de_home_age" -> Array("75"),
      "de_home_lived_years" -> Array("10"),
      "de_home_area" -> Array("1000")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.deHomeAreaType.value shouldBe 2000
    output.deHomeType.value shouldBe 2
    output.deHomeTypeOtherDescription.value shouldBe "something else"
    output.deHomeConstructionDecade.value shouldBe 75
    output.deHomeYearsLivedIn.value shouldBe 10
    output.deHomeSquareFootage.value shouldBe 1000
  }

  it should "handle floating points in home area" in {
    val floatingPointDogField = Map[String, Array[String]](
      "de_home_area" -> Array("1000.34")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, floatingPointDogField)
      )
    output.deHomeSquareFootage.value shouldBe 1000L
  }

  it should "map all heating-related fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_primary_heat" -> Array("98"),
      "de_primary_heat_other" -> Array("a large campfire"),
      "de_secondary_heat_yn" -> Array("1"),
      "de_secondary_heat" -> Array("98"),
      "de_secondary_heat_other" -> Array("a small campfire"),
      "de_primary_stove" -> Array("98"),
      "de_primary_stove_other" -> Array("a moderately sized campfire"),
      "de_secondary_stove_yn" -> Array("1"),
      "de_secondary_stove" -> Array("98"),
      "de_secondary_stove_other" -> Array("a grill")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.dePrimaryHeatFuel.value shouldBe 98L
    output.dePrimaryHeatFuelOtherDescription.value shouldBe "a large campfire"
    output.deSecondaryHeatFuelUsed.value shouldBe 1L
    output.deSecondaryHeatFuel.value shouldBe 98L
    output.deSecondaryHeatFuelOtherDescription.value shouldBe "a small campfire"
    output.dePrimaryStoveFuel.value shouldBe 98L
    output.dePrimaryStoveFuelOtherDescription.value shouldBe "a moderately sized campfire"
    output.deSecondaryStoveFuelUsed.value shouldBe 1L
    output.deSecondaryStoveFuel.value shouldBe 98L
    output.deSecondaryStoveFuelOtherDescription.value shouldBe "a grill"
  }

  it should "map heating-related fields where there is no secondary heat or stove and no 'other' values selected" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_primary_heat" -> Array("1"),
      "de_primary_heat_other" -> Array("should be ignored"),
      "de_secondary_heat_yn" -> Array("0"),
      "de_secondary_heat" -> Array("98"),
      "de_secondary_heat_other" -> Array("should be ignored"),
      "de_primary_stove" -> Array("3"),
      "de_primary_stove_other" -> Array("should be ignored"),
      "de_secondary_stove_yn" -> Array("0"),
      "de_secondary_stove" -> Array("98"),
      "de_secondary_stove_other" -> Array("should be ignored")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.dePrimaryHeatFuel.value shouldBe 1L
    output.dePrimaryHeatFuelOtherDescription shouldBe None
    output.deSecondaryHeatFuelUsed.value shouldBe 0L
    output.deSecondaryHeatFuel shouldBe None
    output.deSecondaryHeatFuelOtherDescription shouldBe None
    output.dePrimaryStoveFuel.value shouldBe 3L
    output.dePrimaryStoveFuelOtherDescription shouldBe None
    output.deSecondaryStoveFuelUsed.value shouldBe 0L
    output.deSecondaryStoveFuel shouldBe None
    output.deSecondaryStoveFuelOtherDescription shouldBe None
  }

  it should "map all drinking-water-related fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_water_source" -> Array("98"),
      "de_water_source_other" -> Array("puddles"),
      "de_water_filter_yn" -> Array("1"),
      "de_pipe_yn" -> Array("1"),
      "de_pipe_type" -> Array("98"),
      "de_pipe_type_other" -> Array("ceramic")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.deDrinkingWaterSource.value shouldBe 98L
    output.deDrinkingWaterSourceOtherDescription.value shouldBe "puddles"
    output.deDrinkingWaterIsFiltered.value shouldBe 1L
    output.dePipeType.value shouldBe 98L
    output.dePipeTypeOtherDescription.value shouldBe "ceramic"
  }

  it should "map drinking-water-related fields where pipe type is unknown" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_water_source" -> Array("3"),
      "de_water_source_other" -> Array("ignore me"),
      "de_water_filter_yn" -> Array("0"),
      "de_pipe_yn" -> Array("0"),
      "de_pipe_type" -> Array("4"),
      "de_pipe_type_other" -> Array("ignore me")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.deDrinkingWaterSource.value shouldBe 3L
    output.deDrinkingWaterSourceOtherDescription shouldBe None
    output.deDrinkingWaterIsFiltered.value shouldBe 0L
    output.dePipeType.value shouldBe 99L
    output.dePipeTypeOtherDescription shouldBe None
  }

  it should "map all toxin-related fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_2nd_hand_smoke_amt" -> Array("1"),
      "de_central_ac" -> Array("0"),
      "de_room_ac" -> Array("99"),
      "de_central_heat" -> Array("1"),
      "de_asbestos" -> Array("0"),
      "de_radon" -> Array("99"),
      "de_lead" -> Array("1"),
      "de_mothball" -> Array("0"),
      "de_incense" -> Array("99"),
      "de_air_freshener" -> Array("1"),
      "de_air_cleaner" -> Array("0"),
      "de_hepa" -> Array("99"),
      "de_wood_burning" -> Array("1"),
      "de_gas_fireplace" -> Array("1"),
      "de_wood_fireplace_lit" -> Array("5"),
      "de_gas_fireplace_lit" -> Array("6")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.deSecondHandSmokeHoursPerDay.value shouldBe 1L
    output.deCentralAirConditioningPresent.value shouldBe 0L
    output.deRoomOrWindowAirConditioningPresent.value shouldBe 99L
    output.deCentralHeatPresent.value shouldBe 1L
    output.deAsbestosPresent.value shouldBe 0L
    output.deRadonPresent.value shouldBe 99L
    output.deLeadPresent.value shouldBe 1L
    output.deMothballPresent.value shouldBe 0L
    output.deIncensePresent.value shouldBe 99L
    output.deAirFreshenerPresent.value shouldBe 1L
    output.deAirCleanerPresent.value shouldBe 0L
    output.deHepaPresent.value shouldBe 99L
    output.deWoodFireplacePresent.value shouldBe 1L
    output.deGasFireplacePresent.value shouldBe 1L
    output.deWoodFireplaceLightingsPerWeek.value shouldBe 5L
    output.deGasFireplaceLightingsPerWeek.value shouldBe 6L
  }

  it should "map all flooring-related fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_floor_wood_yn" -> Array("1"),
      "de_floor_wood_freq" -> Array("0"),
      "de_floor_carpet_yn" -> Array("1"),
      "de_floor_carpet_freq" -> Array("1"),
      "de_floor_concrete_yn" -> Array("1"),
      "de_floor_concrete_freq" -> Array("2"),
      "de_floor_tile_yn" -> Array("1"),
      "de_floor_tile_freq" -> Array("3"),
      "de_floor_linoleum_yn" -> Array("1"),
      "de_floor_linoleum_freq" -> Array("0"),
      "de_floor_laminate_yn" -> Array("1"),
      "de_floor_laminate_freq" -> Array("1"),
      "de_floor_other_yn" -> Array("1"),
      "de_floor_other" -> Array("pool noodles"),
      "de_floor_other_freq" -> Array("2"),
      "de_stairs" -> Array("1"),
      "de_stairs_nbr" -> Array("3")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.deFloorTypesWood.value shouldBe true
    output.deFloorFrequencyOnWood.value shouldBe 0L
    output.deFloorTypesCarpet.value shouldBe true
    output.deFloorFrequencyOnCarpet.value shouldBe 1L
    output.deFloorTypesConcrete.value shouldBe true
    output.deFloorFrequencyOnConcrete.value shouldBe 2L
    output.deFloorTypesTile.value shouldBe true
    output.deFloorFrequencyOnTile.value shouldBe 3L
    output.deFloorTypesLinoleum.value shouldBe true
    output.deFloorFrequencyOnLinoleum.value shouldBe 0L
    output.deFloorTypesLaminate.value shouldBe true
    output.deFloorFrequencyOnLaminate.value shouldBe 1L
    output.deFloorTypesOther.value shouldBe true
    output.deFloorTypesOtherDescription.value shouldBe "pool noodles"
    output.deFloorFrequencyOnOther.value shouldBe 2L
    output.deStairsInHome.value shouldBe true
    output.deStairsAvgFlightsPerDay.value shouldBe 3L
  }

  it should "map flooring-related fields where no flooring types are selected" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_floor_wood_yn" -> Array("0"),
      "de_floor_wood_freq" -> Array("0"),
      "de_floor_carpet_yn" -> Array("0"),
      "de_floor_carpet_freq" -> Array("0"),
      "de_floor_concrete_yn" -> Array("0"),
      "de_floor_concrete_freq" -> Array("0"),
      "de_floor_tile_yn" -> Array("0"),
      "de_floor_tile_freq" -> Array("0"),
      "de_floor_linoleum_yn" -> Array("0"),
      "de_floor_linoleum_freq" -> Array("0"),
      "de_floor_laminate_yn" -> Array("0"),
      "de_floor_laminate_freq" -> Array("0"),
      "de_floor_other_yn" -> Array("0"),
      "de_floor_other" -> Array("should be ignored"),
      "de_floor_other_freq" -> Array("0"),
      "de_stairs" -> Array("0"),
      "de_stairs_nbr" -> Array("0")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.deFloorTypesWood.value shouldBe false
    output.deFloorFrequencyOnWood shouldBe None
    output.deFloorTypesCarpet.value shouldBe false
    output.deFloorFrequencyOnCarpet shouldBe None
    output.deFloorTypesConcrete.value shouldBe false
    output.deFloorFrequencyOnConcrete shouldBe None
    output.deFloorTypesTile.value shouldBe false
    output.deFloorFrequencyOnTile shouldBe None
    output.deFloorTypesLinoleum.value shouldBe false
    output.deFloorFrequencyOnLinoleum shouldBe None
    output.deFloorTypesLaminate.value shouldBe false
    output.deFloorFrequencyOnLaminate shouldBe None
    output.deFloorTypesOther.value shouldBe false
    output.deFloorTypesOtherDescription shouldBe None
    output.deFloorFrequencyOnOther shouldBe None
    output.deStairsInHome.value shouldBe false
    output.deStairsAvgFlightsPerDay shouldBe None
  }

  it should "map all property-related fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_property_size" -> Array("2"),
      "de_property_access_yn" -> Array("1"),
      "de_property_access" -> Array("2"),
      "de_property_fence_yn" -> Array("4"),
      "de_property_fence_other" -> Array("a different fence"),
      "de_outside_water" -> Array("1", "3", "98"),
      "de_outside_water_other" -> Array("a river"),
      "de_yard_weed_ctl_yn" -> Array("1"),
      "de_yard_weed_ctl_freq" -> Array("5"),
      "de_yard_pest_ctl_yn" -> Array("1"),
      "de_yard_pest_ctl_freq" -> Array("6")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.dePropertyArea.value shouldBe 2L
    output.dePropertyAccessible.value shouldBe true
    output.dePropertyAreaAccessible.value shouldBe 2L
    output.dePropertyContainmentType.value shouldBe 4L
    output.dePropertyContainmentTypeOtherDescription.value shouldBe "a different fence"
    output.dePropertyDrinkingWaterNone.value shouldBe false
    output.dePropertyDrinkingWaterBowl.value shouldBe true
    output.dePropertyDrinkingWaterHose.value shouldBe false
    output.dePropertyDrinkingWaterPuddles.value shouldBe true
    output.dePropertyDrinkingWaterUnknown.value shouldBe false
    output.dePropertyDrinkingWaterOther.value shouldBe true
    output.dePropertyDrinkingWaterOtherDescription.value shouldBe "a river"
    output.dePropertyWeedControlFrequency.value shouldBe 5L
    output.dePropertyPestControlFrequency.value shouldBe 6L
  }

  it should "map property-related fields where there are no access, no weeds, and no pests" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_property_size" -> Array("3"),
      "de_property_access_yn" -> Array("0"),
      "de_property_access" -> Array("2"),
      "de_property_fence_yn" -> Array("4"),
      "de_property_fence_other" -> Array("ignore this"),
      "de_outside_water" -> Array("0", "2", "99"),
      "de_outside_water_other" -> Array("ignore this"),
      "de_yard_weed_ctl_yn" -> Array("0"),
      "de_yard_weed_ctl_freq" -> Array("5"),
      "de_yard_pest_ctl_yn" -> Array("0"),
      "de_yard_pest_ctl_freq" -> Array("6")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.dePropertyArea.value shouldBe 3L
    output.dePropertyAccessible.value shouldBe false
    output.dePropertyAreaAccessible shouldBe None
    output.dePropertyContainmentType shouldBe None
    output.dePropertyContainmentTypeOtherDescription shouldBe None
    output.dePropertyDrinkingWaterNone.value shouldBe true
    output.dePropertyDrinkingWaterBowl.value shouldBe false
    output.dePropertyDrinkingWaterHose.value shouldBe true
    output.dePropertyDrinkingWaterPuddles.value shouldBe false
    output.dePropertyDrinkingWaterUnknown.value shouldBe true
    output.dePropertyDrinkingWaterOther.value shouldBe false
    output.dePropertyDrinkingWaterOtherDescription shouldBe None
    output.dePropertyWeedControlFrequency.value shouldBe 0
    output.dePropertyPestControlFrequency.value shouldBe 0
  }

  it should "map all neighborhood-related fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_traffic_noise_house" -> Array("1"),
      "de_traffic_noise_yard" -> Array("2"),
      "de_sidewalk_yn" -> Array("3"),
      "de_parks_near" -> Array("0"),
      "de_animal_interact" -> Array("1"),
      "de_animal_interact_present" -> Array("0"),
      "de_human_interact" -> Array("1"),
      "de_human_interact_present" -> Array("1")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.deTrafficNoiseInHomeFrequency.value shouldBe 1L
    output.deTrafficNoiseInPropertyFrequency.value shouldBe 2L
    output.deNeighborhoodHasSidewalks.value shouldBe 3L
    output.deNeighborhoodHasParks.value shouldBe false
    output.deInteractsWithNeighborhoodAnimals.value shouldBe true
    output.deInteractsWithNeighborhoodAnimalsWithOwner.value shouldBe false
    output.deInteractsWithNeighborhoodHumans.value shouldBe true
    output.deInteractsWithNeighborhoodHumansWithOwner.value shouldBe true
  }

  it should "map neighborhood-related fields where there are no neighborhood animal interactions" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_animal_interact" -> Array("0"),
      "de_animal_interact_present" -> Array("1"),
      "de_human_interact" -> Array("0"),
      "de_human_interact_present" -> Array("0")
    )
    val missingExpectedExampleDogFields = Map[String, Array[String]](
      "de_animal_interact" -> Array("0"),
      "de_human_interact" -> Array("0")
    )
    val filledOutput =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )
    val blankedOutput =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, missingExpectedExampleDogFields)
      )

    Seq(filledOutput, blankedOutput).foreach(output => {
      output.deInteractsWithNeighborhoodAnimals.value shouldBe false
      output.deInteractsWithNeighborhoodAnimalsWithOwner shouldBe None
      output.deInteractsWithNeighborhoodHumans.value shouldBe false
      output.deInteractsWithNeighborhoodHumansWithOwner shouldBe None
    })
  }
}
