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
      "oc_address2_yn" -> Array("1"),
      "de_zip_nbr" -> Array("10"),
      "de_zip_01_only" -> Array("this should be ignored"),
      "de_zip_01" -> Array("11111"),
      "de_zip_02" -> Array("22222"),
      "de_zip_03" -> Array("33333"),
      "de_zip_04" -> Array("44444"),
      "de_zip_05" -> Array("55555"),
      "de_zip_06" -> Array("66666"),
      "de_zip_07" -> Array("77777"),
      "de_zip_08" -> Array("88888"),
      "de_zip_09" -> Array("99999"),
      "de_zip_10" -> Array("10101"),
      "de_country_nbr" -> Array("10"),
      "de_country_01_only" -> Array("this should be ignored too"),
      "de_country_01" -> Array("country1"),
      "de_country_02" -> Array("country2"),
      "de_country_03" -> Array("country3"),
      "de_country_04" -> Array("country4"),
      "de_country_05" -> Array("country5"),
      "de_country_06" -> Array("country6"),
      "de_country_07" -> Array("country7"),
      "de_country_08" -> Array("country8"),
      "de_country_09" -> Array("country9"),
      "de_country_10" -> Array("country10")
    )
    val output =
      ResidentialEnvironmentTransformations.mapResidentialEnvironment(
        RawRecord(id = 1, exampleDogFields)
      )

    output.deLifetimeResidenceCount.value shouldBe 12
    output.dePastResidenceZipCount.value shouldBe 10
    output.dePastResidenceZip1.value shouldBe "11111"
    output.dePastResidenceZip2.value shouldBe "22222"
    output.dePastResidenceZip3.value shouldBe "33333"
    output.dePastResidenceZip4.value shouldBe "44444"
    output.dePastResidenceZip5.value shouldBe "55555"
    output.dePastResidenceZip6.value shouldBe "66666"
    output.dePastResidenceZip7.value shouldBe "77777"
    output.dePastResidenceZip8.value shouldBe "88888"
    output.dePastResidenceZip9.value shouldBe "99999"
    output.dePastResidenceZip10.value shouldBe "10101"
    output.dePastResidenceCountryCount.value shouldBe 10
    output.dePastResidenceCountry1.value shouldBe "country1"
    output.dePastResidenceCountry2.value shouldBe "country2"
    output.dePastResidenceCountry3.value shouldBe "country3"
    output.dePastResidenceCountry4.value shouldBe "country4"
    output.dePastResidenceCountry5.value shouldBe "country5"
    output.dePastResidenceCountry6.value shouldBe "country6"
    output.dePastResidenceCountry7.value shouldBe "country7"
    output.dePastResidenceCountry8.value shouldBe "country8"
    output.dePastResidenceCountry9.value shouldBe "country9"
    output.dePastResidenceCountry10.value shouldBe "country10"
  }

  // TODO Test past residence mapping where one past residence & single current home

  it should "map all home-related fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_type_area" -> Array("2000"),
      "de_type_home" -> Array("2"),
      "he_type_home_other" -> Array("something else"),
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

  // TODO map heating fields with no secondary heat or stove

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

  // TODO map drinking fields where unknown

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

  // TODO test flooring where no flooring types are selected

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
    output.dePropertyDrinkingWaterBowl.value shouldBe true
    output.dePropertyDrinkingWaterHose.value shouldBe false
    output.dePropertyDrinkingWaterPuddles.value shouldBe true
    output.dePropertyDrinkingWaterUnknown.value shouldBe false
    output.dePropertyDrinkingWaterOther.value shouldBe true
    output.dePropertyDrinkingWaterOtherDescription.value shouldBe "a river"
    output.dePropertyWeedControlFrequency.value shouldBe 5L
    output.dePropertyPestControlFrequency.value shouldBe 6L
  }

  // TODO test property where there is no access, no weeds, no pests

  it should "map all neighborhood-related fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "de_traffic_noise_house" -> Array("1"),
      "de_traffic_noise_yard" -> Array("2"),
      "de_sidewalk_yn" -> Array("3"),
      "de_parks_near" -> Array("0"),
      "de_animal_interact" -> Array("1"),
      "de_animal_interact_present" -> Array("1"),
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
    output.deInteractsWithNeighborhoodAnimalsWithoutOwner.value shouldBe false
    output.deInteractsWithNeighborhoodHumans.value shouldBe true
    output.deInteractsWithNeighborhoodHumansWithoutOwner.value shouldBe false
  }

  // TODO test neighborhood where there's no interaction
}
