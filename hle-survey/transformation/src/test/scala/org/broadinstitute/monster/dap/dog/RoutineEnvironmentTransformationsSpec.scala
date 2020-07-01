package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RoutineEnvironmentTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "RoutineEnvironmentTransformations"

  it should "map dogpark fields when available" in {

    // de_dogpark_get_to = ('Walk' , 'Bike' , 'Other')
    val example1 = Map[String, Array[String]](
      "de_dogpark_yn" -> Array("1"),
      "de_dogpark_get_to" -> Array("1", "3", "98"),
      "de_dogpark_freq" -> Array("10"),
      "de_dogpark_get_to_other" -> Array("Helicopter Shuttle"),
      "de_dogpark_hr" -> Array("5"),
      "de_dogpark_min" -> Array("30")
    )

    val output1 = RoutineEnvironmentTransformations.mapRoutineEnvironment(
      RawRecord(id = 1, example1)
    )

    output1.deDogpark.value shouldBe true
    output1.deDogparkDaysPerMonth.value shouldBe 10
    output1.deDogparkTravelWalk.value shouldBe true
    output1.deDogparkTravelDrive.value shouldBe false
    output1.deDogparkTravelBike.value shouldBe true
    output1.deDogparkTravelPublicTransportation.value shouldBe false
    output1.deDogparkTravelOther.value shouldBe true
    output1.deDogparkTravelOtherDescription shouldBe "Helicopter Shuttle"
    output1.deDogparkTravelTimeMinutes.value shouldBe 330

    // de_dogpark_get_to = ('Drive' , 'Public Transport')
    val example2 = Map[String, Array[String]](
      "de_dogpark_yn" -> Array("1"),
      "de_dogpark_get_to" -> Array("2", "4"),
      "de_dogpark_freq" -> Array("15"),
      "de_dogpark_get_to_other" -> Array("Helicopter Shuttle"),
      "de_dogpark_hr" -> Array("10"),
      "de_dogpark_min" -> Array("15")
    )

    val output2 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example2))

    output2.deDogpark.value shouldBe true
    output2.deDogparkDaysPerMonth.value shouldBe 15
    output2.deDogparkTravelWalk.value shouldBe false
    output2.deDogparkTravelDrive.value shouldBe true
    output2.deDogparkTravelBike.value shouldBe false
    output2.deDogparkTravelPublicTransportation.value shouldBe true
    output2.deDogparkTravelOther.value shouldBe false
    output2.deDogparkTravelOtherDescription shouldBe None
    output2.deDogparkTravelTimeMinutes.value shouldBe 615

    // de_dogpark_yn = '0'
    val example3 = Map[String, Array[String]](
      "de_dogpark_yn" -> Array("0"),
      "de_dogpark_get_to" -> Array("1", "3", "98"),
      "de_dogpark_freq" -> Array("10"),
      "de_dogpark_get_to_other" -> Array("Helicopter Shuttle"),
      "de_dogpark_hr" -> Array("5"),
      "de_dogpark_min" -> Array("30")
    )

    val output3 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example3))

    output3.deDogpark.value shouldBe false
    output3.deDogparkDaysPerMonth shouldBe None
    output3.deDogparkTravelWalk shouldBe None
    output3.deDogparkTravelDrive shouldBe None
    output3.deDogparkTravelBike shouldBe None
    output3.deDogparkTravelPublicTransportation shouldBe None
    output3.deDogparkTravelOther shouldBe None
    output3.deDogparkTravelOtherDescription shouldBe None
    output3.deDogparkTravelTimeMinutes shouldBe None
  }


  it should "map recreational spaces when available" in {

    // de_spaces_yn = '1' AND de_spaces_get_to(98) = ("Walk", "Drive", "Bike")
    val example1 = Map[String, Array[String]](
      "de_spaces_yn" -> Array("1"),
      "de_spaces_get_to" -> Array("1", "2", "3"),
      "de_spaces_freq" -> Array("12"),
      "de_spaces_get_to_other" -> Array(""),
      "de_spaces_hr" -> Array("1"),
      "de_spaces_min" -> Array("20")
    )
    val output1 = RoutineEnvironmentTransformations.mapRoutineEnvironment(
      RawRecord(id = 1, example1)
    )
    output1.deRecreationalSpaces.value shouldBe true
    output1.deRecreationalSpacesDaysPerMonth.value shouldBe 12
    output1.deRecreationalSpacesTravelWalk.value shouldBe true
    output1.deRecreationalSpacesTravelDrive.value shouldBe true
    output1.deRecreationalSpacesTravelBike.value shouldBe true
    output1.deRecreationalSpacesTravelPublicTransportation.value shouldBe false
    output1.deRecreationalSpacesTravelOther.value shouldBe false
    output1.deRecreationalSpacesTravelOtherDescription shouldBe None
    output1.deRecreationalSpacesTravelTimeMinutes.value shouldBe 80

    // de_spaces_yn = '1' AND de_spaces_get_to(98) = ("Public Transport", "Other")
    val example2 = Map[String, Array[String]](
      "de_spaces_yn" -> Array("1"),
      "de_spaces_get_to" -> Array("4", "98"),
      "de_spaces_freq" -> Array("5"),
      "de_spaces_get_to_other" -> Array("Jetpack"),
      "de_spaces_hr" -> Array("2"),
      "de_spaces_min" -> Array("30")
    )
    val output2 = RoutineEnvironmentTransformations.mapRoutineEnvironment(
      RawRecord(id = 1, example2)
    )
    output2.deRecreationalSpaces.value shouldBe true
    output2.deRecreationalSpacesDaysPerMonth.value shouldBe 5
    output2.deRecreationalSpacesTravelWalk.value shouldBe false
    output2.deRecreationalSpacesTravelDrive.value shouldBe false
    output2.deRecreationalSpacesTravelBike.value shouldBe false
    output2.deRecreationalSpacesTravelPublicTransportation.value shouldBe true
    output2.deRecreationalSpacesTravelOther.value shouldBe true
    output2.deRecreationalSpacesTravelOtherDescription.value shouldBe "Jetpack"
    output2.deRecreationalSpacesTravelTimeMinutes.value shouldBe 150

    // de_spaces_yn = 0
    val example3 = Map[String, Array[String]](
      "de_spaces_yn" -> Array("0"),
      "de_spaces_get_to" -> Array("1", "2"),
      "de_spaces_freq" -> Array("0"),
      "de_spaces_get_to_other" -> Array("swim"),
      "de_spaces_hr" -> Array("0"),
      "de_spaces_min" -> Array("20")
    )
    val output3 = RoutineEnvironmentTransformations.mapRoutineEnvironment(
      RawRecord(id = 1, example3)
    )
    output3.deRecreationalSpaces.value shouldBe false
    output3.deRecreationalSpacesDaysPerMonth shouldBe None
    output3.deRecreationalSpacesTravelWalk shouldBe None
    output3.deRecreationalSpacesTravelDrive shouldBe None
    output3.deRecreationalSpacesTravelBike shouldBe None
    output3.deRecreationalSpacesTravelPublicTransportation shouldBe None
    output3.deRecreationalSpacesTravelOther shouldBe None
    output3.deRecreationalSpacesTravelOtherDescription shouldBe None
    output3.deRecreationalSpacesTravelTimeMinutes shouldBe None
  }

  it should "map Work fields when available" in {
    // de_dog_to_work = 1, de_dog_to_work_how = "Walk", "Drive", "Bike"
    val example1 = Map[String, Array[String]](
      "de_dog_to_work" -> Array("1"),
      "de_dog_to_work_how" -> Array("1", "2", "3"),
      "de_dog_to_work_freq" -> Array("3"),
      "de_dog_to_work_how_other" -> Array(""),
      "de_dog_to_work_hr" -> Array("1"),
      "de_dog_to_work_min" -> Array("0")
    )
    val output1 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example1))

    output1.deWork.value shouldBe true
    output1.deWorkDaysPerMonth.value shouldBe 3
    output1.deWorkTravelWalk.value shouldBe true
    output1.deWorkTravelDrive.value shouldBe true
    output1.deWorkTravelBike.value shouldBe true
    output1.deWorkTravelPublicTransportation.value shouldBe false
    output1.deWorkTravelOther.value shouldBe false
    output1.deWorkTravelOtherDescription shouldBe None
    output1.deWorkTravelTimeMinutes.value shouldBe 60

    // de_dog_to_work = 1, de_dog_to_work_how = "Public Transportation", "Other"
    val example2 = Map[String, Array[String]](
      "de_dog_to_work" -> Array("1"),
      "de_dog_to_work_how" -> Array("4", "98"),
      "de_dog_to_work_freq" -> Array("5"),
      "de_dog_to_work_how_other" -> Array("Submarine"),
      "de_dog_to_work_hr" -> Array("3"),
      "de_dog_to_work_min" -> Array("10")
    )
    val output2 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example2))

    output2.deWork.value shouldBe true
    output2.deWorkDaysPerMonth.value shouldBe 5
    output2.deWorkTravelWalk.value shouldBe false
    output2.deWorkTravelDrive.value shouldBe false
    output2.deWorkTravelBike.value shouldBe false
    output2.deWorkTravelPublicTransportation.value shouldBe true
    output2.deWorkTravelOther.value shouldBe true
    output2.deWorkTravelOtherDescription.value shouldBe "Submarine"
    output2.deWorkTravelTimeMinutes.value shouldBe 190

    // de_dog_to_work = 0, de_dog_to_work_how = "Other"
    val example3 = Map[String, Array[String]](
      "de_dog_to_work" -> Array("0"),
      "de_dog_to_work_how" -> Array("98"),
      "de_dog_to_work_freq" -> Array("0"),
      "de_dog_to_work_how_other" -> Array("Rocketship"),
      "de_dog_to_work_hr" -> Array("3"),
      "de_dog_to_work_min" -> Array("10")
    )
    val output3 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example3))

    output3.deWork.value shouldBe false
    output3.deWorkDaysPerMonth shouldBe None
    output3.deWorkTravelWalk shouldBe None
    output3.deWorkTravelDrive shouldBe None
    output3.deWorkTravelBike shouldBe None
    output3.deWorkTravelPublicTransportation shouldBe None
    output3.deWorkTravelOther shouldBe None
    output3.deWorkTravelOtherDescription shouldBe None
    output3.deWorkTravelTimeMinutes shouldBe None
  }

  // FIXME how can I separate the different test cases (try different sets of arrays)
  // FIXME how to change the unit test structure to be better organized for that?
  // FIXME ex... should the last case be separated into a diff "it should" block?
  // FIXME I guess I could create much larger examples that span multiple mapping pieces (dogpark + rec spaces + work)
  // FIXME but since I split them into groups that make more sense conceptually, I can unit test them that way anyways
  // FIXME don't see a benefit in unit testing multiple functions ? maybe one large example at the end?

  it should "map Sitter fields when available" in {
    // de_sitter_yn = 1, de_sitter_how = "Walk", "Drive", "Public Transport"
    val example1 = Map[String, Array[String]](
      "de_sitter_yn" -> Array("1"),
      "de_sitter_how" -> Array("1", "2", "4"),
      "de_sitter_freq" -> Array("5"),
      //"de_sitter_how_other" -> Array(""), // FIXME does this even need to be included here?
      "de_sitter_hr" -> Array("4"),
      "de_sitter_min" -> Array("30")
    )

    val output1 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example1))

    output1.deSitterOrDaycare.value shouldBe true
    output1.deSitterOrDaycareDaysPerMonth.value shouldBe 5
    output1.deSitterOrDaycareTravelWalk.value shouldBe true
    output1.deSitterOrDaycareTravelDrive.value shouldBe true
    output1.deSitterOrDaycareTravelBike.value shouldBe false
    output1.deSitterOrDaycareTravelPublicTransportation.value shouldBe true
    output1.deSitterOrDaycareTravelOther.value shouldBe false
    output1.deSitterOrDaycareTravelOtherDescription shouldBe None
    output1.deSitterOrDaycareTravelTimeMinutes.value shouldBe 270

    // de_sitter_yn = 1, de_sitter_how = "Bike", "Other"
    val example2 = Map[String, Array[String]](
      "de_sitter_yn" -> Array("1"),
      "de_sitter_how" -> Array("3", "98"),
      "de_sitter_freq" -> Array("10"),
      "de_sitter_how_other" -> Array("Hot Air Balloon"),
      "de_sitter_hr" -> Array("10"),
      "de_sitter_min" -> Array("10")
    )
    val output2 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example2))

    output2.deSitterOrDaycare.value shouldBe true
    output2.deSitterOrDaycareDaysPerMonth.value shouldBe 10
    output2.deSitterOrDaycareTravelWalk.value shouldBe false
    output2.deSitterOrDaycareTravelDrive.value shouldBe false
    output2.deSitterOrDaycareTravelBike.value shouldBe true
    output2.deSitterOrDaycareTravelPublicTransportation.value shouldBe false
    output2.deSitterOrDaycareTravelOther.value shouldBe true
    output2.deSitterOrDaycareTravelOtherDescription.value shouldBe "Hot Air Balloon"
    output2.deSitterOrDaycareTravelTimeMinutes.value shouldBe 610

    // de_sitter_yn = '0'
    val example3 = Map[String, Array[String]](
      "de_sitter_yn" -> Array("0"),
      //FIXME do i need all these values here cause they shouldn't get processed if the above value is 0?
      //FIXME should i fill them with empty/0?
      "de_sitter_how" -> Array("1"),
      "de_sitter_freq" -> Array("0"),
      "de_sitter_how_other" -> Array(""),
      "de_sitter_hr" -> Array(""),
      "de_sitter_min" -> Array("")
    )

    val output3 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example3))

    output3.deSitterOrDaycare.value shouldBe false
    output3.deSitterOrDaycareDaysPerMonth shouldBe None
    output3.deSitterOrDaycareTravelWalk shouldBe None
    output3.deSitterOrDaycareTravelDrive shouldBe None
    output3.deSitterOrDaycareTravelBike shouldBe None
    output3.deSitterOrDaycareTravelPublicTransportation shouldBe None
    output3.deSitterOrDaycareTravelOther shouldBe None
    output3.deSitterOrDaycareTravelOtherDescription shouldBe None
    output3.deSitterOrDaycareTravelTimeMinutes shouldBe None
  }

  it should "map EatsFeces fields when available" in {
    // de_eat_feces_yn = '1', de_eat_feces_type = "Dog (own)", "Cat", "Cattle", "Other", de_water_outdoor_yn = 1
    val example1 = Map[String, Array[String]](
      "de_eat_grass" -> Array("1"),
      "de_eat_feces_yn" -> Array("1"),
      "de_eat_feces_type" -> Array("1", "3", "5", "98"),
      "de_eat_feces_type_other" -> Array("Human"),
      "de_water_outdoor_yn" -> Array("1"),
      "de_water_outdoor_freq" -> Array("1")
    )
    val output1 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example1))

    output1.deEatsGrassFrequency.value shouldBe 1
    output1.deEatsFeces.value shouldBe 1
    output1.deEatsFecesOwnFeces.value shouldBe true
    output1.deEatsFecesOtherDog.value shouldBe false
    output1.deEatsFecesCat.value shouldBe true
    output1.deEatsFecesHorse.value shouldBe false
    output1.deEatsFecesCattle.value shouldBe true
    output1.deEatsFecesWildlife.value shouldBe false
    output1.deEatsFecesOther.value shouldBe true
    output1.deEatsFecesOtherDescription.value shouldBe "Human"
    output1.deDrinksOutdoorWater.value shouldBe true
    output1.deDrinksOutdoorWaterFrequency.value shouldBe 1

    // de_eat_feces_yn = '1', de_eat_feces_type = "Dog (other)", "Horse", "Wildlife"), de_water_outdoor_yn = 0
    val example2 = Map[String, Array[String]](
      "de_eat_grass" -> Array("0"),
      "de_eat_feces_yn" -> Array("1"),
      "de_eat_feces_type" -> Array("2", "4", "6"),
      //"de_eat_feces_type_other" -> Array(""), //FIXME remove?
      "de_water_outdoor_yn" -> Array("0"),
      //"de_water_outdoor_freq" -> Array("0") //FIXME remove?
    )
    val output2 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example2))

    output2.deEatsGrassFrequency.value shouldBe 0
    output2.deEatsFeces.value shouldBe 1
    output2.deEatsFecesOwnFeces.value shouldBe false
    output2.deEatsFecesOtherDog.value shouldBe true
    output2.deEatsFecesCat.value shouldBe false
    output2.deEatsFecesHorse.value shouldBe true
    output2.deEatsFecesCattle.value shouldBe false
    output2.deEatsFecesWildlife.value shouldBe true
    output2.deEatsFecesOther.value shouldBe false
    output2.deEatsFecesOtherDescription shouldBe None
    output2.deDrinksOutdoorWater.value shouldBe false
    output2.deDrinksOutdoorWaterFrequency shouldBe None

    // de_eat_feces_yn = '0', de_water_outdoor_yn = 1
    val example3 = Map[String, Array[String]](
      "de_eat_grass" -> Array("1"),
      "de_eat_feces_yn" -> Array("0"),
      //"de_eat_feces_type" -> Array("2", "4", "6"),//FIXME remove?
      //"de_eat_feces_type_other" -> Array(""), //FIXME remove?
      "de_water_outdoor_yn" -> Array("1"),
      "de_water_outdoor_freq" -> Array("2")
    )
    val output3 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example3))

    output3.deEatsGrassFrequency.value shouldBe 1
    output3.deEatsFeces.value shouldBe 0
    output3.deEatsFecesOwnFeces shouldBe None
    output3.deEatsFecesOtherDog shouldBe None
    output3.deEatsFecesCat shouldBe None
    output3.deEatsFecesHorse shouldBe None
    output3.deEatsFecesCattle shouldBe None
    output3.deEatsFecesWildlife shouldBe None
    output3.deEatsFecesOther shouldBe None
    output3.deEatsFecesOtherDescription shouldBe None
    output3.deDrinksOutdoorWater.value shouldBe true
    output3.deDrinksOutdoorWaterFrequency.value shouldBe 2

    // de_eat_feces_yn = '0', de_water_outdoor_yn = 0
    val example4 = Map[String, Array[String]](
      "de_eat_grass" -> Array("1"),
      "de_eat_feces_yn" -> Array("0"),
      "de_water_outdoor_yn" -> Array("0")
    )
    val output4 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example4))

    output4.deEatsGrassFrequency.value shouldBe 1
    output4.deEatsFeces.value shouldBe 0
    output4.deEatsFecesOwnFeces shouldBe None
    output4.deEatsFecesOtherDog shouldBe None
    output4.deEatsFecesCat shouldBe None
    output4.deEatsFecesHorse shouldBe None
    output4.deEatsFecesCattle shouldBe None
    output4.deEatsFecesWildlife shouldBe None
    output4.deEatsFecesOther shouldBe None
    output4.deEatsFecesOtherDescription shouldBe None
    output4.deDrinksOutdoorWater.value shouldBe false
    output4.deDrinksOutdoorWaterFrequency shoudlBe None
  }

  //de_toys_yn = 1
  deRoutineToys
  deRoutineToysIncludePlastic
  deRoutineToysIncludeStuffedFabric
  deRoutineToysIncludeUnstuffedFabric
  deRoutineToysIncludeRubber
  deRoutineToysIncludeMetal
  deRoutineToysIncludeAnimalProducts
  deRoutineToysIncludeLatex
  deRoutineToysIncludeRope
  deRoutineToysIncludeTennisBalls
  deRoutineToysIncludeSticks
  deRoutineToysIncludeOther
  //de_toy_other_yn = '1'
  deRoutineToysOtherDescription
  deRoutineToysHoursPerDay
  deLicksChewsOrPlaysWithNonToys
  deRoutineToys
  deLicksChewsOrPlaysWithNonToys

  //de_toys_yn = 0
  deRoutineToys
  deLicksChewsOrPlaysWithNonToys

  //CASES
  // de_toys_yn = '1', de_toy_other_yn = '1',
  // de_toys_yn = '1', de_toy_other_yn = '0'
  // de_toys_yn = 0






  it should "map Toys fields when available" in {
    // de_toys_yn = '1', de_toy_other_yn = '1', de_chew_other_yn = '1'
    val example1 = Map[String, Array[String]](
      "de_toys_yn" -> Array("1"),
      "de_toy_plastic" -> Array("1"),
      "de_toy_stuffed_fabric" -> Array("0"),
      "de_toy_fabric_unstuffed" -> Array("1"),
      "de_toy_rubber" -> Array("0"),
      "de_toy_metal" -> Array("1"),
      "de_toy_animal" -> Array("0"),
      "de_toy_latex" -> Array("1"),
      "de_toy_rope" -> Array("0"),
      "de_toy_tennis_ball" -> Array("1"),
      "de_toy_sticks " -> Array("0"),
      "de_toy_other_yn" -> Array("1"),
      "de_toy_other" -> Array("Soccer Ball"),
      "de_toys_amt" -> Array("3"),
      "de_chew_other_yn" -> Array("1")
    )
    val output1 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example1))

    output1.deRoutineToys.value shouldBe true
    output1.deRoutineToysIncludePlastic.value shouldBe 1
    output1.deRoutineToysIncludeStuffedFabric.value shouldBe 0
    output1.deRoutineToysIncludeUnstuffedFabric.value shouldBe 1
    output1.deRoutineToysIncludeRubber.value shouldBe 0
    output1.deRoutineToysIncludeMetal.value shouldBe 1
    output1.deRoutineToysIncludeAnimalProducts.value shouldBe 0
    output1.deRoutineToysIncludeLatex.value shouldBe 1
    output1.deRoutineToysIncludeRope.value shouldBe 0
    output1.deRoutineToysIncludeTennisBalls.value shouldBe 1
    output1.deRoutineToysIncludeSticks.value shouldBe 0
    output1.deRoutineToysIncludeOther.value shouldBe 1
    output1.deRoutineToysOtherDescription.value shouldBe "Soccer Ball"
    output1.deRoutineToysHoursPerDay.value shouldBe 3
    output1.deLicksChewsOrPlaysWithNonToys.value true

    // de_toys_yn = '1', de_toy_other_yn = '0', de_chew_other_yn = '0'
    val example2 = Map[String, Array[String]](
      "de_toys_yn" -> Array("1"),
      "de_toy_plastic" -> Array("0"),
      "de_toy_stuffed_fabric" -> Array("1"),
      "de_toy_fabric_unstuffed" -> Array("0"),
      "de_toy_rubber" -> Array("1"),
      "de_toy_metal" -> Array("0"),
      "de_toy_animal" -> Array("1"),
      "de_toy_latex" -> Array("0"),
      "de_toy_rope" -> Array("1"),
      "de_toy_tennis_ball" -> Array("0"),
      "de_toy_sticks " -> Array("1"),
      "de_toy_other_yn" -> Array("0"),
      "de_toy_other" -> Array("Softball"), //FIXME necessary?
      "de_toys_amt" -> Array("2"),
      "de_chew_other_yn" -> Array("0")
    )
    val output2 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example2))

    output2.deRoutineToys.value shouldBe true
    output2.deRoutineToysIncludePlastic.value shouldBe 0
    output2.deRoutineToysIncludeStuffedFabric.value shouldBe 1
    output2.deRoutineToysIncludeUnstuffedFabric.value shouldBe 0
    output2.deRoutineToysIncludeRubber.value shouldBe 1
    output2.deRoutineToysIncludeMetal.value shouldBe 0
    output2.deRoutineToysIncludeAnimalProducts.value shouldBe 1
    output2.deRoutineToysIncludeLatex.value shouldBe 0
    output2.deRoutineToysIncludeRope.value shouldBe 1
    output2.deRoutineToysIncludeTennisBalls.value shouldBe 0
    output2.deRoutineToysIncludeSticks.value shouldBe 1
    output2.deRoutineToysIncludeOther.value shouldBe 0
    output2.deRoutineToysOtherDescription shouldBe None
    output2.deRoutineToysHoursPerDay.value shouldBe 2
    output2.deLicksChewsOrPlaysWithNonToys.value false

    // de_toys_yn = '0', de_chew_other_yn = '1'
    val example3 = Map[String, Array[String]](
      "de_toys_yn" -> Array("0"),
      "de_chew_other_yn" -> Array("1")
    )
    val output3 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example3))

    output3.deRoutineToys.value shouldBe false
    output3.deLicksChewsOrPlaysWithNonToys.value true

    //FIXME is this case useful at all?
    // de_toys_yn = '0', de_chew_other_yn = '0'
    val example4 = Map[String, Array[String]](
      "de_toys_yn" -> Array("0"),
      "de_chew_other_yn" -> Array("0")
    )
    val output4 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example4))

    output4.deRoutineToys.value shouldBe false
    output4.deLicksChewsOrPlaysWithNonToys.value false
  }








  it should "map SleepLocation fields when available" in {
    // if CONDITION
    val example1 = Map[String, Array[String]](
    )
    val output1 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example1))

    output1.FIELD.value shouldBe false
    output1.FIELD should be None
  }




  it should "map ToxinsIngested fields when available" in {
    // if CONDITION
    val example1 = Map[String, Array[String]](
      //EXAMPLE DATA
    )
    val output1 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example1))

    output1.FIELD.value shouldBe false
    output1.FIELD should be None
  }




  it should "map OtherAnimals fields when available" in {
    // if CONDITION
    val example1 = Map[String, Array[String]](
      //EXAMPLE DATA
    )
    val output1 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example1))

    output1.FIELD.value shouldBe false
    output1.FIELD should be None
  }















  it should "map GeneralRoutine fields when available" in {
    val example1 = Map[String, Array[String]](
      "de_routine_consistent" -> Array("1"),
      "de_amt_crate" -> Array("1"),
      "de_amt_roam_house" -> Array("3"),
      "de_amt_garage" -> Array("0"),
      "de_amt_kennel" -> Array("2"),
      "de_amt_yard" -> Array("2"),
      "de_amt_roam_outside" -> Array("1"),
      "de_amt_chain_outside" -> Array("0"),
      "de_amt_diff_location" -> Array("2"),
      "de_amt_other_animals" -> Array("3"),
      "de_amt_adults" -> Array("24"),
      "de_amt_teens" -> Array("0"),
      "de_amt_children" -> Array("1")
    )
    val output1 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example1))

    output1.deRoutineConsistency.value shouldBe 1
    output1.deRoutineHoursPerDayInCrate.value shouldBe 1
    output1.deRoutineHoursPerDayRoamingHouse.value shouldBe 3
    output1.deRoutineHoursPerDayInGarage.value shouldBe 0
    output1.deRoutineHoursPerDayInOutdoorKennel.value shouldBe 2
    output1.deRoutineHoursPerDayInYard.value shouldBe 2
    output1.deRoutineHoursPerDayRoamingOutside.value shouldBe 1
    output1.deRoutineHoursPerDayChainedOutside.value shouldBe 0
    output1.deRoutineHoursPerDayAwayFromHome.value shouldBe 2
    output1.deRoutineHoursPerDayWithOtherAnimals.value shouldBe 3
    output1.deRoutineHoursPerDayWithAdults.value shouldBe 24
    output1.deRoutineHoursPerDayWithTeens.value shouldBe 0
    output1.deRoutineHoursPerDayWithChildren.value shouldBe 1
  }

}
