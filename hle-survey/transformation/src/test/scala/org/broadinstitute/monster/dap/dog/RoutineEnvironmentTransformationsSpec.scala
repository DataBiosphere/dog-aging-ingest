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

    // de_sitter_yn = 0
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
    // if CONDITION
    val example1 = Map[String, Array[String]](
      //EXAMPLE DATA
    )
    val output1 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example1))

    output1.FIELD.value shouldBe false
    output1.FIELD should be None
  }
  it should "map Toys fields when available" in {
    // if CONDITION
    val example1 = Map[String, Array[String]](
      //EXAMPLE DATA
    )
    val output1 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example1))

    output1.FIELD.value shouldBe false
    output1.FIELD should be None
  }
  it should "map SleepLocation fields when available" in {
    // if CONDITION
    val example1 = Map[String, Array[String]](
      //EXAMPLE DATA
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
    // if CONDITION
    val example1 = Map[String, Array[String]](
      //EXAMPLE DATA
    )
    val output1 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example1))

    output1.FIELD.value shouldBe false
    output1.FIELD should be None
  }

}
