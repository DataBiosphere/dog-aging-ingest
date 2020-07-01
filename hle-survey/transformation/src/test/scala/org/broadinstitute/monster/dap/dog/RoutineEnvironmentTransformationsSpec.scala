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

    // if de_spaces_yn = '1' AND de_spaces_get_to(98) = ("Walk", "Drive", "Bike")
    val example1 = Map[String, Array[String]](
      "de_spaces_yn" -> Array("1"),
      "de_spaces_freq" -> Array("12"),
      "de_spaces_get_to" -> Array("1", "2", "3"),
      "de_spaces_get_to_other" -> Array("15"),
      "de_dogpark_get_to_other" -> Array("Helicopter Shuttle"),
      "de_dogpark_hr" -> Array("10"),
      "de_dogpark_min" -> Array("15")
    )

    val output1 = RoutineEnvironmentTransformations.mapRoutineEnvironment(
      RawRecord(id = 1, example1)
    )


    output1.deRecreationalSpaces.value
    output1.deRecreationalSpacesDaysPerMonth
    output1.deRecreationalSpacesTravelWalk
    output1.deRecreationalSpacesTravelDrive
    output1.deRecreationalSpacesTravelBike
    output1.deRecreationalSpacesTravelPublicTransportation
    output1.deRecreationalSpacesTravelOther

    // de_spaces_yn = '1' AND de_spaces_get_to(98) = '1'

    // if de_spaces_yn = 0


    // Drive / Public Transport
    val example2 = Map[String, Array[String]](

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

    val example3 = Map[String, Array[String]](

    )

    val output3 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example3))

    output3.deDogpark.value shouldBe false
  }


  // FIXME how can I separate the different test cases (try different sets of arrays)
  // FIXME how to change the unit test structure to be better organized for that?
  // FIXME ex... should the last case be separated into a diff "it should" block?

  //TODO
  it should "map Work fields when available" in {
    // if CONDITION
    val example1 = Map[String, Array[String]](
      //EXAMPLE DATA
    )
    val output1 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example1))

    output1.FIELD.value shouldBe false
    output1.FIELD should be None
  }
  it should "map Sitter fields when available" in {
    // if CONDITION
    val example1 = Map[String, Array[String]](
      //EXAMPLE DATA
    )
    val output1 =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(1, example1))

    output1.FIELD.value shouldBe false
    output1.FIELD should be None
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
