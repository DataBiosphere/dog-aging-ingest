package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RoutineEnvironmentTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "RoutineEnvironmentTransformations"

  it should "map all high level fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "" -> Array("")
    )
    val output =
      RoutineEnvironmentTransformations.mapRoutineEnvironment(RawRecord(id = 1, exampleDogFields))

    output.deDogpark shouldBe None
  }
}
