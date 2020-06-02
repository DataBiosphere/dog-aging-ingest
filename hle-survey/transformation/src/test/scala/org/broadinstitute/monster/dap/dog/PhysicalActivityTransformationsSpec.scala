package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PhysicalActivityTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "PhysicalActivityTransformations"

  it should "map physical activity fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "" -> Array("")
    )
    val output = PhysicalActivityTransformations.mapPhysicalActivity(
      RawRecord(id = 1, exampleDogFields),
      HlesDog.init(dogId = 1, ownerId = 1)
    )
  }
}
