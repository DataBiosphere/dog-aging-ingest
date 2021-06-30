package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DeathTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "DeathTransformations"

  it should "map death transformations variables where complete" in {
    val DeathTransform = Map(
      "eol_death_location" -> Array("1"),
      "eol_anyone_present" -> Array("1"),
      "eol_who_present" -> Array("1", "2"),
      "eol_death_witness_who_you" -> Array("true"),
      "eol_death_witness_who_family" -> Array("true")
    )

    val deathMapped = DeathTransformations.mapDeathTransformations(
      RawRecord(1, DeathTransform)
    )

    deathMapped.eolDeathLocation.value shouldBe 1L
    deathMapped.eolDeathWitness.value shouldBe 1L
    deathMapped.eolDeathWitnessWhoYou.value shouldBe true
    deathMapped.eolDeathWitnessWhoFamily.value shouldBe true
  }
}
