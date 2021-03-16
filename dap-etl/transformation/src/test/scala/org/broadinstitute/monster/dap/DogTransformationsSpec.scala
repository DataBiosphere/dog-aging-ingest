package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.fragment._
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.OptionValues

class DogTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "DogTransformations"

  it should "map required fields" in {
    val mapped = DogTransformations.mapDog(
      RawRecord(
        1,
        Map(
          "st_owner_id" -> Array("2"),
          "df_supplement_daily" -> Array("0"),
          "df_supplement_ltd" -> Array("0")
        )
      )
    )
    mapped shouldBe Some(
      HlesDog(
        dogId = 1L,
        ownerId = 2L,
        hlesDogStudyStatus = Some(HlesDogStudyStatus.init()),
        hlesDogDemographics = Some(HlesDogDemographics.init()),
        hlesDogResidences = Some(HlesDogResidences.init()),
        hlesDogBehavior = Some(HlesDogBehavior.init()),
        hlesDogDiet = Some(
          HlesDogDiet
            .init()
            .copy(dfDailySupplements = Some(false), dfInfrequentSupplements = Some(false))
        ),
        hlesDogHealthSummary = Some(HlesDogHealthSummary.init()),
        hlesDogPhysicalActivity = Some(HlesDogPhysicalActivity.init()),
        hlesDogResidentialEnvironment = Some(HlesDogResidentialEnvironment.init()),
        hlesDogRoutineEnvironment = Some(HlesDogRoutineEnvironment.init()),
        hlesDogMedsPreventatives = Some(HlesDogMedsPreventatives.init()),
        hlesDogFutureStudies = Some(HlesDogFutureStudies.init())
      )
    )
  }

  it should "not map dog when st_owner_id is missing" in {
    val mapped = DogTransformations.mapDog(RawRecord(1, Map()))
    mapped shouldBe None
  }
}
