package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.LocalDate

class DogTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "DogTransformations"

  // TODO add tests for dog demographics transformation

  it should "map study status fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "st_vip_or_staff" -> Array("2"),
      "st_batch_label" -> Array("this is my label"),
      "st_invite_to_portal" -> Array("2020-05-22"),
      "st_portal_account_date" -> Array("2000-01-01"),
      "st_dap_pack_date" -> Array("2020-01-15 10:21")
    )
    val output = DogTransformations.mapStudyStatus(
      RawRecord(id = 1, exampleDogFields),
      HlesDog.init(dogId = 1, ownerId = 1)
    )

    output.stVipOrStaff shouldBe Some(2)
    output.stBatchLabel shouldBe Some("this is my label")
    output.stPortalInvitationDate shouldBe Some(LocalDate.of(2020, 5, 22))
    output.stPortalAccountCreationDate shouldBe Some(LocalDate.of(2000, 1, 1))
    output.stHlesCompletionDate shouldBe Some(LocalDate.of(2020, 1, 15))
  }

  it should "map pure-breed demographics fields" in {
    val akcExample = Map[String, Array[String]](
      "dd_dog_pure_or_mixed" -> Array("1"),
      "dd_dog_breed" -> Array("10"),
      "dd_dog_breed_non_akc" -> Array("This should not pass through"),
      "dd_dog_breed_mix_1" -> Array("Uh oh, this shouldn't be here!")
    )
    val nonAkcExample = Map[String, Array[String]](
      "dd_dog_pure_or_mixed" -> Array("1"),
      "dd_dog_breed" -> Array("277"),
      "dd_dog_breed_non_akc" -> Array("This should pass through")
    )

    val akcOut = DogTransformations.mapBreed(
      RawRecord(id = 1, akcExample),
      HlesDog.init(dogId = 1, ownerId = 1)
    )
    val nonAkcOut = DogTransformations.mapBreed(
      RawRecord(id = 1, nonAkcExample),
      HlesDog.init(dogId = 1, ownerId = 1)
    )

    akcOut.ddBreedPureOrMixed shouldBe Some(1L)
    akcOut.ddBreedPure shouldBe Some(10L)
    akcOut.ddBreedPureNonAkc shouldBe None
    akcOut.ddBreedMixedPrimary shouldBe None

    nonAkcOut.ddBreedPureOrMixed shouldBe Some(1L)
    nonAkcOut.ddBreedPure shouldBe Some(277L)
    nonAkcOut.ddBreedPureNonAkc shouldBe Some("This should pass through")
  }

  it should "map mixed-breed demographics fields" in {
    val example = Map[String, Array[String]](
      "dd_dog_pure_or_mixed" -> Array("2"),
      "dd_dog_breed" -> Array("wot, shouldn't be here"),
      "dd_dog_breed_mix_1" -> Array("11"),
      "dd_dog_breed_mix_2" -> Array("3")
    )
    val out = DogTransformations.mapBreed(
      RawRecord(id = 1, example),
      HlesDog.init(dogId = 1, ownerId = 1)
    )

    out.ddBreedPureOrMixed shouldBe Some(2L)
    out.ddBreedPure shouldBe None
    out.ddBreedMixedPrimary shouldBe Some(11L)
    out.ddBreedMixedSecondary shouldBe Some(3L)
  }

  it should "map age-related demographics fields when birth year and month are known" in {
    ???
  }

  it should "map age-related demographics fields when birth year is known" in {
    ???
  }

  it should "map age-related demographics fields when age is estimated by owner" in {
    ???
  }

  it should "map sex-related demographics fields for male dogs" in {
    ???
  }

  it should "map sex-related demographics fields for female dogs" in {
    ???
  }

  it should "map weight-related demographics fields" in {
    ???
  }

  it should "map insurance-related demographics fields" in {
    ???
  }

  it should "map acquisition-related demographics fields" in {
    ???
  }

  it should "map activity-related demographics fields" in {
    ???
  }

  it should "map residence-related demographics fields" in {
    ???
  }
}
