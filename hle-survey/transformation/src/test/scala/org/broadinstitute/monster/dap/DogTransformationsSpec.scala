package org.broadinstitute.monster.dap

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DogTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "DogTransformations"

  // TODO add tests for dog demographics transformation

  it should "correctly map study status fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "st_vip_or_staff" -> Array("2"), // multiple choice
      "st_batch_label" -> Array("this is my label"),
      "st_portal_invitation_date" -> Array("05-22-2020"),
      "st_portal_account_creation_date" -> Array("01-01-2000"),
      "st_hles_completion_date" -> Array("12-31-1999")
    )
    val exampleDogRecord =
      RawRecord(id = 1, exampleDogFields)
    val output = DogTransformations.mapDog(exampleDogRecord)

    output.stVipOrStaff shouldBe None
    output.stBatchLabel shouldBe None
    output.stPortalInvitationDate shouldBe None
    output.stPortalAccountCreationDate shouldBe None
    output.stHlesCompletionDate shouldBe None
  }
}
