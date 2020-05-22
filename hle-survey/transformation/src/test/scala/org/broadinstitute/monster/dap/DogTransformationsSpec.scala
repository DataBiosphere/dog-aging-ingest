package org.broadinstitute.monster.dap

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.LocalDate

class DogTransformationsSpec extends AnyFlatSpec with Matchers {
  behavior of "DogTransformations"

  // TODO add tests for dog demographics transformation

  it should "correctly map study status fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "dd_us_born" -> Array("1"), // a required field
      "st_vip_or_staff" -> Array("2"),
      "st_batch_label" -> Array("this is my label"),
      "st_invite_to_portal" -> Array("05-22-2020"),
      "st_portal_account_date" -> Array("01-01-2000"),
      "st_dap_pack_date" -> Array("12-31-1999")
    )
    val output = DogTransformations.mapDog(RawRecord(id = 1, exampleDogFields))

    output.stVipOrStaff shouldBe Some("2")
    output.stBatchLabel shouldBe Some("this is my label")
    output.stPortalInvitationDate shouldBe Some(LocalDate.of(2020, 5, 22))
    output.stPortalAccountCreationDate shouldBe Some(LocalDate.of(2000, 1, 1))
    output.stHlesCompletionDate shouldBe Some(LocalDate.of(1999, 12, 31))
  }
}
