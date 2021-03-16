package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.common.RawRecord

import java.time.LocalDate
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StudyStatusTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "StudyStatusTransformations"

  it should "map study status fields" in {
    val exampleDogFields = Map[String, Array[String]](
      "st_vip_or_staff" -> Array("2"),
      "st_batch_label" -> Array("this is my label"),
      "st_invite_to_portal" -> Array("2020-05-22"),
      "st_portal_account_date" -> Array("2000-01-01"),
      "st_dap_pack_date" -> Array("2020-01-15 10:21")
    )
    val output = StudyStatusTransformations.mapStudyStatus(RawRecord(id = 1, exampleDogFields))

    output.stVipOrStaff.value shouldBe 2
    output.stPortalInvitationDate.value shouldBe LocalDate.of(2020, 5, 22)
    output.stPortalAccountCreationDate.value shouldBe LocalDate.of(2000, 1, 1)
    output.stHlesCompletionDate.value shouldBe LocalDate.of(2020, 1, 15)
  }
}
