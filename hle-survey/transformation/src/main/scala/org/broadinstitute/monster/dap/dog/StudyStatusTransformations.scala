package org.broadinstitute.monster.dap.dog

import java.time.LocalDate

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog

object StudyStatusTransformations {

  /**
    * Parse all study-status-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapStudyStatus(rawRecord: RawRecord, dog: HlesDog): HlesDog =
    dog.copy(
      stVipOrStaff = rawRecord.getOptionalNumber("st_vip_or_staff"),
      stBatchLabel = rawRecord.getOptional("st_batch_label"),
      stPortalInvitationDate = rawRecord.getOptionalDate("st_invite_to_portal"),
      stPortalAccountCreationDate = rawRecord.getOptionalDate("st_portal_account_date"),
      stHlesCompletionDate = rawRecord.getOptional("st_dap_pack_date").map { timeString =>
        val splitPoint = timeString.indexOf(" ")
        LocalDate.parse(timeString.take(splitPoint))
      }
    )
}
