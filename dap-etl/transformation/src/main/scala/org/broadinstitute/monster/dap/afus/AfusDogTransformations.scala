package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.afus.AfusTransformationPipelineBuilder.logger
import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.AfusDog

object AfusDogTransformations {

  def mapAfusDog(rawRecord: RawRecord): Option[AfusDog] = {
    val completeDate = rawRecord.getOptionalDate("fu_complete_date")
    rawRecord.getOptionalNumber("st_owner_id") match {
      case None =>
        MissingOwnerIdError(s"Record has less than 1 value for field st_owner_id").log
        None
      case Some(owner_id) =>
        try {
          Some(
            AfusDog(
              dogId = rawRecord.id,
              ownerId = owner_id,
              ownerIsVipOrStaff = rawRecord.getOptionalNumber("st_vip_or_staff"),
              afusCompleteDate = completeDate,
              // todo: figure out how to get year out of the Option[LocalDate]
              afusCalendarYear = completeDate match {
                case Some(date) => Some(date.getYear.toLong)
                case None       => None
              },
              // why was "st_fup_current_event" listed as the RC field?
              afusFollowupYear = Some(
                rawRecord.getRequired("redcap_event_name").split("_")(1).filter(_.isDigit).toLong
              ),
//              // owner fields? these are all in the owner table do we need them in the dog table as well??
//                owner_id
//                owner_is_vip_or_staff
//                afus_oc_primary_residence_state
//                afus_oc_primary_residence_census_division
//                afus_oc_primary_residence_ownership
//                afus_oc_primary_residence_ownership_other_description
//                afus_oc_primary_residence_time_percentage
//                afus_oc_secondary_residence
//                afus_oc_secondary_residence_state
//                afus_oc_secondary_residence_ownership
//                afus_oc_secondary_residence_ownership_other_description
//                afus_oc_secondary_residence_time_percentage
              // separate transform scripts
              afusDogDemographics = Some(DogDemographicsTransformations.mapDemographics(rawRecord)),
              afusDogResidences = Some(DogResidenceTransformations.mapDogResidences(rawRecord))
//              afusDogActivity = Some(Transformations.map(rawRecord))
//              afusDogEnvironment = Some(Transformations.map(rawRecord))
//              afusDogBehavior = Some(Transformations.map(rawRecord))
//              afusDogDiet = Some(Transformations.map(rawRecord))
//              afusDogMedsPreventatives = Some(Transformations.map(rawRecord))
//              afusDogDora = Some(Transformations.map(rawRecord))
//              afusDogMdors = Some(Transformations.map(rawRecord))
//              afusDogHealth = Some(Transformations.map(rawRecord))
            )
          )
        } catch {
          case e: AFUSTransformationError =>
            e.log
            None
        }
    }
  }
}
