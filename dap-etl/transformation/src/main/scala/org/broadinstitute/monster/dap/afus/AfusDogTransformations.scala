package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.afus.AfusTransformationPipelineBuilder.logger
import org.broadinstitute.monster.dap.common.{TransformationError, MissingOwnerIdError, RawRecord}
import org.broadinstitute.monster.dogaging.jadeschema.table.AfusDog

object AfusDogTransformations {

  def mapAfusDog(rawRecord: RawRecord): Option[AfusDog] = {
    rawRecord.getOptionalNumber("st_owner_id") match {
      case None =>
        MissingOwnerIdError(s"Record has less than 1 value for field st_owner_id").log
        None
      case Some(owner_id) =>
        try {
          val completeDate = rawRecord.getOptionalDate("fu_complete_date")
          val redcapEventName = rawRecord.getOptional("redcap_event_name")
          Some(
            AfusDog(
              dogId = rawRecord.id,
              ownerId = owner_id,
              ownerIsVipOrStaff = rawRecord.getOptionalNumber("st_vip_or_staff"),
              afusCompleteDate = completeDate,
              afusCalendarYear = completeDate match {
                case Some(date) => Some(date.getYear.toLong)
                case None       => None
              },
              // todo: redcap_event_name is not available with the current grouping of records (see TransformationHelper)
              // todo: figure out how to do the correct grouping of records
              afusFollowupYear = redcapEventName match {
                case Some(event) => Some(event.split("_")(1).toLong)
                case None        => None
              },
              //Some(redcapEvent.split("_")(1).filter(_.isDigit).toLong),
              //Some(redcapEvent.split("_").redcapEventName(0).filter(_.isDigit)afus_dd_alternate_recent_residence_count
              afusDogDemographics = Some(DemographicsTransformations.mapDemographics(rawRecord)),
              afusDogPhysicalActivity =
                Some(PhysicalActivityTransformations.mapPhysicalActivities(rawRecord)),
              afusDogEnvironment = Some(EnvironmentTransformations.mapEnvironment(rawRecord)),
              afusDogBehavior = Some(BehaviorTransformations.mapBehavior(rawRecord)),
              afusDogDiet = Some(DietTransformations.mapDiet(rawRecord)),
              afusDogMedsPreventatives =
                Some(MedsPreventativesTransformations.mapMedsPreventatives(rawRecord)),
              afusDogDora = Some(DoraTransformations.mapDora(rawRecord)),
              afusDogMdors = Some(MdorsTransformations.mapMdors(rawRecord)),
              afusDogHealthStatus = Some(HealthStatusTransformations.mapHealthStatus(rawRecord))
            )
          )
        } catch {
          case e: TransformationError =>
            e.log
            None
        }
    }
  }
}
