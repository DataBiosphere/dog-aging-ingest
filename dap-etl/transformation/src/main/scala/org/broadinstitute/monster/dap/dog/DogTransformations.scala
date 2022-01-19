package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.common.{MissingOwnerIdError, RawRecord, TransformationError}
import org.broadinstitute.monster.dap.hles.HLESurveyTransformationPipelineBuilder.logger
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog

object DogTransformations {

  /** Parse all dog-related fields out of a raw RedCap record. */
  def mapDog(rawRecord: RawRecord): Option[HlesDog] = {
    val dogId = rawRecord.id

    rawRecord.getOptional("st_owner_id") match {
      case None =>
        MissingOwnerIdError(s"Record $dogId has less than 1 value for field st_owner_id").log
        None
      case Some(ownerId) =>
        try {
          Some(
            HlesDog(
              dogId = dogId,
              ownerId = ownerId.toLong,
              hlesDogStudyStatus = Some(StudyStatusTransformations.mapStudyStatus(rawRecord)),
              hlesDogDemographics = Some(DemographicsTransformations.mapDemographics(rawRecord)),
              hlesDogResidences = Some(DogResidenceTransformations.mapDogResidences(rawRecord)),
              hlesDogPhysicalActivity =
                Some(PhysicalActivityTransformations.mapPhysicalActivity(rawRecord)),
              hlesDogResidentialEnvironment =
                Some(ResidentialEnvironmentTransformations.mapResidentialEnvironment(rawRecord)),
              hlesDogRoutineEnvironment =
                Some(RoutineEnvironmentTransformations.mapRoutineEnvironment(rawRecord)),
              hlesDogBehavior = Some(BehaviorTransformations.mapBehavior(rawRecord)),
              hlesDogDiet = Some(DietTransformations.mapDiet(rawRecord)),
              hlesDogMedsPreventatives =
                Some(MedsAndPreventativesTransformations.mapMedsPreventatives(rawRecord)),
              hlesDogHealthSummary = Some(HealthStatusTransformations.mapHealthSummary(rawRecord)),
              hlesDogFutureStudies =
                Some(AdditionalStudiesTransformations.mapFutureStudies(rawRecord))
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
