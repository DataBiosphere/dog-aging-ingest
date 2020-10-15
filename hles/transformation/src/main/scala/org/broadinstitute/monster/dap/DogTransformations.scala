package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dap.dog._
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog

object DogTransformations {

  /** Parse all dog-related fields out of a raw RedCap record. */
  def mapDog(rawRecord: RawRecord): Option[HlesDog] = {
    val dog_id = rawRecord.id

    rawRecord.getOptional("st_owner_id") match {
      case None =>
        MissingOwnerId(s"Record $dog_id has more/less than 1 value for field st_owner_id")
        None
      case Some(ownerId) =>
        Some(
          HlesDog(
            dogId = dog_id,
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
            hlesDogFutureStudies = Some(AdditionalStudiesTransformations.mapFutureStudies(rawRecord))
          )
        )
    }
  }
}
