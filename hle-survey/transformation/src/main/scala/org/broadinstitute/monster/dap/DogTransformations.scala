package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dap.dog._
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog

object DogTransformations {

  /** Parse all dog-related fields out of a raw RedCap record. */
  def mapDog(rawRecord: RawRecord): HlesDog = HlesDog(
    dogId = rawRecord.id,
    ownerId = rawRecord.getRequired("st_owner_id").toLong,
    hlesDogStudyStatus = Some(StudyStatusTransformations.mapStudyStatus(rawRecord)),
    hlesDogDemographics = Some(DemographicsTransformations.mapDemographics(rawRecord)),
    hlesDogResidences = Some(DogResidenceTransformations.mapDogResidences(rawRecord)),
    hlesDogPhysicalActivity = Some(PhysicalActivityTransformations.mapPhysicalActivity(rawRecord)),
    hlesDogResidentialEnvironment =
      Some(ResidentialEnvironmentTransformations.mapResidentialEnvironment(rawRecord)),
    hlesDogBehavior = Some(BehaviorTransformations.mapBehavior(rawRecord)),
    hlesDogDiet = Some(DietTransformations.mapDiet(rawRecord)),
    hlesDogMedsPreventatives =
      Some(MedsAndPreventativesTransformations.mapMedsPreventatives(rawRecord)),
    hlesDogHealthSummary = Some(HealthStatusTransformations.mapHealthSummary(rawRecord)),
    hlesDogFutureStudies = Some(AdditionalStudiesTransformations.mapFutureStudies(rawRecord))
  )
}
