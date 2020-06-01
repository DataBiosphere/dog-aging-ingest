package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dap.dog._
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog

object DogTransformations {

  /** Parse all dog-related fields out of a raw RedCap record. */
  def mapDog(rawRecord: RawRecord): HlesDog = {
    val dogBase = HlesDog.init(
      dogId = rawRecord.id,
      ownerId = rawRecord.getRequired("st_owner_id").toLong
    )

    val transformations = List(
      StudyStatusTransformations.mapStudyStatus _,
      DemographicsTransformations.mapDemographics _,
      BehaviorTransformations.mapBehavior _
    )

    transformations.foldLeft(dogBase)((acc, f) => f(rawRecord, acc))
  }
}
