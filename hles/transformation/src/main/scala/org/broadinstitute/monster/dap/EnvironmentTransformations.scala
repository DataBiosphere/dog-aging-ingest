package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dap.environment._
import org.broadinstitute.monster.dogaging.jadeschema.table.Environment

object EnvironmentTransformations {

  /** Parse all environment related fields out of a RedCap record.
    * The schema for environment variables has been separated into 5 jade-fragments.
    */
  def mapEnvironment(rawRecord: RawRecord): Option[Environment] = {
    val dog_id = rawRecord.id
    Some(
      Environment(
        dogId = dog_id,
        addressMonthYear = rawRecord.getRequired("redcap_event_name"),
        environmentGeocoding = Some(GeocodingTransformations.mapGeocodingMetadata(rawRecord)),
        environmentCensus = Some(CensusTransformations.mapCensusVariables(rawRecord)),
        environmentPollutants = Some(PollutantTransformations.mapPollutantVariables(rawRecord)),
        environmentTemperaturePrecipitation = Some(
          TemperaturePrecipitationTransformations.mapTemperaturePrecipitationVariables(rawRecord)
        ),
        environmentWalkability = Some(WalkabilityTransformations.mapWalkabilityVariables(rawRecord))
      )
    )
  }
}
