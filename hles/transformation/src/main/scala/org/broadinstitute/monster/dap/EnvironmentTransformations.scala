package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dap.environment._
import org.broadinstitute.monster.dogaging.jadeschema.table.Environment

object EnvironmentTransformations {

  /** Parse all environment related fields out of a RedCap record.
    * The schema for environment variables has been separated into 5 jade-fragments.
    */
  def mapEnvironment(rawRecord: RawRecord): Option[Environment] = {
    val dogId = rawRecord.id
    val redcapEventName = rawRecord.getRequired("redcap_event_name").split("_")
    if (redcapEventName(0).equals("baseline")) {
      return None
    }

    // set address sequence
    val addSeq: String =
      redcapEventName(1) match {
        case "arm"       => "1"
        case "secondary" => "2"
      }

    // set address month
    val addMonth = redcapEventName(0).filterNot(_.isDigit)

    // set address year
    val addYear = redcapEventName(0).filter(_.isDigit)

    Some(
      Environment(
        dogId = dogId,
        addressMonth = addMonth,
        addressYear = addYear,
        environmentGeocoding = Some(GeocodingTransformations.mapGeocodingMetadata(rawRecord)),
        environmentCensus = Some(CensusTransformations.mapCensusVariables(rawRecord)),
        environmentPollutants = Some(PollutantTransformations.mapPollutantVariables(rawRecord)),
        environmentTemperaturePrecipitation = Some(
          TemperaturePrecipitationTransformations.mapTemperaturePrecipitationVariables(rawRecord)
        ),
        environmentWalkability =
          Some(WalkabilityTransformations.mapWalkabilityVariables(rawRecord)),
        address1Or2 = addSeq
      )
    )
  }
}
