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
    // set address sequence
    val addSeq: String = if (redcapEventName(0) == "baseline") {
      "baseline"
    } else {
      redcapEventName(1) match {
        case "arm"       => "primary"
        case "secondary" => "secondary"
      }
    }
    // set address month
    val addMonth = if (redcapEventName(0) != "baseline") {
      Some(redcapEventName(0).filterNot(_.isDigit))
    } else {
      None
    }

    // set address year
    val addYear = if (redcapEventName(0) != "baseline") {
      Some(redcapEventName(0).filter(_.isDigit))
    } else {
      None
    }

    Some(
      Environment(
        dogId = dogId,
        addressSequence = addSeq,
        addressMonth = addMonth,
        addressYear = addYear,
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
