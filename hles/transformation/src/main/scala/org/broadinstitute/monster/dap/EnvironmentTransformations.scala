package org.broadinstitute.monster.dap

import org.slf4j.LoggerFactory
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
    val addSeq: String =
      redcapEventName(1) match {
        case "arm"       => "1"
        case "secondary" => "2"
      }

    // set address month
    val addMonth: Option[String] = redcapEventName(0).filterNot(_.isDigit) match {
      case "jan"  => Some("1")
      case "feb"  => Some("2")
      case "mar"  => Some("3")
      case "apr"  => Some("4")
      case "may"  => Some("5")
      case "june"  => Some("6")
      case "july" => Some("7")
      case "aug"  => Some("8")
      case "sept" => Some("9")
      case "oct"  => Some("10")
      case "nov"  => Some("11")
      case "dec"  => Some("12")
      case _      => None
    }

    // set address year
    val addYear = redcapEventName(0).filter(_.isDigit)

    addMonth match {
      case None =>
        val logger = LoggerFactory.getLogger(getClass)
        val rawMonth = redcapEventName(0).filterNot(_.isDigit)
        logger.error(s"Record ID ${dogId} has invalid raw month '${rawMonth}'")
        None
      case Some(addressMonthStr) =>
        Some(
          Environment(
            dogId = dogId,
            addressMonth = addressMonthStr,
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
}
