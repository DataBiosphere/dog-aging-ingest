package org.broadinstitute.monster.dap.common

import caseapp.core.Error.MalformedValue
import caseapp.core.argparser.{ArgParser, SimpleArgParser}
import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import org.broadinstitute.monster.buildinfo.DogAgingHlesExtractionBuildInfo

import java.time.OffsetDateTime
import java.time.format.DateTimeParseException

@AppName("DAP HLE extraction pipeline")
@AppVersion(DogAgingHlesExtractionBuildInfo.version)
@ProgName("dog-aging-ingest")
case class Args(
  @HelpMessage("API token to use when querying RedCap")
  apiToken: String,
  @HelpMessage("Only extract records created/updated at or after this timestamp")
  startTime: Option[OffsetDateTime],
  @HelpMessage("Only extract records created/updated before or at this timestamp")
  endTime: Option[OffsetDateTime],
  @HelpMessage("Path where extracted JSON should be written")
  outputPrefix: String,
  @HelpMessage("Extract data dictionaries")
  pullDataDictionaries: Boolean
)

object Args {

  implicit val odtParser: ArgParser[OffsetDateTime] = SimpleArgParser.from("timestamp") { s =>
    try {
      Right(OffsetDateTime.parse(s))
    } catch {
      case e: DateTimeParseException => Left(MalformedValue("date", e.getMessage))
    }
  }
}
