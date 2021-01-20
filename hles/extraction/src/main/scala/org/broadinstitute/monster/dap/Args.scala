package org.broadinstitute.monster.dap

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import org.broadinstitute.monster.buildinfo.DogAgingHlesExtractionBuildInfo

@AppName("DAP HLE extraction pipeline")
@AppVersion(DogAgingHlesExtractionBuildInfo.version)
@ProgName("dog-aging-ingest")
case class Args(
  @HelpMessage("API token to use when querying RedCap")
  apiToken: String,
  @HelpMessage("Only extract records created/updated at or after this timestamp")
  startTime: Option[String],
  @HelpMessage("Only extract records created/updated before or at this timestamp")
  endTime: Option[String],
  @HelpMessage("Path where extracted JSON should be written")
  outputPrefix: String,
  @HelpMessage("Extract data dictionaries")
  pullDataDictionaries: Boolean
)
