package org.broadinstitute.monster.dap

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import org.broadinstitute.monster.buildinfo.DogAgingHleTransformationBuildInfo

@AppName("HLESurvey transformation pipeline")
@AppVersion(DogAgingHleTransformationBuildInfo.version)
@ProgName("org.broadinstitute.monster.etl.dap.HLESurveyTransformationPipeline")
case class Args(
  @HelpMessage("Path to the top-level directory where JSON was extracted")
  inputPrefix: String,
  @HelpMessage("Path where transformed JSON should be written")
  outputPrefix: String
)
