package org.broadinstitute.monster.dap

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._

object EnvironmentExtractionPipeline extends ScioApp[Args] {

  val forms = List(
    "geocoding_metadata",
    "census_variables",
    "pollutant_variables",
    "temperature_and_precipitation_variables",
    "walkability_variables"
  )

  // Magic marker for "completed".
  // NB: We are looking for baseline_complete -> 2
  val extractionFilters: List[FilterDirective] = List(
    FilterDirective("baseline_complete", FilterOps.==, "2")
  )

  val subdir = "environment"
  //todo: need to query for all arms and work through arms serially
  val arm =
    List(
      "baseline_arm_1",
      "dec2019_arm_1",
      "dec2019_secondary_arm_1",
      "jan2020_arm_1",
      "jan2020_secondary_arm_1",
      "feb2020_arm_1",
      "feb2020_secondary_arm_1",
      "mar2020_arm_1",
      "mar2020_secondary_arm_1",
      "apr2020_arm_1",
      "apr2020_secondary_arm_1",
      "may2020_arm_1",
      "may2020_secondary_arm_1",
      "june2020_arm_1",
      "june2020_secondary_arm_1"
    )
  val fieldList = List("baseline_complete")

  override def pipelineBuilder: PipelineBuilder[Args] =
    new ExtractionPipelineBuilder(
      forms,
      extractionFilters,
      arm,
      fieldList,
      subdir,
      100,
      RedCapClient.apply
    )
}
