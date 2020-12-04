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
  // as well as each individual form's *_complete field -> 2
  val extractionFilters: List[FilterDirective] = forms
    .map(form => FilterDirective(s"${form}_complete", FilterOps.==, "2")) ++ List(
    FilterDirective("baseline_complete", FilterOps.==, "2")
  )

  val subdir = "environment"
  val arm = List("baseline_arm_1")
  //val arm = "annual_2020_arm_1"
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
