package org.broadinstitute.monster.dap.environment

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common._

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
  def extractionFiltersGenerator(args: Args): List[FilterDirective] =
    List(FilterDirective("baseline_complete", FilterOps.==, "2")) ++
      args.startTime
        .map(start =>
          List(FilterDirective("bl_dap_pack_date", FilterOps.>, RedCapClient.redcapFormatDate(start)))
        )
        .getOrElse(List()) ++
      args.endTime
        .map(end =>
          List(FilterDirective("bl_dap_pack_date", FilterOps.<, RedCapClient.redcapFormatDate(end)))
        )
        .getOrElse(List())

  val subdir = "environment"

  val arm =
    List(
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
      "june2020_secondary_arm_1",
      "july2020_arm_1",
      "july2020_secondary_arm_1",
      "aug2020_arm_1",
      "aug2020_secondary_arm_1",
      "sept2020_arm_1",
      "sept2020_secondary_arm_1",
      "oct2020_arm_1",
      "oct2020_secondary_arm_1",
      "nov2020_arm_1",
      "nov2020_secondary_arm_1",
      "dec2020_arm_1",
      "dec2020_secondary_arm_1"
    )
  val fieldList = List("baseline_complete")

  def buildPipelineWithWrapper(wrapper: HttpWrapper): PipelineBuilder[Args] =
    new ExtractionPipelineBuilder(
      forms,
      extractionFiltersGenerator,
      arm,
      fieldList,
      subdir,
      // RedCap times out at the default batch size of 100
      10,
      RedCapClient.apply(_: List[String], wrapper)
    )

  override def pipelineBuilder: PipelineBuilder[Args] = buildPipelineWithWrapper(new OkWrapper())
}
