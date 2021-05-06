package org.broadinstitute.monster.dap.sample

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common._

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._

object SampleExtractionPipeline extends ScioApp[Args] {

  val forms = List(
    "dna_kit_tracker",
    "sample_kit_tracker"
  )

  def extractionFiltersGenerator(args: Args): List[FilterDirective] =
    //  TODO: filter directives need to be confirmed
    List(
      FilterDirective("dna_kit_tracker_complete", FilterOps.==, "2")
    ) ++
      args.startTime
        .map(start =>
          List(FilterDirective("k1_verform_date", FilterOps.>, RedCapClient.redcapFormatDate(start)))
        )
        .getOrElse(List()) ++
      args.endTime
        .map(end =>
          List(FilterDirective("k1_verform_date", FilterOps.<, RedCapClient.redcapFormatDate(end)))
        )
        .getOrElse(List())

  // TODO: Need to confirm the fieldlist to pull from
  val subdir = "sample";
  val arm = "baseline_arm_1"
  val fieldList = List("co_consent")

  def buildPipelineWithWrapper(wrapper: HttpWrapper): PipelineBuilder[Args] =
    new ExtractionPipelineBuilder(
      forms,
      extractionFiltersGenerator,
      (_, _) => List(arm),
      fieldList,
      subdir,
      100,
      RedCapClient.apply(_: List[String], wrapper)
    )

  override def pipelineBuilder: PipelineBuilder[Args] = buildPipelineWithWrapper(new OkWrapper())
}
