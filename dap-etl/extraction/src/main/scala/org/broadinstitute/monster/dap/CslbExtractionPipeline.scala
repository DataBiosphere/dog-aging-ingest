package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dap.common.{
  Args,
  ExtractionPipelineBuilder,
  FilterDirective,
  FilterOps,
  HttpWrapper,
  OkWrapper,
  RedCapClient
}
import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._

object CslbExtractionPipeline extends ScioApp[Args] {

  val forms = List(
    "recruitment_fields",
    "canine_social_and_learned_behavior"
  )

  // Magic marker for "completed".
  // NB: We are purposefully excluding the recruitment_fields_complete -> 2
  // mapping, as that conflicts with the CSLB data
  def extractionFiltersGenerator(args: Args): List[FilterDirective] =
    List(
      FilterDirective("canine_social_and_learned_behavior_complete", FilterOps.==, "2")
    ) ++
      args.startTime
        .map(start =>
          List(FilterDirective("cslb_date", FilterOps.>, RedCapClient.redcapFormatDate(start)))
        )
        .getOrElse(List()) ++
      args.endTime
        .map(end => List(FilterDirective("cslb_date", FilterOps.<, RedCapClient.redcapFormatDate(end))))
        .getOrElse(List())

  val subdir = "cslb"
  val arm = List("annual_2020_arm_1")
  val fieldList = List("co_consent")

  def buildPipelineWithWrapper(wrapper: HttpWrapper): PipelineBuilder[Args] =
    new ExtractionPipelineBuilder(
      forms,
      extractionFiltersGenerator,
      arm,
      fieldList,
      subdir,
      100,
      RedCapClient.apply(_: List[String], wrapper)
    )

  override def pipelineBuilder: PipelineBuilder[Args] = buildPipelineWithWrapper(new OkWrapper())
}
