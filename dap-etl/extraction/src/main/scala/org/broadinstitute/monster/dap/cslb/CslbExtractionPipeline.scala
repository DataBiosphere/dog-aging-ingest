package org.broadinstitute.monster.dap.cslb

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common._
import java.time.{LocalDate, OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

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

  // get List[LocalDate] at 1 year intervals between startDate and endTime
  def getDateYearList(start: LocalDate, end: LocalDate): List[LocalDate] =
    (Iterator.iterate(start)(_ plusYears 1) takeWhile (_ isBefore end)).toList

  // use args.startTime and args.endTime to determine full list
  // branching logic: if args are provided, use dates to generate a list of the subset of arms
  def extractionArmsGenerator(args: Args): List[String] = {
    val startDate = args.startTime
      .map(start => start.toLocalDate())
      // use first year of hles if startTime was not provided
      .getOrElse(LocalDate.of(2020, 1, 1))
    val endDate = args.endTime
      .map(end => end.toLocalDate())
      // use current date if endTime was not provided
      .getOrElse(LocalDate.now())
    // cslb has one arm per year ("annual_{yyyy}_arm_1")
    getDateYearList(startDate, endDate).map{
      date => s"annual_${date.getYear()}_arm_1"
    }
  }

  val fieldList = List("co_consent")

  def buildPipelineWithWrapper(wrapper: HttpWrapper): PipelineBuilder[Args] =
    new ExtractionPipelineBuilder(
      forms,
      extractionFiltersGenerator,
      extractionArmsGenerator,
      fieldList,
      subdir,
      100,
      RedCapClient.apply(_: List[String], wrapper)
    )

  override def pipelineBuilder: PipelineBuilder[Args] = buildPipelineWithWrapper(new OkWrapper())
}
