package org.broadinstitute.monster.dap.cslb

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common._

import java.time.{OffsetDateTime, ZoneOffset}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._
class CslbExtractionFailException() extends Exception

object CslbExtractionPipeline extends ScioApp[Args] {

  val CSLBEpoch = OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

  val forms = List(
    "recruitment_fields",
    "canine_social_and_learned_behavior",
    "study_status"
  )

  // Magic marker for "completed".
  // NB: We are purposefully excluding the recruitment_fields_complete -> 2
  // mapping, as that conflicts with the CSLB data
  def extractionFiltersGenerator(args: Args): List[FilterDirective] = {
    val standardDirectives: List[FilterDirective] = List(
      FilterDirective("canine_social_and_learned_behavior_complete", FilterOps.==, "2"),
      FilterDirective("st_vip_or_staff", FilterOps.==, "0")
    )
    val dateFilters: List[FilterDirective] = {
      args.endTime
        .map(end => List(FilterDirective("cslb_date", FilterOps.<, RedCapClient.redcapFormatDate(end))))
        .getOrElse(List())
    }
    standardDirectives ++ dateFilters
  }

  val subdir = "cslb"

  // get list of individual dates, then get the set of years and return a list of distinct years
  def getYearList(start: OffsetDateTime, end: OffsetDateTime): List[Int] = {
    if (start.isAfter(end)) throw new CslbExtractionFailException
    val dateList =
      (Iterator.iterate(start)(_ plusDays 1) takeWhile (_ isBefore end.plusDays(1))).toList
    dateList.map(date => date.getYear).distinct
  }

  // use args.startTime and args.endTime to determine full list
  // branching logic: if args are provided, use dates to generate a list of the subset of arms
  def extractionArmsGenerator(
    startTime: Option[OffsetDateTime],
    endTime: Option[OffsetDateTime]
  ): List[String] = {
    // use first year of cslb if startTime was not provided
    val startDate = startTime.getOrElse(CSLBEpoch)
    // use current date if endTime was not provided
    val endDate = endTime.getOrElse(OffsetDateTime.now())
    // cslb has one arm per year ("annual_{yyyy}_arm_1")
    getYearList(startDate, endDate).map { date =>
      s"annual_${date}_arm_1"
    } ++ List("baseline_arm_1")
  }

  val fieldList = List("co_consent", "st_vip_or_staff")

  def buildPipelineWithWrapper(wrapper: HttpWrapper): PipelineBuilder[Args] =
    new ExtractionPipelineBuilder(
      forms,
      extractionFiltersGenerator,
      extractionArmsGenerator,
      fieldList,
      subdir,
      10,
      RedCapClient.apply(_: List[String], wrapper)
    )

  override def pipelineBuilder: PipelineBuilder[Args] = buildPipelineWithWrapper(new OkWrapper())
}
