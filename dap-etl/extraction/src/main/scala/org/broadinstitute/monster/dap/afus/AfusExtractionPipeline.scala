package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common._

import java.time.{OffsetDateTime, ZoneOffset}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._
class AfusExtractionFailException() extends Exception

object AfusExtractionPipeline extends ScioApp[Args] {

  // january 1, 2018 - we ignore any records before this by default (though there shouldn't be any)
  val AfusEpoch = OffsetDateTime.of(2021, 9, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

  val forms = List(
    "followup_owner_contact",
    "study_status"
  )

  def extractionFiltersGenerator(args: Args): List[FilterDirective] = {
    // make sure we are only collecting completed survey forms
    val completionFilters: List[FilterDirective] = forms
      .map(form => FilterDirective(s"${form}_complete", FilterOps.==, "2"))
    // FORM = followup_status
    val standardDirectives: List[FilterDirective] = List(
      FilterDirective("fu_is_completed", FilterOps.==, "1")
    )
    // todo: what date field should we filter on?
    val dateFilters: List[FilterDirective] = {
      args.startTime
        .map(start =>
          List(
            FilterDirective("k1_rtn_tracking_date", FilterOps.>, RedCapClient.redcapFormatDate(start))
          )
        )
        .getOrElse(List()) ++
        args.endTime
          .map(end =>
            List(
              FilterDirective("k1_rtn_tracking_date", FilterOps.<, RedCapClient.redcapFormatDate(end))
            )
          )
          .getOrElse(List())
    }
    // todo: check on date filters
    completionFilters ++ standardDirectives ++ dateFilters
  }

  val subdir = "sample";
  val arm = "baseline_arm_1"
  val fieldList = List("k1_tube_serial", "k1_rtn_tracking_date")

  // get list of individual dates, then get the set of years and return a list of distinct years
  def getYearList(start: OffsetDateTime, end: OffsetDateTime): List[Int] = {
    if (start.isAfter(end)) throw new AfusExtractionFailException
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
    val startDate = startTime.getOrElse(AfusEpoch)
    // use current date if endTime was not provided
    val endDate = endTime.getOrElse(OffsetDateTime.now())
    // afus has one arm per year ("fup_{sequence}_arm_1")
    val yearList = getYearList(startDate, endDate)
    val yearSeqList = List.range(1, yearList.length)
    yearSeqList.map { seq =>
      s"fup_${seq}_arm_1"
    }
  }

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
