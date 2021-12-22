package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common._

import java.time.{OffsetDateTime, ZoneOffset}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._
class AfusExtractionFailException() extends Exception

object AfusExtractionPipeline extends ScioApp[Args] {

  //todo update this: January 1, 2021 - we ignore any records before this by default (though there shouldn't be any)
  val AfusEpoch = OffsetDateTime.of(2021, 1, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

  val forms = List(
    "recruitment_fields",
    "followup_status",
    "followup_owner_contact",
    "study_status",
    "followup_owner_demographics",
    "followup_physical_activity",
    "followup_environment",
    "followup_behavior",
    "followup_diet",
    "followup_meds_and_preventives",
    "followup_canine_eating_behavior_dora",
    "followup_dogowner_relationship_survey_mdors",
    "followup_health_status"
  )

  def extractionFiltersGenerator(args: Args): List[FilterDirective] = {
    // FORM = followup_status
    val standardDirectives: List[FilterDirective] = List(
      FilterDirective("fu_is_completed", FilterOps.==, "1")
    )
    val dateFilters: List[FilterDirective] = {
      args.startTime
        .map(start =>
          List(
            FilterDirective(
              "fu_complete_date",
              FilterOps.>,
              RedCapClient.redcapFormatDate(start)
            )
          )
        )
        .getOrElse(List()) ++
        args.endTime
          .map(end =>
            List(
              FilterDirective(
                "fu_complete_date",
                FilterOps.<,
                RedCapClient.redcapFormatDate(end)
              )
            )
          )
          .getOrElse(List())
    }

    standardDirectives ++ dateFilters
  }

  val subdir = "afus";
  val fieldList = List("fu_is_completed", "st_owner_id")

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
    // use first year of afus if startTime was not provided
    val startDate = startTime.getOrElse(AfusEpoch)
    // use current date if endTime was not provided
    val endDate = endTime.getOrElse(OffsetDateTime.now())
    // afus has one arm per year ("fup_{sequence}_arm_1")
    val yearList = getYearList(startDate, endDate)
    val yearSeqList = List.range(1, yearList.length + 1)
    yearSeqList.map { seq =>
      s"fup_${seq}_arm_1"
    } ++ List("baseline_arm_1")
  }

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
