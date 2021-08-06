package org.broadinstitute.monster.dap.environment

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common._

import java.time.{OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._

class EnvironmentExtractionFailException() extends Exception

object EnvironmentExtractionPipeline extends ScioApp[Args] {

  val formatter = DateTimeFormatter.ofPattern("MMMyyyy")
  val formatterAlt = DateTimeFormatter.ofPattern("MMMMyyyy")

  val EnvironmentEpoch = OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

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

  // get list of individual dates, then get the set of years and return a list of distinct monthYears
  def getMonthYearList(start: OffsetDateTime, end: OffsetDateTime): List[String] = {
    if (start.isAfter(end)) throw new EnvironmentExtractionFailException
    val dateList =
      (Iterator.iterate(start)(_ plusDays 1) takeWhile (_ isBefore end.plusDays(1))).toList
    dateList
      .map(date =>
        // event names for June, July, and Sept have 4 alpha characters before the year
        if (List(6,7,9).contains(date.getMonthValue) ) {
          date.format(formatterAlt).toLowerCase.distinct
        } else date.format(formatter).toLowerCase
      )
      .distinct
  }

  def extractionArmsGenerator(
    startTime: Option[OffsetDateTime],
    endTime: Option[OffsetDateTime]
  ): List[String] = {
    // use first year of environment if startTime was not provided
    val startDate = startTime.getOrElse(EnvironmentEpoch)
    // use current date if endTime was not provided
    val endDate = endTime.getOrElse(OffsetDateTime.now())
    // environment has two arms per month
    getMonthYearList(startDate, endDate).flatMap { date =>
      List(s"${date}_arm_1", s"${date}_secondary_arm_1")
    }
  }

  val fieldList = List("baseline_complete")

  def buildPipelineWithWrapper(wrapper: HttpWrapper): PipelineBuilder[Args] =
    new ExtractionPipelineBuilder(
      forms,
      extractionFiltersGenerator,
      extractionArmsGenerator,
      fieldList,
      subdir,
      // RedCap times out at the default batch size of 100
      10,
      RedCapClient.apply(_: List[String], wrapper)
    )

  override def pipelineBuilder: PipelineBuilder[Args] = buildPipelineWithWrapper(new OkWrapper())
}
