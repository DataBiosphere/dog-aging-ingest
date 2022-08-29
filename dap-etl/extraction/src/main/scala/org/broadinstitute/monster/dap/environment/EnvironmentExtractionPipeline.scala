package org.broadinstitute.monster.dap.environment

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common._

import java.time.{OffsetDateTime, ZoneOffset}
import java.time.temporal.ChronoField
import java.time.format.DateTimeFormatterBuilder
import collection.JavaConverters._

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._

class EnvironmentExtractionFailException() extends Exception

object EnvironmentExtractionPipeline extends ScioApp[Args] {

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
  def extractionFiltersGenerator(args: Args): List[FilterDirective] = {
    val dateFilters: List[FilterDirective] = {
      List(FilterDirective("baseline_complete", FilterOps.==, "2")) ++
        args.startTime
          .map(start =>
            List(
              FilterDirective("bl_dap_pack_date", FilterOps.>, RedCapClient.redcapFormatDate(start))
            )
          )
          .getOrElse(List()) ++
        args.endTime
          .map(end =>
            List(FilterDirective("bl_dap_pack_date", FilterOps.<, RedCapClient.redcapFormatDate(end)))
          )
          .getOrElse(List())
    }
    dateFilters
  }

  val subdir = "environment"

  // Utilizing the custom date formatter since June, July, and Sept have non standard abbreviations
  val monthMap: java.util.Map[java.lang.Long, String] = Map(
    Long.box(1L) -> "Jan",
    Long.box(2L) -> "Feb",
    Long.box(3L) -> "Mar",
    Long.box(4L) -> "Apr",
    Long.box(5L) -> "May",
    Long.box(6L) -> "June",
    Long.box(7L) -> "July",
    Long.box(8L) -> "Aug",
    Long.box(9L) -> "Sept",
    Long.box(10L) -> "Oct",
    Long.box(11L) -> "Nov",
    Long.box(12L) -> "Dec"
  ).asJava

  val formatter = new DateTimeFormatterBuilder()
    .appendText(ChronoField.MONTH_OF_YEAR, monthMap)
    .appendPattern("yyyy")
    .toFormatter()

  // get list of individual dates, then get the set of years and return a list of distinct monthYears
  def getMonthYearList(start: OffsetDateTime, end: OffsetDateTime): List[String] = {
    if (start.isAfter(end)) throw new EnvironmentExtractionFailException
    val dateList =
      (Iterator.iterate(start)(_ plusDays 1) takeWhile (_ isBefore end.plusDays(1))).toList
    dateList
      .map(date => date.format(formatter).toLowerCase)
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
