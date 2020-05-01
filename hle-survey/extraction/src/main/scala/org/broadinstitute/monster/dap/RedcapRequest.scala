package org.broadinstitute.monster.dap

import java.time.OffsetDateTime

sealed trait RedcapRequest

/**
  * NOTE: Record-level filters are combined with AND-ing logic. Field-level
  * filters are combined with OR-ing logic. For hitting the records endpoint.
  *
  * @param ids IDs of the specific records to download. If not set, all
  *            records will be downloaded
  *
  * @param fields  subset of fields to download. If not set, all fields
  *                will be downloaded
  * @param forms   subset of forms to download. If not set, fields from all
  *                forms will be downloaded
  * @param start   if given, only records created-or-updated at or after this
  *                time will be downloaded
  * @param end     if given, only records created-or-updated before or at this
  *                time will be downloaded
  * @param filters arbitrary field-value pairs to use as an exact-match
  *                filter on downloaded records
  **/
case class GetRecords(
  ids: List[String] = Nil,
  fields: List[String] = Nil,
  forms: List[String] = Nil,
  start: Option[OffsetDateTime] = None,
  end: Option[OffsetDateTime] = None,
  filters: Map[String, String] = Map.empty
) extends RedcapRequest

/**
  * For hitting the data dictionary / metadata endpoint.
  *
  * @param instrument name specifying specific data collection instrument
  *                   for which you wish to pull metadata
  */
case class GetDataDictionary(instrument: String) extends RedcapRequest
