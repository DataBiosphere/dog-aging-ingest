package org.broadinstitute.monster.dap.common

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
  * @param filters arbitrary field-value pairs to use as an exact-match
  *                filter on downloaded records
  */
case class GetRecords(
  ids: List[String] = Nil,
  fields: List[String] = Nil,
  forms: List[String] = Nil,
  filters: List[FilterDirective] = List.empty,
  arm: Seq[String] = List.empty
) extends RedcapRequest

/**
  * For hitting the data dictionary / metadata endpoint.
  *
  * @param form name specifying specific data collection instrument
  *             for which you wish to pull metadata
  */
case class GetDataDictionary(form: String) extends RedcapRequest
