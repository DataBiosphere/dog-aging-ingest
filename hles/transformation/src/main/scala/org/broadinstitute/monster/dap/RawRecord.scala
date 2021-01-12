package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dap.RawRecord.DAPDateTimeFormatter

import java.lang.NumberFormatException
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.broadinstitute.monster.dap.HLESurveyTransformationPipelineBuilder.logger

/**
  * Container for the raw properties pulled from RedCap for a single dog.
  *
  * @param id unique ID of the dog in the study
  * @param fields raw key-value attributes associated with the dog
  */
case class RawRecord(id: Long, fields: Map[String, Array[String]]) {

  /** Get the raw value(s) paired with the given key, if any. */
  def get(field: String): Option[Array[String]] = fields.get(field)

  /**
    * Get the singleton value for an attribute in this record.
    *
    * Raises an error if the attribute has no value in this record, or
    * if it has multiple values.
    *
    * Adding some conditional logic here to collapse duplicate fields to a set
    * If the values are unique, it would error in the same manner it did before
    * If the recurring field contains the same value, the head of the set is used
    */
  def getRequired(field: String): String = {
    val values = fields.getOrElse(field, Array.empty)
    if (values.toSet.size != 1) {
      throw new IllegalStateException(s"Record $id has more/less than 1 value for field $field")
    } else {
      values.head
    }
  }

  /**
    * Get the singleton value for an attribute in this record, if one exists.
    *
    * Raises an error if the attribute has multiple values.
    *
    * Only unique duplicates will throw an error
    * If we receive multiple fields with the same value, the head of the set is used
    */
  def getOptional(field: String): Option[String] = {
    val values = fields.getOrElse(field, Array.empty).toSet
    // If there are multiple values
    if (values.size > 1) {
      // Filtering out "NA"
      if (values.--(Array("NA")).size > 1) {
        throw new IllegalStateException(s"Record $id has multiple values for field $field")
      } else {
        values.headOption
      }
    } else {
      values.headOption
    }
  }

  def getOptionalStripped(field: String): Option[String] = {
    getOptional(field).map(_.replaceAll("\r?\n", " "))
  }

  /** Get the singleton value for an attribute in this record, parsed as a boolean. */
  def getBoolean(field: String): Boolean = getOptionalBoolean(field).getOrElse(false)

  /** Get the singleton value for an attribute in this record if one exists, parsed as a boolean. */
  def getOptionalBoolean(field: String): Option[Boolean] = getOptional(field).map(_ == "1")

  /** Get the singleton value for an attribute in this record, parsed as a long. */
  def getOptionalNumber(field: String, truncateDecimals: Boolean = true): Option[Long] =
    getOptional(field).map(value => {
      try {
        value.toLong
      } catch {
        case e: NumberFormatException => {
          if (truncateDecimals) {
            // The conversion to double and then long is to handle scientific notation
            val truncatedValue: Long = value.toDouble.toLong

            // don't log this error message until after we've successfully converted the string to a long.
            // this avoids us logging this message erroneously if we were unable to parse the value,
            // e.g. because it was garbled nonsense
            TruncatedDecimalError(
              s"Record $id has an unexpected decimal value in field $field, truncated to integer"
            ).log

            truncatedValue
          } else {
            throw e
          }
        }
      }
    })

  /** Get the singleton value for an attribute in this record if one exists, parsed as a date. */
  def getOptionalDate(field: String): Option[LocalDate] =
    getOptional(field).map(LocalDate.parse(_, RawRecord.DateFormatter))

  def getOptionalDateTime(field: String): Option[LocalDate] = {
    get(field) match {
      case Some(datetimes) => {
        val filtered = datetimes.filter(!_.equals("NA"))
        val dates: Array[LocalDate] = filtered.map { dt =>
          LocalDate.parse(dt, DAPDateTimeFormatter)
        }
        if (dates.toSet.size > 1) {
          throw new RuntimeException(s"More than one date time for field ${field}")
        }
        Some(dates(0))
      }
      case None => None
    }
  }

  /** Get every value for an attribute in this record. */
  def getArray(field: String): Array[String] = fields.getOrElse(field, Array.empty)
}

object RawRecord {
  val DateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val DAPDateTimeFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS]")
}
