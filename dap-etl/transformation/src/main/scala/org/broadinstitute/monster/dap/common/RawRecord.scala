package org.broadinstitute.monster.dap.common

import org.broadinstitute.monster.dap.HLESurveyTransformationPipelineBuilder.logger
import org.broadinstitute.monster.dap.TruncatedDecimalError
import org.broadinstitute.monster.dap.common.RawRecord.DAPDateTimeFormatter

import java.time.LocalDate
import java.time.format.DateTimeFormatter

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
  def getRequired(field: String, permittedValues: Set[String] = Set()): String = {
    getOptional(field, permittedValues) match {
      case Some(value: String) => value
      case None =>
        throw new IllegalStateException(s"Record $id is missing a value for required field $field")
    }
  }

  /** Get the singleton value for an attribute in this record, parsed as a boolean. */
  def getRequiredBoolean(field: String): Boolean = getRequired(field) == "1"

  /**
    * Get the singleton value for an attribute in this record, if one exists.
    *
    * Raises an error if the attribute has multiple values.
    *
    * Only unique duplicates will throw an error
    * If we receive multiple fields with the same value, the head of the set is used
    *
    * If permittedValues is provided, raises an error if the value provided is not in the provided list.
    * Optional values not present in the data are not validated against permittedValues.
    * An empty set means that all possible values are permitted (a question with no permitted answers would be silly)
    */
  def getOptional(field: String, permittedValues: Set[String] = Set()): Option[String] = {
    val values = fields.getOrElse(field, Array.empty).toSet
    // If there are multiple values
    if (values.--(Array("NA")).size > 1) {
      throw new IllegalStateException(s"Record $id has multiple values for field $field")
    }

    val toReturn = values.headOption

    val validValue: Boolean =
      permittedValues.isEmpty || toReturn.map(permittedValues.contains(_)).getOrElse(true)
    if (!validValue) {
      throw new IllegalStateException(s"Record $id has invalid value '$toReturn' for field $field")
    }

    toReturn
  }

  val sequencesToStrip = List("\r?\n", "\t")

  def getOptionalStripped(field: String): Option[String] = {
    getOptional(field).map(fieldValue =>
      sequencesToStrip.fold(fieldValue) { (processedFieldValue, sequence) =>
        processedFieldValue.replaceAll(sequence, " ")
      }
    )
  }

  /** Get the singleton value for an attribute in this record, parsed as a boolean. */
  def getBoolean(field: String): Boolean = getOptionalBoolean(field).getOrElse(false)

  /** Get the singleton value for an attribute in this record if one exists, parsed as a boolean. */
  def getOptionalBoolean(field: String): Option[Boolean] = getOptional(field).map(_ == "1")

  /** Get the singleton value for an attribute in this record, parsed as a long. */
  def getOptionalNumber(
    field: String,
    truncateDecimals: Boolean = true,
    permittedValues: Set[Long] = Set()
  ): Option[Long] =
    getOptional(field, permittedValues.map(_.toString)).map(value => {
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
