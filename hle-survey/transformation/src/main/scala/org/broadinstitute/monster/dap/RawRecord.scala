package org.broadinstitute.monster.dap

/**
  * Container for the raw properties pulled from RedCap for a single dog.
  *
  * @param id unique ID of the dog in the study
  * @param fields raw key-value attributes associated with the dog
  */
case class RawRecord(id: Long, fields: Map[String, Array[String]]) {

  /**
    * Get the singleton value for an attribute in this record.
    *
    * Raises an error if the attribute has no value in this record, or
    * if it has multiple values.
    */
  def getRequired(field: String): String = {
    val values = fields.getOrElse(field, Array.empty)
    if (values.length != 1) {
      throw new IllegalStateException(s"Record $id has more/less than 1 value for field $field")
    } else {
      values.head
    }
  }

  /**
    * Get the singleton value for an attribute in this record, if one exists.
    *
    * Raises an error if the attribute has multiple values.
    */
  def getOptional(field: String): Option[String] = {
    val values = fields.getOrElse(field, Array.empty)
    if (values.length > 1) {
      throw new IllegalStateException(s"Record $id has multiple values for field $field")
    } else {
      values.headOption
    }
  }

  /** Get the singleton value for an attribute in this record, parsed as a boolean. */
  def getBoolean(field: String): Boolean = getOptionalBoolean(field).getOrElse(false)

  /** Get the singleton value for an attribute in this record if one exists, parsed as a boolean. */
  def getOptionalBoolean(field: String): Option[Boolean] = getOptional(field).map(_ == "1")

  /** Get the singleton value for an attribute in this record, parsed as a long. */
  def getOptionalNumber(field: String): Option[Long] = getOptional(field).map(_.toLong)

  /** Get every value for an attribute in this record. */
  def getArray(field: String): Array[String] = fields.getOrElse(field, Array.empty)

  /** If the field exists, check whether it contains the value, otherwise return None. */
  def containsValue(field: String, value: String): Option[Boolean] =
    if (fields.contains(field)) Some(fields.getOrElse(field, Array.empty).contains(value)) else None
}
