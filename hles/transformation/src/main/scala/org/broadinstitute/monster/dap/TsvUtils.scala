package org.broadinstitute.monster.dap

import java.io.ByteArrayOutputStream
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import kantan.csv.CsvConfiguration.rfc
import kantan.csv.generic._
import kantan.csv.ops._
import kantan.csv._

trait TsvUtils[T <: Product] {
  val terraTsvHeaders: List[String]
  lazy val csvConfig = rfc.withCellSeparator('\t').withHeader(terraTsvHeaders: _*)

  def buildTsvRow(record: T): List[String]

  private final val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val intEncoder: CellEncoder[Int] = CellEncoder.from(_.toString)
  val longEncoder: CellEncoder[Long] = CellEncoder.from(_.toString)
  val strEncoder: CellEncoder[String] = CellEncoder.from(_.toString)
  val dateEncoder: CellEncoder[LocalDate] = CellEncoder.from(_.format(dateFormat))

  def serializeTsvRow(record: T): String = {
    val stream = new ByteArrayOutputStream()

    stream.asCsvWriter[List[String]](csvConfig).write(buildTsvRow(record))

    stream.toString
  }

  def getFieldNames(caseClass: T): List[String] = {
    caseClass.getClass.getDeclaredFields.map(_.getName).toList
  }

  def getFieldValues(caseClass: T): List[Any] = {
    caseClass.productIterator.toList
  }

  // we're forced to handle this type reflection/serialization ourselves because most
  // utilities for dynamic serialization rely on type safety via tuples,
  // and scala does not support tuples with more than 22 values (which encompasses several of our tables)
  def getSerializedFieldValues(caseClass: T): List[String] = {
    getFieldValues(caseClass).map(serializeField)
  }

  def serializeField(field: Any): String = {
    field match {
      case fieldInt: Int        => serialize[Int](intEncoder, fieldInt)
      case fieldLong: Long      => serialize[Long](longEncoder, fieldLong)
      case fieldStr: String     => serialize[String](strEncoder, fieldStr)
      case fieldDate: LocalDate => serialize[LocalDate](dateEncoder, fieldDate)
      case Some(value)          => serializeField(value)
      case None                 => ""
    }
  }

  def serialize[N](encoder: CellEncoder[N], value: N) = encoder.encode(value)

}
