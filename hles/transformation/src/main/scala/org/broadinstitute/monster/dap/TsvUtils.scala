package org.broadinstitute.monster.dap

import java.io.File
import java.time.LocalDate

import kantan.csv.CsvConfiguration.rfc
import kantan.csv.generic._
import kantan.csv.ops._
import kantan.csv._

trait TsvUtils[T <: Product] {
  val terraTsvHeaders: List[String]

  def buildTsvRow(record: T): List[String]

  def writeToTsv(file: File, rows: Traversable[T]): Unit = {
    val writer =
      file.asCsvWriter[List[String]](rfc.withCellSeparator('\t').withHeader(terraTsvHeaders: _*))

    rows.foreach(writer.write(row => buildTsvRow(row)))

    writer.close
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
    getFieldValues(caseClass).map(fieldValue =>
      fieldValue match {
        case fieldInt: Int        => serialize[Int](value = fieldInt)
        case fieldStr: String     => serialize[String](value = fieldStr)
        case fieldDate: LocalDate => serialize[LocalDate](value = fieldDate)
      }
    )
  }

  def serialize[N](implicit encoder: CellEncoder[N], value: N) = encoder.encode(value)

}
