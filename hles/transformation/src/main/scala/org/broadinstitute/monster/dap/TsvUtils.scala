package org.broadinstitute.monster.dap

// import java.io.File

// import kantan.csv.CsvConfiguration.rfc
// import kantan.csv.ops._
import kantan.csv._

trait TsvUtils[T <: Product] {
  val terraTsvHeaders: List[String]

  def buildTsvRow(record: T): List[String]

  // def writeToTsv(file: File): Unit = {
  //   val writer =
  //     file.asCsvWriter[List[String]](rfc.withCellSeparator('\t').withHeader(terraTsvHeaders: _*))
  // }

  def getFieldNames(caseClass: T): List[String] = {
    caseClass.getClass.getDeclaredFields.map(_.getName).toList
  }

  def getFieldValues(caseClass: T): List[Any] = {
    caseClass.productIterator.toList
  }

  // todo figure out how to map across key/value pairs
  def getSerializedFieldMapping(caseClass: T): List[String] = {
    getFieldValueMapping(caseClass).map(pair =>
      pair match {
        case (Class(fieldType), fieldValue) => {
          serialize(value = fieldValue.asInstanceOf[fieldType])
        }
      }
    )
  }

  def serialize[N](implicit encoder: CellEncoder[N], value: N) = encoder.encode(value)

  def getFieldValueMapping(caseClass: T): Map[Class[_], Any] = {
    caseClass.getClass.getDeclaredFields.toList.zip(caseClass.productIterator.toList).toMap
  }

}
