package org.broadinstitute.monster.dap

import java.io.StringWriter

import com.spotify.scio.values.SCollection

import io.circe._
import io.circe.parser._

import com.opencsv.CSVWriter

object TsvUtils {

  def jsonToTsv(
    messages: SCollection[String],
    headers: Seq[String],
    jsonToTsvRowTransform: JsonObject => JsonObject,
    description: String,
    outputPrefix: String
  ) =
    messages
      .transform(s"Convert '$description' JSON to TSV rows")(
        _.map(row => jsonStringToTsvString(row, headers, jsonToTsvRowTransform))
      )
      .withName(s"Write '$description' TSV messages to '$outputPrefix'")
      .saveAsTextFile(
        outputPrefix,
        suffix = ".tsv",
        numShards = 0,
        header = Some(headers.mkString("\t"))
      )

  def stringSeqToTsvRow(cells: Iterable[String]): String = {
    val rowBuilder: StringWriter = new StringWriter()
    val writer = new CSVWriter(rowBuilder, '\t', '"', '"', "\n")
    // the second argument tells the writer how to handle quoting.
    // if it's false, it will only wrap cells containing quote/newline/separator characters in quotes.
    writer.writeNext(cells.toArray, false)

    rowBuilder.toString
  }

  // presumes that the element is not itself a json object (i.e. json objects we map this function across must be flat)
  def jsonElementToString(element: Json): String = {
    element match {
      case str if str.isString    => str.asString.get
      case bool if bool.isBoolean => bool.asBoolean.get.toString
      case num if num.isNumber => {
        val numberized: JsonNumber = num.asNumber.get
        numberized.toLong.map(_.toString).getOrElse(numberized.toDouble.toString)
      }
      case nullValue if nullValue.isNull => ""
    }
  }

  def jsonStringToTsvString(
    jsonStr: String,
    headers: Seq[String],
    rowTransform: JsonObject => JsonObject
  ): String = {
    val transformedJsonObject: JsonObject = parse(jsonStr).fold(
      err => throw new Exception(s"Failed to parse input line as JSON: $jsonStr", err),
      json => rowTransform(json.asObject.get)
    )

    val jsonRow = transformedJsonObject.toMap.mapValues(jsonElementToString(_))

    stringSeqToTsvRow(headers.map(header => jsonRow.getOrElse(header, "")))
  }
}
