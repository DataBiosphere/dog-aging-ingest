package org.broadinstitute.monster.dap

import io.circe.parser._

import com.spotify.scio.values.SCollection

import purecsv.safe._

import org.broadinstitute.monster.common.StorageIO

trait TsvUtils {

  // override to add columns to map or otherwise alter values
  def buildTsvMapFromJson(json: String) = decode[Map[String, Any]](json)

  def jsonToTsv(messages: SCollection[String], description: String, outputPrefix: String) =
    StorageIO.writeListsCommon[String](
      messages,
      m => buildTsvMapFromJson(m).toCSV("\t"),
      description,
      outputPrefix,
      ".tsv"
    )
}
