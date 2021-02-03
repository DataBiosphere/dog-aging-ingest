package org.broadinstitute.monster.dap

import com.spotify.scio.values.SCollection

import io.circe.syntax._
import io.circe.{JsonObject, Encoder}

import org.broadinstitute.monster.common.StorageIO

object ScioUtils {

  def jsonSerializeTransform[M: Encoder](messages: SCollection[M], description: String) = {
    messages.transform(s"Convert '$description' messages to JSON")(
      _.map((msg: M) => msg.asJson.printWith(StorageIO.circePrinter))
    )
  }

  def toJsonAndTsv[M: Encoder](
    messages: SCollection[M],
    description: String,
    headers: Seq[String],
    rowTransform: JsonObject => JsonObject,
    outputPrefix: String
  ) = {
    val jsonStrings = jsonSerializeTransform(messages, description)

    jsonStrings
      .withName(s"Write '$description' messages to '$outputPrefix'")
      .saveAsTextFile(outputPrefix, suffix = ".json", numShards = 0)

    TsvUtils.jsonToTsv(
      jsonStrings,
      headers,
      rowTransform,
      s"$description TSV",
      s"$outputPrefix/tsv"
    )
  }
}
