package org.broadinstitute.monster.dap

import scala.NotImplementedError
import scala.util.Try

object TsvUtils {
  import purecsv.safe.converter.StringConverter
  import purecsv.safe.converter.StringConverterUtils.mkStringConverter

  // helper to let us serialize nested case classes
  def mkOneWayCaseClassSerializer[A](serializer: A => String): StringConverter[A] =
    mkStringConverter(
      _ => Try(throw new NotImplementedError("Attempted to parse unparseable type.")),
      serializer
    )
}
