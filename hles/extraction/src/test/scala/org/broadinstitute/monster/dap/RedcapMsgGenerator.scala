package org.broadinstitute.monster.dap

import upack._

object RedcapMsgGenerator {

  // Input records can simply be a list of maps from field names to their values
  //   e.g. List(Map("a" -> "b"))
  // This code will convert them to redcap format (and to upack data structures)
  //   e.g. Arr(Obj(Str("record") -> Str("1"), Str("field_name") -> Str("a"), Str("value") -> Str("b")))
  def toRedcapFormat(records: List[Map[String, String]]): Arr = {
    Arr(records.zipWithIndex.flatMap {
      case (record, i) =>
        record.map {
          case (k, v) =>
            Obj(
              Str("record") -> Str(i.toString),
              Str("field_name") -> Str(k),
              Str("value") -> Str(v)
            )
        }
    }: _*)
  }
}
