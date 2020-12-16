package org.broadinstitute.monster.dap

import upack._

object RedcapMsgGenerator {

  def redcapifyRecords(records: List[Map[String, String]]): Arr = {
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
