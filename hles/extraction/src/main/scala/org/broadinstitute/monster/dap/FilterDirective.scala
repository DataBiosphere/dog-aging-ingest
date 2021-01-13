package org.broadinstitute.monster.dap

sealed trait FilterOp {
  def op: String
}

object FilterOps {

  case object == extends FilterOp {
    def op: String = "="
  }

  case object > extends FilterOp {
    def op: String = ">"
  }

  case object < extends FilterOp {
    def op: String = "<"
  }
}

/**
  * Represents a typesafe REDCap filterLogic directive.
  * Their API expects directives to be of the form [field][op][value]
  */
case class FilterDirective(field: String, operation: FilterOp, comparand: String)
