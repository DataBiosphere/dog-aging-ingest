package org.broadinstitute.monster.dap

import scala.reflect.runtime.universe.{Symbol, Type, TypeTag, typeOf, termNames}

object CaseClassInspector {

  def snakeCaseHeaderRow[T: TypeTag](delimiter: String = "\t"): String =
    snakeCaseHeaderList[T].mkString(delimiter)

  def snakeCaseHeaderList[T: TypeTag]: Seq[String] = snakeCaseHeaderList(typeOf[T])

  private def snakeCaseHeaderList(t: Type) =
    flattenParamsList(t).map(_.name.toString).map(snakify(_))

  private def flattenParamsList(t: Type): Seq[Symbol] = {
    caseClassParamsList(t).flatMap(expandParamsList(_))
  }

  private def caseClassParamsList(t: Type): Seq[Symbol] =
    t.decl(termNames.CONSTRUCTOR).asMethod.paramLists(0)

  private def expandParamsList(field: Symbol): Seq[Symbol] = {
    getNestedSymbols(field.info).getOrElse(Seq(field))
  }

  // courtesy of https://github.com/lift/framework/blob/master/core/util/src/main/scala/net/liftweb/util/StringHelpers.scala
  private def snakify(name: String) =
    name
      .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .replaceAll("([a-z\\d])([A-Z])", "$1_$2")
      .replaceAll("([a-z])(\\d)", "$1_$2")
      .toLowerCase

  private def getNestedSymbols(targetType: Type): Option[Seq[Symbol]] = {
    targetType match {
      case optional if optional <:< typeOf[Option[Any]] => getNestedSymbols(optional.typeArgs(0))
      case product if product <:< typeOf[Product]       => Some(flattenParamsList(product))
      case _                                            => None
    }
  }
}
