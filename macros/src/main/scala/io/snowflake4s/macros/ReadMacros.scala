package io.snowflake4s.macros

import scala.reflect.macros.blackbox

object ReadMacros {

  def deriveImpl[A: c.WeakTypeTag](c: blackbox.Context): c.Tree = {
    val bundle = new ReadMacroBundle[c.type](c)
    bundle.deriveFor[A]
  }

  private final class ReadMacroBundle[C <: blackbox.Context](val c: C) {
    import c.universe.*

    // (fieldName, columnName, fieldType, index)
    private type FieldMetadata = (TermName, String, Type, Int)

    def deriveFor[A: c.WeakTypeTag]: Tree = {
      val targetType = weakTypeOf[A].dealias
      val classSymbol = validateCaseClass(targetType)
      val primaryCtor = classSymbol.primaryConstructor.asMethod
      val companion = getCompanion(classSymbol)

      // Flatten to single param list
      val allParams = primaryCtor.paramLists.flatten
      validateParameters(allParams, targetType)

      val fieldMetadata = extractFieldMetadata(allParams, targetType)
      val fieldExtractions = buildFieldExtractions(fieldMetadata)
      val constructorCall = buildConstructorCall(companion, fieldMetadata)

      q"""
        _root_.io.snowflake4s.jdbc.Read.instance[$targetType] { (rs, meta) =>
          $fieldExtractions.map { values => $constructorCall }
        }
      """
    }

    private def validateCaseClass(tpe: Type): ClassSymbol = {
      val symbol = tpe.typeSymbol
      if (!symbol.isClass) abort(s"$tpe is not a class")
      val classSymbol = symbol.asClass
      if (!classSymbol.isCaseClass) abort(s"Can only derive Read for case classes, but $tpe is not a case class")
      classSymbol
    }

    private def getCompanion(classSymbol: ClassSymbol): Tree = c.internal.gen.mkAttributedRef(classSymbol.companion)

    private def validateParameters(params: List[Symbol], tpe: Type): Unit = {
      if (params.isEmpty) abort(s"Cannot derive Read for $tpe: no constructor parameters found")
      if (params.exists(_.isImplicit)) abort(s"Cannot derive Read for $tpe: implicit parameters are not supported")
      if (params.exists(_.asTerm.isByNameParam))
        abort(s"Cannot derive Read for $tpe: by-name parameters are not supported")
    }

    private def extractFieldMetadata(params: List[Symbol], targetType: Type): List[FieldMetadata] = {
      params.zipWithIndex.map { case (param, idx) =>
        val term = param.asTerm
        val fieldName = term.name
        val columnName = fieldName.decodedName.toString
        val fieldType = term.typeSignatureIn(targetType).dealias

        (fieldName, columnName, fieldType, idx)
      }
    }

    private def buildFieldExtractions(fieldMetadata: List[FieldMetadata]): Tree = {
      val emptyVector = q"_root_.scala.collection.immutable.Vector.empty[Any]"
      val initial = q"_root_.scala.Right($emptyVector)"

      fieldMetadata.foldLeft(initial) { case (acc, (_, columnName, fieldType, _)) =>
        val columnLiteral = Literal(Constant(columnName))
        val fieldTypeTree = TypeTree(fieldType)

        q"""
          $acc.flatMap { collectedValues =>
            meta.resolveColumn($columnLiteral)
              .flatMap { columnIndex =>
                _root_.io.snowflake4s.jdbc.Get[$fieldTypeTree]
                  .get(rs, columnIndex)
                  .map { value => 
                    collectedValues :+ value
                  }
              }
          }
        """
      }
    }

    private def buildConstructorCall(
        companion: Tree,
        fieldMetadata: List[FieldMetadata]
    ): Tree = {
      val applySelect = Select(companion, TermName("apply"))
      val args = fieldMetadata.map { case (_, _, fieldType, index) =>
        q"values($index).asInstanceOf[${TypeTree(fieldType)}]"
      }
      Apply(applySelect, args)
    }

    private def abort(message: String): Nothing = c.abort(c.enclosingPosition, message)

  }
}
