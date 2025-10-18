package io.snowflake4s.macros

import scala.reflect.macros.blackbox

object ReadMacros {
  
  def deriveImpl[A: c.WeakTypeTag](c: blackbox.Context): c.Tree = {
    import c.universe._
    
    val tpe = weakTypeOf[A]
    
    // Validate it's a case class
    val symbol = tpe.typeSymbol
    if (!symbol.isClass || !symbol.asClass.isCaseClass) {
      c.abort(c.enclosingPosition, s"Can only derive Read for case classes, but $tpe is not a case class")
    }
    
    val companion = tpe.typeSymbol.companion
    val fields = tpe.decls.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }.toList
    
    if (fields.isEmpty) {
      c.abort(c.enclosingPosition, s"Cannot derive Read for $tpe: no case accessor fields found")
    }
    
    // Generate field extraction - fold over fields to build a List of values
    val initial = q"_root_.scala.Right(_root_.scala.collection.immutable.List.empty[Any])"
    
    val fieldExtractions = fields.foldLeft(initial) { (acc, field) =>
      val fieldName = field.name.decodedName.toString
      val fieldType = field.returnType
      q"""
        $acc.flatMap { lst =>
          meta.resolveColumn($fieldName)
            .left.map(err => err: _root_.io.snowflake4s.SnowflakeError.DecodingError)
            .flatMap { colIdx =>
              _root_.io.snowflake4s.jdbc.Get[$fieldType].get(rs, colIdx).map(v => lst :+ v)
            }
        }
      """
    }
    
    // Generate constructor call
    val constructorParams = fields.indices.map { i =>
      q"values($i).asInstanceOf[${fields(i).returnType}]"
    }
    
    q"""
      _root_.io.snowflake4s.jdbc.Read.instance { (rs, meta) =>
        val result: _root_.scala.Either[_root_.io.snowflake4s.SnowflakeError.DecodingError, _root_.scala.collection.immutable.List[Any]] =
          $fieldExtractions
        
        result.map { values =>
          $companion(..$constructorParams)
        }
      }
    """
  }
}
