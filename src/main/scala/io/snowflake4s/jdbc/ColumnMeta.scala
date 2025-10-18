package io.snowflake4s.jdbc

import io.snowflake4s.SnowflakeError.DecodingError

import java.sql.{ResultSet, ResultSetMetaData}
import scala.util.Try

/** Metadata about columns in a ResultSet. Provides efficient column name resolution and metadata lookup. */
final case class ColumnMeta(
    nameToIndex: Map[String, Int],
    indexToName: Map[Int, String],
    totalColumns: Int
) {

  def resolveColumn(name: String): Either[DecodingError, Int] = {
    nameToIndex.get(normalize(name)) match {
      case Some(idx) => Right(idx)
      case None      => Left(DecodingError.ColumnNotFound(name, indexToName.values.toList.sorted))
    }
  }

  def columnName(index: Int): Either[DecodingError, String] = {
    indexToName.get(index) match {
      case Some(name) => Right(name)
      case None       => Left(DecodingError.InvalidColumnIndex(index, totalColumns))
    }
  }

  private def normalize(name: String): String =
    camelToSnake(name).toLowerCase.replaceAll("[^a-z0-9]", "")

  private def camelToSnake(name: String): String =
    name.replaceAll("([a-z])([A-Z])", "$1_$2")
}

object ColumnMeta {

  def fromResultSet(rs: ResultSet): Either[DecodingError, ColumnMeta] = {
    Try {
      val meta: ResultSetMetaData = rs.getMetaData
      val count = meta.getColumnCount

      val nameMap = (1 to count).map { idx =>
        val name = meta.getColumnLabel(idx)
        normalize(name) -> idx
      }.toMap

      val indexMap = (1 to count).map { idx =>
        idx -> meta.getColumnLabel(idx)
      }.toMap

      ColumnMeta(nameMap, indexMap, count)
    }.toEither.left.map(ex => DecodingError.Custom(s"Failed to extract column metadata: ${ex.getMessage}"))
  }

  private def normalize(name: String): String =
    camelToSnake(name).toLowerCase.replaceAll("[^a-z0-9]", "")

  private def camelToSnake(name: String): String =
    name.replaceAll("([a-z])([A-Z])", "$1_$2")
}
