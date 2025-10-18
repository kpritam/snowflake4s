package io.snowflake4s.jdbc

import io.snowflake4s.SnowflakeError.DecodingError
import java.sql.ResultSet
import scala.language.experimental.macros

/** Typeclass for decoding an entire row from a ResultSet.
  *
  * @tparam A
  *   The target type to decode to
  */
trait Read[A] {
  def read(rs: ResultSet, meta: ColumnMeta): Either[DecodingError, A]

  final def map[B](f: A => B): Read[B] = Read.instance((rs, meta) => read(rs, meta).map(f))

  final def emap[B](f: A => Either[String, B]): Read[B] = Read.instance { (rs, meta) =>
    read(rs, meta).flatMap(a => f(a).left.map(DecodingError.Custom(_)))
  }
}

object Read {

  def apply[A](implicit ev: Read[A]): Read[A] = ev

  def instance[A](f: (ResultSet, ColumnMeta) => Either[DecodingError, A]): Read[A] =
    (rs: ResultSet, meta: ColumnMeta) => f(rs, meta)

  // Derive a Read instance using macros
  def derived[A]: Read[A] = macro io.snowflake4s.macros.ReadMacros.deriveImpl[A]

  // Single-column reads (primitive types)

  implicit val int: Read[Int] = instance((rs, _) => Get.int.get(rs, 1))

  implicit val long: Read[Long] = instance((rs, _) => Get.long.get(rs, 1))

  implicit val double: Read[Double] = instance((rs, _) => Get.double.get(rs, 1))

  implicit val float: Read[Float] = instance((rs, _) => Get.float.get(rs, 1))

  implicit val boolean: Read[Boolean] = instance((rs, _) => Get.boolean.get(rs, 1))

  implicit val string: Read[String] = instance((rs, _) => Get.string.get(rs, 1))

  implicit val bigDecimal: Read[BigDecimal] = instance((rs, _) => Get.bigDecimal.get(rs, 1))

  implicit val instant: Read[java.time.Instant] = instance((rs, _) => Get.instant.get(rs, 1))

  implicit val localDate: Read[java.time.LocalDate] = instance((rs, _) => Get.localDate.get(rs, 1))

  implicit val localDateTime: Read[java.time.LocalDateTime] = instance((rs, _) => Get.localDateTime.get(rs, 1))

  implicit val uuid: Read[java.util.UUID] = instance((rs, _) => Get.uuid.get(rs, 1))
}
