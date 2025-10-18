package io.snowflake4s.jdbc

import io.snowflake4s.SnowflakeError.DecodingError
import java.sql.ResultSet
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, ZonedDateTime}
import java.util.UUID

/** Typeclass for decoding a single column value from a ResultSet.
  *
  * @tparam A
  *   The target type to decode to
  */
trait Get[A] {
  def get(rs: ResultSet, index: Int): Either[DecodingError, A]

  final def map[B](f: A => B): Get[B] = Get.instance { (rs, idx) =>
    get(rs, idx).map(f)
  }

  final def emap[B](f: A => Either[String, B]): Get[B] = Get.instance { (rs, idx) =>
    get(rs, idx).flatMap(a => f(a).left.map(DecodingError.Custom(_)))
  }
}

object Get {

  def apply[A](implicit ev: Get[A]): Get[A] = ev

  def instance[A](f: (ResultSet, Int) => Either[DecodingError, A]): Get[A] =
    (rs: ResultSet, index: Int) => f(rs, index)

  // Primitives

  implicit val int: Get[Int] = instance { (rs, idx) =>
    safeGet(rs, idx, "Int")(_.getInt(idx))
  }

  implicit val long: Get[Long] = instance { (rs, idx) =>
    safeGet(rs, idx, "Long")(_.getLong(idx))
  }

  implicit val double: Get[Double] = instance { (rs, idx) =>
    safeGet(rs, idx, "Double")(_.getDouble(idx))
  }

  implicit val float: Get[Float] = instance { (rs, idx) =>
    safeGet(rs, idx, "Float")(_.getFloat(idx))
  }

  implicit val boolean: Get[Boolean] = instance { (rs, idx) =>
    safeGet(rs, idx, "Boolean")(_.getBoolean(idx))
  }

  implicit val string: Get[String] = instance { (rs, idx) =>
    try {
      val value = rs.getString(idx)
      if (value == null || rs.wasNull()) {
        Left(DecodingError.NullValue(idx, "String"))
      } else {
        Right(value)
      }
    } catch {
      case ex: java.sql.SQLException =>
        Left(DecodingError.TypeMismatch(idx, "unknown", "String", ex.getMessage))
      case ex: Exception =>
        Left(DecodingError.Custom(s"Unexpected error decoding String at column $idx: ${ex.getMessage}"))
    }
  }

  implicit val bigDecimal: Get[BigDecimal] = instance { (rs, idx) =>
    try {
      val bd = rs.getBigDecimal(idx)
      if (bd == null || rs.wasNull()) {
        Left(DecodingError.NullValue(idx, "BigDecimal"))
      } else {
        Right(BigDecimal(bd))
      }
    } catch {
      case ex: java.sql.SQLException =>
        Left(DecodingError.TypeMismatch(idx, "unknown", "BigDecimal", ex.getMessage))
      case ex: Exception =>
        Left(DecodingError.Custom(s"Unexpected error decoding BigDecimal at column $idx: ${ex.getMessage}"))
    }
  }

  implicit val byteArray: Get[Array[Byte]] = instance { (rs, idx) =>
    try {
      val value = rs.getBytes(idx)
      if (value == null || rs.wasNull()) {
        Left(DecodingError.NullValue(idx, "Array[Byte]"))
      } else {
        Right(value)
      }
    } catch {
      case ex: java.sql.SQLException =>
        Left(DecodingError.TypeMismatch(idx, "unknown", "Array[Byte]", ex.getMessage))
      case ex: Exception =>
        Left(DecodingError.Custom(s"Unexpected error decoding Array[Byte] at column $idx: ${ex.getMessage}"))
    }
  }

  // java.time types

  implicit val instant: Get[Instant] = instance { (rs, idx) =>
    try {
      val ts = rs.getTimestamp(idx)
      if (ts == null || rs.wasNull()) {
        Left(DecodingError.NullValue(idx, "Instant"))
      } else {
        Right(ts.toInstant)
      }
    } catch {
      case ex: java.sql.SQLException =>
        Left(DecodingError.TypeMismatch(idx, "unknown", "Instant", ex.getMessage))
      case ex: Exception =>
        Left(DecodingError.Custom(s"Unexpected error decoding Instant at column $idx: ${ex.getMessage}"))
    }
  }

  implicit val localDate: Get[LocalDate] = instance { (rs, idx) =>
    try {
      val d = rs.getDate(idx)
      if (d == null || rs.wasNull()) {
        Left(DecodingError.NullValue(idx, "LocalDate"))
      } else {
        Right(d.toLocalDate)
      }
    } catch {
      case ex: java.sql.SQLException =>
        Left(DecodingError.TypeMismatch(idx, "unknown", "LocalDate", ex.getMessage))
      case ex: Exception =>
        Left(DecodingError.Custom(s"Unexpected error decoding LocalDate at column $idx: ${ex.getMessage}"))
    }
  }

  implicit val localDateTime: Get[LocalDateTime] = instance { (rs, idx) =>
    try {
      val ts = rs.getTimestamp(idx)
      if (ts == null || rs.wasNull()) {
        Left(DecodingError.NullValue(idx, "LocalDateTime"))
      } else {
        Right(ts.toLocalDateTime)
      }
    } catch {
      case ex: java.sql.SQLException =>
        Left(DecodingError.TypeMismatch(idx, "unknown", "LocalDateTime", ex.getMessage))
      case ex: Exception =>
        Left(DecodingError.Custom(s"Unexpected error decoding LocalDateTime at column $idx: ${ex.getMessage}"))
    }
  }

  implicit val localTime: Get[LocalTime] = instance { (rs, idx) =>
    try {
      val t = rs.getTime(idx)
      if (t == null || rs.wasNull()) {
        Left(DecodingError.NullValue(idx, "LocalTime"))
      } else {
        Right(t.toLocalTime)
      }
    } catch {
      case ex: java.sql.SQLException =>
        Left(DecodingError.TypeMismatch(idx, "unknown", "LocalTime", ex.getMessage))
      case ex: Exception =>
        Left(DecodingError.Custom(s"Unexpected error decoding LocalTime at column $idx: ${ex.getMessage}"))
    }
  }

  implicit val zonedDateTime: Get[ZonedDateTime] = instance { (rs, idx) =>
    try {
      val ts = rs.getTimestamp(idx)
      if (ts == null || rs.wasNull()) {
        Left(DecodingError.NullValue(idx, "ZonedDateTime"))
      } else {
        Right(ZonedDateTime.ofInstant(ts.toInstant, java.time.ZoneId.systemDefault()))
      }
    } catch {
      case ex: java.sql.SQLException =>
        Left(DecodingError.TypeMismatch(idx, "unknown", "ZonedDateTime", ex.getMessage))
      case ex: Exception =>
        Left(DecodingError.Custom(s"Unexpected error decoding ZonedDateTime at column $idx: ${ex.getMessage}"))
    }
  }

  implicit val offsetDateTime: Get[OffsetDateTime] = instance { (rs, idx) =>
    try {
      val ts = rs.getTimestamp(idx)
      if (ts == null || rs.wasNull()) {
        Left(DecodingError.NullValue(idx, "OffsetDateTime"))
      } else {
        Right(OffsetDateTime.ofInstant(ts.toInstant, java.time.ZoneId.systemDefault()))
      }
    } catch {
      case ex: java.sql.SQLException =>
        Left(DecodingError.TypeMismatch(idx, "unknown", "OffsetDateTime", ex.getMessage))
      case ex: Exception =>
        Left(DecodingError.Custom(s"Unexpected error decoding OffsetDateTime at column $idx: ${ex.getMessage}"))
    }
  }

  // UUID

  implicit val uuid: Get[UUID] = instance { (rs, idx) =>
    try {
      val str = rs.getString(idx)
      if (str == null || rs.wasNull()) {
        Left(DecodingError.NullValue(idx, "UUID"))
      } else {
        Right(UUID.fromString(str))
      }
    } catch {
      case ex: java.sql.SQLException =>
        Left(DecodingError.TypeMismatch(idx, "unknown", "UUID", ex.getMessage))
      case ex: Exception =>
        Left(DecodingError.Custom(s"Unexpected error decoding UUID at column $idx: ${ex.getMessage}"))
    }
  }

  // Option

  implicit def option[A](implicit get: Get[A]): Get[Option[A]] = instance { (rs, idx) =>
    get.get(rs, idx) match {
      case Right(value)                     => Right(Some(value))
      case Left(_: DecodingError.NullValue) => Right(None)
      case Left(err)                        => Left(err)
    }
  }

  // Helper functions

  private def safeGet[A](rs: ResultSet, idx: Int, typeName: String)(f: ResultSet => A): Either[DecodingError, A] =
    decodeCatch(idx, typeName) {
      val value = f(rs)
      if (rs.wasNull()) Left(DecodingError.NullValue(idx, typeName)) else Right(value)
    }

  private def decodeCatch[A](idx: Int, typeName: String)(thunk: => Either[DecodingError, A]): Either[DecodingError, A] =
    try thunk
    catch {
      case ex: java.sql.SQLException => Left(DecodingError.TypeMismatch(idx, "unknown", typeName, ex.getMessage))
      case ex: Exception =>
        Left(DecodingError.Custom(s"Unexpected error decoding $typeName at column $idx: ${ex.getMessage}"))
    }
}
