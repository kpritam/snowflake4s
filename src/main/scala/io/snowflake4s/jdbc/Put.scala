package io.snowflake4s.jdbc

import io.snowflake4s.SnowflakeError.QueryError
import java.sql.{PreparedStatement, Types}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.UUID

/** Typeclass for encoding a value as a PreparedStatement parameter.
  *
  * @tparam A
  *   The source type to encode from
  */
trait Put[A] {
  def put(ps: PreparedStatement, index: Int, value: A): Either[QueryError, Unit]

  final def contramap[B](f: B => A): Put[B] = Put.instance { (ps, idx, b) =>
    put(ps, idx, f(b))
  }
}

object Put {

  def apply[A](implicit ev: Put[A]): Put[A] = ev

  def instance[A](f: (PreparedStatement, Int, A) => Either[QueryError, Unit]): Put[A] =
    (ps: PreparedStatement, index: Int, value: A) => f(ps, index, value)

  private def safePut[A](ps: PreparedStatement, idx: Int, value: A)(f: => Unit): Either[QueryError, Unit] = {
    try {
      f
      Right(())
    } catch {
      case ex: Exception =>
        Left(QueryError.ParameterBindingError(idx, ex.getMessage))
    }
  }

  // Primitives

  implicit val int: Put[Int] = instance { (ps, idx, v) =>
    safePut(ps, idx, v)(ps.setInt(idx, v))
  }

  implicit val long: Put[Long] = instance { (ps, idx, v) =>
    safePut(ps, idx, v)(ps.setLong(idx, v))
  }

  implicit val double: Put[Double] = instance { (ps, idx, v) =>
    safePut(ps, idx, v)(ps.setDouble(idx, v))
  }

  implicit val float: Put[Float] = instance { (ps, idx, v) =>
    safePut(ps, idx, v)(ps.setFloat(idx, v))
  }

  implicit val boolean: Put[Boolean] = instance { (ps, idx, v) =>
    safePut(ps, idx, v)(ps.setBoolean(idx, v))
  }

  implicit val string: Put[String] = instance { (ps, idx, v) =>
    safePut(ps, idx, v)(ps.setString(idx, v))
  }

  implicit val bigDecimal: Put[BigDecimal] = instance { (ps, idx, v) =>
    safePut(ps, idx, v)(ps.setBigDecimal(idx, v.bigDecimal))
  }

  implicit val byteArray: Put[Array[Byte]] = instance { (ps, idx, v) =>
    safePut(ps, idx, v)(ps.setBytes(idx, v))
  }

  // java.time types

  implicit val instant: Put[Instant] = instance { (ps, idx, v) =>
    safePut(ps, idx, v)(ps.setTimestamp(idx, java.sql.Timestamp.from(v)))
  }

  implicit val localDate: Put[LocalDate] = instance { (ps, idx, v) =>
    safePut(ps, idx, v)(ps.setDate(idx, java.sql.Date.valueOf(v)))
  }

  implicit val localDateTime: Put[LocalDateTime] = instance { (ps, idx, v) =>
    safePut(ps, idx, v)(ps.setTimestamp(idx, java.sql.Timestamp.valueOf(v)))
  }

  implicit val localTime: Put[LocalTime] = instance { (ps, idx, v) =>
    safePut(ps, idx, v)(ps.setTime(idx, java.sql.Time.valueOf(v)))
  }

  // UUID

  implicit val uuid: Put[UUID] = instance { (ps, idx, v) =>
    safePut(ps, idx, v)(ps.setString(idx, v.toString))
  }

  // Option

  implicit def option[A](implicit put: Put[A]): Put[Option[A]] = instance { (ps, idx, opt) =>
    opt match {
      case Some(value) => put.put(ps, idx, value)
      case None        => safePut(ps, idx, None)(ps.setNull(idx, Types.NULL))
    }
  }
}
