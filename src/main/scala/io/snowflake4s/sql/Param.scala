package io.snowflake4s.sql

import io.snowflake4s.SnowflakeError.QueryError
import io.snowflake4s.jdbc.Put
import java.sql.PreparedStatement

/** A SQL query parameter that can be bound to a PreparedStatement. */
final case class Param(bind: (PreparedStatement, Int) => Either[QueryError, Unit])

object Param {

  /** Creates a parameter from a value.
    *
    * @param value
    *   the parameter value
    * @param put
    *   the encoder for the value type
    * @tparam A
    *   the value type
    * @return
    *   a new parameter
    */
  def apply[A](value: A)(implicit put: Put[A]): Param =
    new Param((ps, idx) => put.put(ps, idx, value))

  /** Creates a parameter from a value.
    *
    * @param value
    *   the parameter value
    * @param put
    *   the encoder for the value type
    * @tparam A
    *   the value type
    * @return
    *   a new parameter
    */
  def of[A](value: A)(implicit put: Put[A]): Param = apply(value)
}
