package io.snowflake4s.sql

import io.snowflake4s.SnowflakeError.QueryError
import io.snowflake4s.core.QueryExecutor
import io.snowflake4s.core.QueryExecutor.UpdateResult
import io.snowflake4s.jdbc.Put
import java.sql.Connection

/** Represents a SQL statement that does not return rows. */
final case class Command[A](fragment: Fragment, encoder: A => Vector[Param]) {

  /** Transforms the input type of the command.
    *
    * @param f
    *   the transformation function
    * @tparam B
    *   the new input type
    * @return
    *   a new command with transformed input
    */
  def contramap[B](f: B => A): Command[B] =
    Command(fragment, encoder.compose(f))

  /** Converts this command to a parameterless command.
    *
    * @return
    *   a Command0
    */
  def toCommand0: Command0 = Command0(fragment)

  /** Runs the command with the given arguments on a connection.
    *
    * @param args
    *   the command arguments
    * @param connection
    *   the database connection
    * @return
    *   the update result or an error
    */
  def run(args: A, connection: Connection): Either[QueryError, UpdateResult] =
    QueryExecutor.executeUpdate(connection, fragment.sql, encoder(args).toList)
}

object Command {

  /** Creates a command from a fragment.
    *
    * @param fragment
    *   the SQL fragment
    * @return
    *   a new command
    */
  def fromFragment(fragment: Fragment): Command[Unit] =
    Command(fragment, (_: Unit) => Vector.empty)

  /** Creates a command from SQL.
    *
    * @param sql
    *   the SQL string
    * @return
    *   a new command
    */
  def apply(sql: String): Command[Unit] = fromFragment(Fragment.const(sql))

  /** Creates a command with parameters.
    *
    * @param sql
    *   the SQL string
    * @param put
    *   the encoder for the parameter type
    * @tparam A
    *   the parameter type
    * @return
    *   a new command
    */
  def withParams[A](sql: String)(implicit put: Put[A]): Command[A] =
    Command(Fragment.const(sql), (value: A) => Vector(Param(value)))
}

/** Represents a parameterless SQL command. */
final case class Command0(fragment: Fragment) {

  /** Runs the command on a connection.
    *
    * @param connection
    *   the database connection
    * @return
    *   the update result or an error
    */
  def run(connection: Connection): Either[QueryError, UpdateResult] =
    QueryExecutor.executeUpdate(connection, fragment.sql, fragment.params.toList)
}
