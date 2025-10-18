package io.snowflake4s.sql

import io.snowflake4s.SnowflakeError.QueryError
import io.snowflake4s.core.QueryExecutor
import io.snowflake4s.core.QueryExecutor.QueryResult
import io.snowflake4s.jdbc.{Put, Read}
import java.sql.Connection

/** Represents SQL queries that return rows of a specific type. */
final case class Query[A, B](fragment: Fragment, encoder: A => Vector[Param], decoder: Read[B]) {

  /**
   * Transforms the input type of the query.
   *
   * @param f the transformation function
   * @tparam C the new input type
   * @return a new query with transformed input
   */
  def contramap[C](f: C => A): Query[C, B] = copy(encoder = encoder.compose(f))

  /**
   * Transforms the output type of the query.
   *
   * @param f the transformation function
   * @tparam C the new output type
   * @return a new query with transformed output
   */
  def map[C](f: B => C): Query[A, C] = copy(decoder = decoder.map(f))

  /**
   * Converts this query to a parameterless query.
   *
   * @return a Query0
   */
  def toQuery0: Query0[B] = Query0(fragment, decoder)

  /**
   * Runs the query with the given arguments on a connection.
   *
   * @param args the query arguments
   * @param connection the database connection
   * @return the query result or an error
   */
  def run(args: A, connection: Connection): Either[QueryError, QueryResult[B]] = {
    val params = encoder(args)
    QueryExecutor.executeQuery(connection, fragment.sql, params)(decoder)
  }
}

object Query {
  /**
   * Creates a query from a fragment.
   *
   * @param fragment the SQL fragment
   * @param read the decoder for the result type
   * @tparam B the result type
   * @return a new query
   */
  def fromFragment[B](fragment: Fragment)(implicit read: Read[B]): Query[Unit, B] =
    Query(fragment, (_: Unit) => Vector.empty, read)

  /**
   * Creates a simple query from SQL.
   *
   * @param sql the SQL string
   * @param read the decoder for the result type
   * @tparam B the result type
   * @return a new query
   */
  def simple[B](sql: String)(implicit read: Read[B]): Query[Unit, B] =
    fromFragment(Fragment.const(sql))

  /**
   * Creates a query with a single parameter.
   *
   * @param sql the SQL string
   * @param put the encoder for the parameter type
   * @param read the decoder for the result type
   * @tparam A the parameter type
   * @tparam B the result type
   * @return a new query
   */
  def withParam[A, B](sql: String)(implicit put: Put[A], read: Read[B]): Query[A, B] =
    Query(Fragment.const(sql), (a: A) => Vector(Param(a)), read)
}

/** Represents a parameterless SQL query. */
final case class Query0[A](fragment: Fragment, decoder: Read[A]) {

  /**
   * Changes the result type of the query.
   *
   * @param read the new decoder
   * @tparam B the new result type
   * @return a new Query0
   */
  def as[B](implicit read: Read[B]): Query0[B] = Query0(fragment, read)

  /**
   * Transforms the result of the query.
   *
   * @param f the transformation function
   * @tparam B the new result type
   * @return a new Query0
   */
  def map[B](f: A => B): Query0[B] = Query0(fragment, decoder.map(f))

  /**
   * Runs the query on a connection.
   *
   * @param connection the database connection
   * @return the query result or an error
   */
  def run(connection: Connection): Either[QueryError, QueryResult[A]] =
    QueryExecutor.executeQuery(connection, fragment.sql, fragment.params.toList)(decoder)

  /**
   * Runs the query and returns the first result as an option.
   *
   * @param connection the database connection
   * @return the first result or None, or an error
   */
  def option(connection: Connection): Either[QueryError, Option[A]] =
    run(connection).map(_.rows.headOption)

  /**
   * Runs the query and returns exactly one result.
   *
   * @param connection the database connection
   * @return the single result or an error
   */
  def unique(connection: Connection): Either[QueryError, A] =
    run(connection).flatMap { result =>
      result.rows match {
        case head :: Nil => Right(head)
        case Nil => Left(QueryError.ResultSetError("Expected exactly one row, but none returned", result.queryId))
        case _   => Left(QueryError.ResultSetError("Expected exactly one row, but multiple returned", result.queryId))
      }
    }
}
