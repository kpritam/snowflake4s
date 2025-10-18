package io.snowflake4s

import io.snowflake4s.SnowflakeError.QueryError
import io.snowflake4s.core.{ConnectionProvider, QueryExecutor}
import io.snowflake4s.core.QueryExecutor.{QueryResult, UpdateResult}
import io.snowflake4s.sql.{Command0, Query0}

import java.sql.Connection

/** A session API for synchronous JDBC interactions. */
final class SnowflakeSession private (connection: Connection) {

  /**
   * Executes a query and returns the results.
   *
   * @param query the query to execute
   * @tparam A the type of the query result
   * @return the query results or an error
   */
  def query[A](query: Query0[A]): Either[SnowflakeError, QueryResult[A]] =
    QueryExecutor.executeQuery(connection, query.fragment.sql, query.fragment.params.toList)(query.decoder)

  /**
   * Executes a query and returns the results as a list.
   *
   * @param query the query to execute
   * @tparam A the type of the query result
   * @return the list of results or an error
   */
  def list[A](query: Query0[A]): Either[SnowflakeError, List[A]] =
    this.query(query).map(_.rows)

  /**
   * Executes a query and returns the first result as an option.
   *
   * @param query the query to execute
   * @tparam A the type of the query result
   * @return the first result or None, or an error
   */
  def option[A](query: Query0[A]): Either[SnowflakeError, Option[A]] =
    this.query(query).map(_.rows.headOption)

  /**
   * Executes a query and returns exactly one result.
   *
   * @param query the query to execute
   * @tparam A the type of the query result
   * @return the single result or an error
   */
  def unique[A](query: Query0[A]): Either[SnowflakeError, A] =
    this.query(query).flatMap { result =>
      result.rows match {
        case head :: Nil => Right(head)
        case Nil         => Left(QueryError.ResultSetError("Expected 1 row but query returned none", result.queryId))
        case _           => Left(QueryError.ResultSetError("Expected 1 row but query returned many", result.queryId))
      }
    }

  /**
   * Executes a query and returns the results as a lazy list.
   *
   * @param query the query to execute
   * @tparam A the type of the query result
   * @return the lazy list of results or an error
   */
  def stream[A](query: Query0[A]): Either[SnowflakeError, LazyList[A]] =
    this.query(query).map(_.rows.to(LazyList))

  /**
   * Executes a command that does not return results.
   *
   * @param command the command to execute
   * @return the update result or an error
   */
  def execute(command: Command0): Either[SnowflakeError, UpdateResult] =
    QueryExecutor.executeUpdate(connection, command.fragment.sql, command.fragment.params.toList)

  /**
   * Executes operations within a database transaction.
   *
   * @param f the function to execute within the transaction
   * @tparam A the return type of the function
   * @return the result of the function or an error
   */
  def transaction[A](
      f: SnowflakeSession => Either[SnowflakeError, A]
  ): Either[SnowflakeError, A] = {
    val originalAutoCommit = connection.getAutoCommit
    try {
      connection.setAutoCommit(false)
      f(this).flatMap { value =>
        try {
          connection.commit()
          Right(value)
        } catch {
          case ex: Exception =>
            connection.rollback()
            Left(QueryError.TransactionError("commit", ex.getMessage, Some(ex)))
        }
      }
    } catch {
      case ex: Exception =>
        connection.rollback()
        Left(QueryError.TransactionError("transaction", ex.getMessage, Some(ex)))
    } finally {
      connection.setAutoCommit(originalAutoCommit)
    }
  }
}

object SnowflakeSession {

  /**
   * Executes a function with a database session, managing the connection lifecycle.
   *
   * @param provider the connection provider
   * @param f the function to execute with the session
   * @tparam A the return type of the function
   * @return the result of the function or an error
   */
  def withSession[A](provider: ConnectionProvider)(
      f: SnowflakeSession => Either[SnowflakeError, A]
  ): Either[SnowflakeError, A] =
    provider.withConnection(conn => f(new SnowflakeSession(conn)))
}
