package io.snowflake4s.core

import io.snowflake4s.SnowflakeError.{DecodingError, QueryError}
import io.snowflake4s.jdbc.{ColumnMeta, Read}
import io.snowflake4s.sql.Param

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import scala.annotation.tailrec

/** Low-level query execution logic. */
object QueryExecutor {

  final case class QueryResult[T](rows: List[T], queryId: Option[String])
  final case class UpdateResult(updated: Int, queryId: Option[String])
  final case class BatchResult(updated: List[Int], queryId: Option[String])

  /**
   * Executes a query and returns the results.
   *
   * @param connection the database connection
   * @param sql the SQL query string
   * @param params the query parameters
   * @param read the decoder for the result type
   * @tparam T the result type
   * @return the query result or an error
   */
  def executeQuery[T](
      connection: Connection,
      sql: String,
      params: Seq[Param]
  )(implicit read: Read[T]): Either[QueryError, QueryResult[T]] = {
    prepareStatement(connection, sql, params).flatMap { stmt =>
      try {
        val queryId = extractQueryId(stmt)
        executeResultSet(stmt) { rs =>
          ColumnMeta.fromResultSet(rs).left.map(err => toQueryError(err, extractQueryId(stmt))).flatMap { meta =>
            collectResults(stmt, rs, meta, read).map(rows => QueryResult(rows, queryId))
          }
        }
      } finally {
        stmt.close()
      }
    }
  }

  /**
   * Executes an update command.
   *
   * @param connection the database connection
   * @param sql the SQL command string
   * @param params the command parameters
   * @return the update result or an error
   */
  def executeUpdate(connection: Connection, sql: String, params: Seq[Param]): Either[QueryError, UpdateResult] = {
    prepareStatement(connection, sql, params).flatMap { stmt =>
      try executeUpdateStatement(stmt).map(count => UpdateResult(count, extractQueryId(stmt)))
      finally stmt.close()
    }
  }

  /**
   * Executes a batch of commands.
   *
   * @param connection the database connection
   * @param sql the SQL command string
   * @param batches the list of parameter batches
   * @return the batch result or an error
   */
  def executeBatch(
      connection: Connection,
      sql: String,
      batches: List[Seq[Param]]
  ): Either[QueryError, BatchResult] = {
    prepareStatement(connection, sql, Seq.empty).flatMap { stmt =>
      try {
        val added = batches.foldLeft[Either[QueryError, Unit]](Right(())) { (acc, params) =>
          acc.flatMap { _ =>
            bindParameters(stmt, params).map { _ =>
              stmt.addBatch()
              stmt.clearParameters()
            }
          }
        }
        added.flatMap { _ =>
          catching(stmt) {
            Right(BatchResult(stmt.executeBatch().toList, extractQueryId(stmt)))
          }
        }
      } finally {
        stmt.close()
      }
    }
  }

  private def prepareStatement(
      connection: Connection,
      sql: String,
      params: Seq[Param]
  ): Either[QueryError, PreparedStatement] = {
    try {
      val stmt = connection.prepareStatement(sql)
      applyStatementSettings(connection, stmt)
      bindParameters(stmt, params).map(_ => stmt)
    } catch {
      case ex: SQLException =>
        Left(QueryError.SqlException(Option(ex.getSQLState), ex.getErrorCode, ex.getMessage, None, Some(ex)))
      case ex: Exception =>
        Left(QueryError.PreparedStatementError(ex.getMessage, Some(ex)))
    }
  }

  private def bindParameters(
      stmt: PreparedStatement,
      params: Seq[Param]
  ): Either[QueryError, Unit] = {
    params.zipWithIndex.foldLeft[Either[QueryError, Unit]](Right(())) { case (acc, (param, idx)) =>
      acc.flatMap(_ => param.bind(stmt, idx + 1))
    }
  }

  private def executeResultSet[A](
      stmt: PreparedStatement
  )(f: ResultSet => Either[QueryError, A]): Either[QueryError, A] = {
    catching(stmt) {
      val rs = stmt.executeQuery()
      try f(rs)
      finally rs.close()
    }
  }

  private def executeUpdateStatement(stmt: PreparedStatement): Either[QueryError, Int] =
    catching(stmt) { Right(stmt.executeUpdate()) }

  private def collectResults[T](
      stmt: PreparedStatement,
      rs: ResultSet,
      meta: ColumnMeta,
      read: Read[T]
  ): Either[QueryError, List[T]] = {
    val results = List.newBuilder[T]

    @tailrec
    def loop(): Either[QueryError, List[T]] = {
      if (rs.next()) read.read(rs, meta).left.map(err => toQueryError(err, extractQueryId(stmt))) match {
        case Right(value) =>
          results += value
          loop()
        case Left(err) => Left(err)
      }
      else Right(results.result())
    }

    catching(stmt) { loop() }
  }

  private def toQueryError(err: DecodingError, queryId: Option[String]): QueryError =
    QueryError.ResultSetError(err.message, queryId)

  private def applyStatementSettings(connection: Connection, stmt: PreparedStatement): Unit = {
    setIntClientInfo(connection, "queryTimeout", stmt.setQueryTimeout)
    setIntClientInfo(connection, "fetchSize", stmt.setFetchSize)
  }

  private def setIntClientInfo(connection: Connection, key: String, apply: Int => Unit): Unit = {
    try {
      val value = connection.getClientInfo(key)
      if (value != null) {
        apply(value.toInt)
      }
    } catch {
      case _: Exception => ()
    }
  }

  private def extractQueryId(stmt: PreparedStatement): Option[String] = {
    try {
      Option(stmt.unwrap(classOf[net.snowflake.client.jdbc.SnowflakeStatement]).getQueryID)
    } catch {
      case _: Exception => None
    }
  }

  // Common exception handling for JDBC operations tied to a PreparedStatement.
  private def catching[A](stmt: PreparedStatement)(thunk: => Either[QueryError, A]): Either[QueryError, A] = {
    try thunk
    catch {
      case ex: SQLException =>
        Left(
          QueryError.SqlException(
            sqlState = Option(ex.getSQLState),
            errorCode = ex.getErrorCode,
            msg = ex.getMessage,
            queryId = extractQueryId(stmt),
            cause = Some(ex)
          )
        )
      case ex: Exception =>
        Left(QueryError.ResultSetError(ex.getMessage, extractQueryId(stmt), Some(ex)))
    }
  }
}
