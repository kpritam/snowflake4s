package io.snowflake4s

import com.zaxxer.hikari.HikariConfig
import io.snowflake4s.SnowflakeError.{ConnectionError, QueryError}
import io.snowflake4s.core.QueryExecutor.{BatchResult, QueryResult, UpdateResult}
import io.snowflake4s.core.{ConnectionProvider, QueryExecutor}
import io.snowflake4s.jdbc.Read
import io.snowflake4s.sql.{Command0, Param, Query0}

import java.sql.Connection
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps

/** Client for synchronous operations with Snowflake database. */
final class SnowflakeClient(provider: ConnectionProvider) {

  /**
   * Executes a query with the given SQL and parameters.
   *
   * @param sql the SQL query string
   * @param params the query parameters
   * @param read the reader for the result type
   * @tparam T the type of the query result
   * @return the query results or an error
   */
  def query[T](sql: String, params: Seq[Param] = Seq.empty)(implicit
      read: Read[T]
  ): Either[SnowflakeError, QueryResult[T]] =
    provider.withConnection(QueryExecutor.executeQuery(_, sql, params))

  /**
   * Executes a query object.
   *
   * @param query the query to execute
   * @tparam T the type of the query result
   * @return the query results or an error
   */
  def query[T](query: Query0[T]): Either[SnowflakeError, QueryResult[T]] =
    provider.withConnection(conn =>
      QueryExecutor.executeQuery(conn, query.fragment.sql, query.fragment.params.toList)(query.decoder)
    )

  /**
   * Executes a command with the given SQL and parameters.
   *
   * @param sql the SQL command string
   * @param params the command parameters
   * @return the update result or an error
   */
  def execute(sql: String, params: Seq[Param] = Seq.empty): Either[SnowflakeError, UpdateResult] =
    provider.withConnection(QueryExecutor.executeUpdate(_, sql, params))

  /**
   * Executes a command object.
   *
   * @param command the command to execute
   * @return the update result or an error
   */
  def execute(command: Command0): Either[SnowflakeError, UpdateResult] =
    provider.withConnection(conn =>
      QueryExecutor.executeUpdate(conn, command.fragment.sql, command.fragment.params.toList)
    )

  /**
   * Executes a batch of commands.
   *
   * @param sql the SQL command string
   * @param batches the list of parameter batches
   * @return the batch result or an error
   */
  def executeBatch(sql: String, batches: List[Seq[Param]]): Either[SnowflakeError, BatchResult] =
    provider.withConnection(QueryExecutor.executeBatch(_, sql, batches))

  /**
   * Executes operations within a transaction.
   *
   * @param f the function to execute within the transaction
   * @tparam A the return type of the function
   * @return the result of the function or an error
   */
  def withTransaction[A](f: SnowflakeClient.Transaction => Either[SnowflakeError, A]): Either[SnowflakeError, A] =
    provider.withConnection { conn =>
      val autoCommit = conn.getAutoCommit
      try {
        conn.setAutoCommit(false)
        f(SnowflakeClient.Transaction(this, conn)).flatMap { result =>
          commitSilently(conn).map(_ => result)
        }
      } catch {
        case ex: Exception =>
          rollbackSilently(conn)
          Left(QueryError.TransactionError("transaction", ex.getMessage, Some(ex)))
      } finally {
        conn.setAutoCommit(autoCommit)
      }
    }

  /**
   * Closes the client and releases resources.
   *
   * @return success or an error
   */
  def close(): Either[ConnectionError, Unit] = provider.close()

  /**
   * Executes a function with a session.
   *
   * @param f the function to execute with the session
   * @tparam A the return type of the function
   * @return the result of the function or an error
   */
  def withSession[A](f: SnowflakeSession => Either[SnowflakeError, A]): Either[SnowflakeError, A] =
    SnowflakeSession.withSession(provider)(f)

  private def commitSilently(connection: Connection): Either[SnowflakeError, Unit] = {
    Try(connection.commit()).toEither.left.map { ex =>
      rollbackSilently(connection).pipe(_ => QueryError.TransactionError("commit", ex.getMessage, Some(ex)))
    }
  }

  private def rollbackSilently(connection: Connection): Unit = {
    try connection.rollback()
    catch { case _: Exception => () }
  }
}

object SnowflakeClient {

  /** Represents a transaction context for executing operations. */
  final case class Transaction(client: SnowflakeClient, connection: Connection) {
    /**
     * Executes a query within the transaction.
     *
     * @param sql the SQL query string
     * @param params the query parameters
     * @param read the reader for the result type
     * @tparam T the type of the query result
     * @return the query results or an error
     */
    def query[T](sql: String, params: Seq[Param] = Seq.empty)(implicit
        read: Read[T]
    ): Either[SnowflakeError, QueryResult[T]] =
      QueryExecutor.executeQuery(connection, sql, params)

    /**
     * Executes a command within the transaction.
     *
     * @param sql the SQL command string
     * @param params the command parameters
     * @return the update result or an error
     */
    def execute(sql: String, params: Seq[Param] = Seq.empty): Either[SnowflakeError, UpdateResult] =
      QueryExecutor.executeUpdate(connection, sql, params)

    /**
     * Executes a batch of commands within the transaction.
     *
     * @param sql the SQL command string
     * @param batches the list of parameter batches
     * @return the batch result or an error
     */
    def executeBatch(sql: String, batches: List[Seq[Param]]): Either[SnowflakeError, BatchResult] =
      QueryExecutor.executeBatch(connection, sql, batches)
  }

  /**
   * Creates a client with pooled connections using the given configuration.
   *
   * @param config the connection configuration
   * @return a new Snowflake client
   */
  def apply(config: ConnectionConfig): SnowflakeClient = pooled(config)

  /**
   * Creates a client with pooled connections using the given configuration.
   *
   * @param config the connection configuration
   * @return a new Snowflake client
   */
  def pooled(config: ConnectionConfig): SnowflakeClient =
    new SnowflakeClient(ConnectionProvider.hikari(config))

  /**
   * Creates a client with pooled connections and custom Hikari configuration.
   *
   * @param config the connection configuration
   * @param customize the Hikari configuration function
   * @return a new Snowflake client
   */
  def pooled(config: ConnectionConfig, customize: HikariConfig => HikariConfig): SnowflakeClient =
    new SnowflakeClient(ConnectionProvider.hikari(config, customize))

  /**
   * Creates a client with simple connections using the given configuration.
   *
   * @param config the connection configuration
   * @return a new Snowflake client
   */
  def simple(config: ConnectionConfig): SnowflakeClient =
    new SnowflakeClient(ConnectionProvider.simple(config))

  /**
   * Executes a function with a client, managing the client's lifecycle.
   *
   * @param config the connection configuration
   * @param f the function to execute with the client
   * @tparam A the return type of the function
   * @return the result of the function or an error
   */
  def withClient[A](
      config: ConnectionConfig
  )(f: SnowflakeClient => Either[SnowflakeError, A]): Either[SnowflakeError, A] = {
    val client = pooled(config)
    f(client).flatMap(result => client.close().map(_ => result))
  }
}
