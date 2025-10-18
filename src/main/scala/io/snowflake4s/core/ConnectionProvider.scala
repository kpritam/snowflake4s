package io.snowflake4s.core

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.snowflake4s.{ConnectionConfig, SnowflakeError}
import io.snowflake4s.SnowflakeError.{ConnectionError, QueryError}
import io.snowflake4s.core.ConnectionProvider.ConnectionProviderKind.*
import io.snowflake4s.core.ConnectionProvider.{mergeResults, safeClose, safelyUse}

import java.sql.{Connection, DriverManager, SQLException, SQLTransientConnectionException}
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps
import scala.util.control.NonFatal

/** Provides JDBC connections for database operations. */
trait ConnectionProvider {

  /** Executes a function with a database connection, ensuring the connection is properly managed.
    *
    * @param use
    *   the function to execute with the connection
    * @tparam A
    *   the return type of the function
    * @return
    *   the result of the function or an error
    */
  def withConnection[A](use: Connection => Either[SnowflakeError, A]): Either[SnowflakeError, A]

  /** Closes the connection provider and releases any associated resources.
    *
    * @return
    *   success or an error if closing fails
    */
  def close(): Either[ConnectionError, Unit]

  private[core] def getConnection: Either[ConnectionError, Connection]

  private[core] def acquireAndUse[A](use: Connection => Either[SnowflakeError, A]): Either[SnowflakeError, A] =
    getConnection.flatMap { connection =>
      val result = safelyUse(connection, use)
      val closed = safeClose(connection.close())
      mergeResults(result, closed)
    }
}

object ConnectionProvider {

  /** Specifies the type of connection provider to use. */
  sealed trait ConnectionProviderKind
  object ConnectionProviderKind {

    /** Uses a simple connection provider that creates new connections on demand. */
    case object Simple extends ConnectionProviderKind

    /** Uses a Hikari connection pool with optional customization. */
    final case class Hikari(customize: HikariConfig => HikariConfig = identity) extends ConnectionProviderKind
  }

  /** Creates a connection provider of the specified kind.
    *
    * @param kind
    *   the type of connection provider
    * @param config
    *   the connection configuration
    * @return
    *   a new connection provider
    */
  def of(kind: ConnectionProviderKind, config: ConnectionConfig): ConnectionProvider = kind match {
    case Simple            => simple(config)
    case Hikari(customize) => hikari(config, customize)
  }

  /** Creates a simple connection provider that creates new connections on demand.
    *
    * @param config
    *   the connection configuration
    * @return
    *   a simple connection provider
    */
  def simple(config: ConnectionConfig): ConnectionProvider = new SimpleConnectionProvider(config)

  /** Creates a Hikari connection pool provider with optional configuration.
    *
    * @param config
    *   the connection configuration
    * @param hikariConfig
    *   optional customization function for the Hikari config
    * @return
    *   a Hikari connection provider
    */
  def hikari(config: ConnectionConfig, hikariConfig: HikariConfig => HikariConfig = identity): ConnectionProvider =
    new HikariConnectionProvider(config, hikariConfig)

  private class SimpleConnectionProvider(config: ConnectionConfig) extends ConnectionProvider {

    override def withConnection[A](use: Connection => Either[SnowflakeError, A]): Either[SnowflakeError, A] =
      acquireAndUse(use)

    private[core] override def getConnection: Either[ConnectionError, Connection] =
      Try {
        Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")
        val connection = DriverManager.getConnection(config.jdbcUrl, config.toProperties)
        configureConnection(connection, config)
        connection
      }.toEither.left.map(ex => ConnectionError.FailedToConnect(ex.getMessage, ex))

    override def close(): Either[ConnectionError, Unit] = Right(())

  }

  private class HikariConnectionProvider(config: ConnectionConfig, configure: HikariConfig => HikariConfig)
      extends ConnectionProvider {
    private val dataSource: HikariDataSource = {
      val hikari = new HikariConfig()
      hikari.setJdbcUrl(config.jdbcUrl)
      hikari.setDriverClassName("net.snowflake.client.jdbc.SnowflakeDriver")
      hikari.setUsername(config.user)
      hikari.setPassword(config.password)
      hikari.setDataSourceProperties(config.toProperties)
      configure(hikari)
      new HikariDataSource(hikari)
    }

    override def withConnection[A](use: Connection => Either[SnowflakeError, A]): Either[SnowflakeError, A] =
      acquireAndUse(use)

    override def getConnection: Either[ConnectionError, Connection] =
      Try(dataSource.getConnection.tap(configureConnection(_, config))).toEither.left.map(mapException)

    override def close(): Either[ConnectionError, Unit] = safeClose(dataSource.close())
  }

  private def configureConnection(connection: Connection, config: ConnectionConfig): Unit = {
    config.networkTimeoutSec.foreach(timeoutSec => Try(connection.setNetworkTimeout(null, timeoutSec * 1000)))
    config.queryTimeoutSec.foreach(timeout => setClientInfo(connection, "queryTimeout", timeout.toString))
    config.fetchSize.foreach(size => setClientInfo(connection, "fetchSize", size.toString))
    config.queryTag.foreach(tag => setClientInfo(connection, "query_tag", tag))
    config.sessionParams.foreach { case (key, value) => setClientInfo(connection, key, value) }
  }

  private def setClientInfo(connection: Connection, key: String, value: String): Unit = {
    try connection.setClientInfo(key, value)
    catch { case _: Exception => () }
  }

  private def safeClose(closeAction: => Unit): Either[ConnectionError, Unit] =
    Try(closeAction).toEither.left.map(e => ConnectionError.ConnectionClosed(e.getMessage))

  private def safelyUse[A](
      connection: Connection,
      use: Connection => Either[SnowflakeError, A]
  ): Either[SnowflakeError, A] =
    try use(connection)
    catch {
      case ex: SQLException =>
        Left(QueryError.SqlException(Option(ex.getSQLState), ex.getErrorCode, ex.getMessage, cause = Some(ex)))
      case NonFatal(ex) =>
        Left(ConnectionError.Unknown(ex))
    }

  private def mergeResults[A](
      result: Either[SnowflakeError, A],
      closeResult: Either[ConnectionError, Unit]
  ): Either[SnowflakeError, A] =
    (result, closeResult) match {
      case (Right(value), Right(_))   => Right(value)
      case (Left(err), _)             => Left(err)
      case (Right(_), Left(closeErr)) => Left(closeErr)
    }

  private def mapException(throwable: Throwable): ConnectionError = throwable match {
    case ex: SQLTransientConnectionException => ConnectionError.PoolExhausted(ex.getMessage)
    case ex: SQLException                    => ConnectionError.FailedToConnect(ex.getMessage, ex)
    case NonFatal(ex)                        => ConnectionError.Unknown(ex)
    case ex                                  => ConnectionError.Unknown(ex)
  }
}
