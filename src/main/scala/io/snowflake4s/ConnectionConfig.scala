package io.snowflake4s

import io.snowflake4s.SnowflakeError.ConfigError
import com.typesafe.config.{Config, ConfigFactory}

/** Configuration for Snowflake connection.
  *
  * @param account
  *   Snowflake account identifier (required)
  * @param user
  *   Username for authentication (required)
  * @param password
  *   Password for authentication (required)
  * @param role
  *   Optional role to use
  * @param warehouse
  *   Optional warehouse to use
  * @param database
  *   Optional default database
  * @param schema
  *   Optional default schema
  * @param authenticator
  *   Optional authenticator (e.g., "snowflake", "externalbrowser")
  * @param clientSessionKeepAlive
  *   Optional keep-alive setting
  * @param application
  *   Optional application name forwarded to Snowflake
  * @param queryTag
  *   Optional query tag for observability
  * @param ocspFailOpen
  *   Optional OCSP behaviour toggle
  * @param jdbcQueryResultFormat
  *   Optional JDBC result format preference (JSON/ARROW)
  * @param queryTimeoutSec
  *   Optional default statement query timeout (seconds)
  * @param networkTimeoutSec
  *   Optional network timeout (seconds)
  * @param useCachedResult
  *   Optional cached result toggle
  * @param fetchSize
  *   Optional preferred JDBC fetch size
  * @param sessionParams
  *   Additional session parameters forwarded as JSON
  * @param privateKeyPath
  *   Optional path to encrypted RSA private key for key pair auth
  * @param privateKeyPassphrase
  *   Optional passphrase for the private key
  */
final case class ConnectionConfig(
    account: String,
    user: String,
    password: String,
    role: Option[String] = None,
    warehouse: Option[String] = None,
    database: Option[String] = None,
    schema: Option[String] = None,
    authenticator: Option[String] = None,
    clientSessionKeepAlive: Option[Boolean] = None,
    application: Option[String] = None,
    queryTag: Option[String] = None,
    ocspFailOpen: Option[Boolean] = None,
    jdbcQueryResultFormat: Option[ConnectionConfig.JdbcQueryResultFormat] = None,
    queryTimeoutSec: Option[Int] = None,
    networkTimeoutSec: Option[Int] = None,
    useCachedResult: Option[Boolean] = None,
    fetchSize: Option[Int] = None,
    sessionParams: Map[String, String] = Map.empty,
    privateKeyPath: Option[String] = None,
    privateKeyPassphrase: Option[String] = None
) {

  def jdbcUrl: String = s"jdbc:snowflake://$account.snowflakecomputing.com"

  def toProperties: java.util.Properties = {
    val props = new java.util.Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)

    role.foreach(r => props.setProperty("role", r))
    warehouse.foreach(w => props.setProperty("warehouse", w))
    database.foreach(db => props.setProperty("db", db))
    schema.foreach(s => props.setProperty("schema", s))
    authenticator.foreach(a => props.setProperty("authenticator", a))
    clientSessionKeepAlive.foreach { keepAlive =>
      props.setProperty("client_session_keep_alive", keepAlive.toString)
    }
    application.foreach(app => props.setProperty("application", app))
    queryTag.foreach(tag => props.setProperty("query_tag", tag))
    ocspFailOpen.foreach(flag => props.setProperty("ocsp_fail_open", flag.toString))
    jdbcQueryResultFormat.foreach(format => props.setProperty("JDBC_QUERY_RESULT_FORMAT", format.entryName))
    queryTimeoutSec.foreach(timeout => props.setProperty("queryTimeout", timeout.toString))
    networkTimeoutSec.foreach(timeout => props.setProperty("networkTimeout", timeout.toString))
    useCachedResult.foreach(flag => props.setProperty("use_cached_result", flag.toString))
    fetchSize.foreach(size => props.setProperty("fetchSize", size.toString))
    if (sessionParams.nonEmpty) {
      props.setProperty("sessionParameters", ConnectionConfig.encodeSessionParams(sessionParams))
    }
    privateKeyPath.foreach(path => props.setProperty("private_key_file", path))
    privateKeyPassphrase.foreach(pass => props.setProperty("private_key_file_pwd", pass))

    props
  }
}

object ConnectionConfig {

  sealed abstract class JdbcQueryResultFormat(val entryName: String)
  object JdbcQueryResultFormat {
    case object Json extends JdbcQueryResultFormat("JSON")
    case object Arrow extends JdbcQueryResultFormat("ARROW")

    private val byName: Map[String, JdbcQueryResultFormat] = List(Json, Arrow).map(f => f.entryName -> f).toMap
    def fromString(name: String): Either[ConfigError.InvalidValue, JdbcQueryResultFormat] = {
      byName
        .get(name.toUpperCase)
        .toRight(
          ConfigError.InvalidValue("jdbcQueryResultFormat", name, s"Expected one of: ${byName.keys.mkString(", ")}")
        )
    }
  }

  def fromConfig(config: Config): Either[ConfigError, ConnectionConfig] = {
    def parseJdbcFormat(raw: Option[String]): Either[ConfigError, Option[JdbcQueryResultFormat]] =
      raw match {
        case None        => Right(None)
        case Some(value) => JdbcQueryResultFormat.fromString(value).map(Some(_))
      }

    try {
      val snowflakeConfig = config.getConfig("snowflake4s")
      for {
        account <- getRequiredString(snowflakeConfig, "account")
        user <- getRequiredString(snowflakeConfig, "user")
        password <- getRequiredString(snowflakeConfig, "password")
        jdbcFmt <- parseJdbcFormat(opt(snowflakeConfig, "jdbcQueryResultFormat")(_.getString(_)))
      } yield ConnectionConfig(
        account = account,
        user = user,
        password = password,
        role = opt(snowflakeConfig, "role")(_.getString(_)),
        warehouse = opt(snowflakeConfig, "warehouse")(_.getString(_)),
        database = opt(snowflakeConfig, "database")(_.getString(_)),
        schema = opt(snowflakeConfig, "schema")(_.getString(_)),
        authenticator = opt(snowflakeConfig, "authenticator")(_.getString(_)),
        clientSessionKeepAlive = opt(snowflakeConfig, "clientSessionKeepAlive")(_.getBoolean(_)),
        application = opt(snowflakeConfig, "application")(_.getString(_)),
        queryTag = opt(snowflakeConfig, "queryTag")(_.getString(_)),
        ocspFailOpen = opt(snowflakeConfig, "ocspFailOpen")(_.getBoolean(_)),
        jdbcQueryResultFormat = jdbcFmt,
        queryTimeoutSec = opt(snowflakeConfig, "queryTimeoutSec")(_.getInt(_)),
        networkTimeoutSec = opt(snowflakeConfig, "networkTimeoutSec")(_.getInt(_)),
        useCachedResult = opt(snowflakeConfig, "useCachedResult")(_.getBoolean(_)),
        fetchSize = opt(snowflakeConfig, "fetchSize")(_.getInt(_)),
        sessionParams = getSessionParams(snowflakeConfig, "sessionParams"),
        privateKeyPath = opt(snowflakeConfig, "privateKeyPath")(_.getString(_)),
        privateKeyPassphrase = opt(snowflakeConfig, "privateKeyPassphrase")(_.getString(_))
      )
    } catch {
      case ex: Exception => Left(ConfigError.LoadFailed(ex.getMessage))
    }
  }

  def load(): Either[ConfigError, ConnectionConfig] = {
    try {
      val config = ConfigFactory.load()
      fromConfig(config)
    } catch {
      case ex: Exception =>
        Left(ConfigError.LoadFailed(ex.getMessage))
    }
  }

  private def getRequiredString(config: Config, path: String): Either[ConfigError, String] = {
    if (config.hasPath(path)) {
      try {
        Right(config.getString(path))
      } catch {
        case ex: Exception =>
          Left(ConfigError.InvalidValue(path, "", ex.getMessage))
      }
    } else {
      Left(ConfigError.MissingRequired(path))
    }
  }

  // Generic safe optional accessor; swallows individual value errors returning None (maintains previous semantics)
  private def opt[A](config: Config, path: String)(read: (Config, String) => A): Option[A] = {
    if (!config.hasPath(path)) None
    else {
      try Some(read(config, path))
      catch { case _: Exception => None }
    }
  }

  private def getSessionParams(config: Config, path: String): Map[String, String] = {
    if (!config.hasPath(path)) {
      Map.empty
    } else {
      val obj = config.getConfig(path)
      import scala.jdk.CollectionConverters.*
      obj
        .entrySet()
        .asScala
        .map { entry =>
          val key = entry.getKey
          val value = obj.getString(key)
          key -> value
        }
        .toMap
    }
  }

  private def encodeSessionParams(params: Map[String, String]): String = {
    def escape(str: String): String = {
      val sb = new StringBuilder(str.length + 8)
      str.foreach {
        case '"'  => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case c if c <= 0x1f || c >= 0x80 =>
          sb.append("\\u").append(f"${c.toInt}%04x")
        case other => sb.append(other)
      }
      sb.toString()
    }
    params.map { case (k, v) => s"\"${escape(k)}\":\"${escape(v)}\"" }.mkString("{", ",", "}")
  }
}
