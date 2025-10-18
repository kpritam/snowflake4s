package io.snowflake4s

import com.typesafe.config.ConfigFactory
import munit.FunSuite

class ConnectionConfigTest extends FunSuite {

  test("ConnectionConfig should build correct JDBC URL") {
    val config = ConnectionConfig(
      account = "myaccount",
      user = "myuser",
      password = "mypassword"
    )

    assertEquals(config.jdbcUrl, "jdbc:snowflake://myaccount.snowflakecomputing.com")
  }

  test("ConnectionConfig should convert to Properties correctly") {
    val config = ConnectionConfig(
      account = "myaccount",
      user = "myuser",
      password = "mypassword",
      role = Some("myrole"),
      warehouse = Some("mywarehouse"),
      database = Some("mydb"),
      schema = Some("myschema"),
      authenticator = Some("snowflake"),
      clientSessionKeepAlive = Some(true),
      application = Some("snowflake4s-tests"),
      queryTag = Some("unit-test"),
      ocspFailOpen = Some(true),
      jdbcQueryResultFormat = Some(ConnectionConfig.JdbcQueryResultFormat.Arrow),
      queryTimeoutSec = Some(30),
      networkTimeoutSec = Some(60),
      useCachedResult = Some(false),
      fetchSize = Some(123),
      sessionParams = Map("TIMEZONE" -> "UTC"),
      privateKeyPath = Some("/keys/test.p8"),
      privateKeyPassphrase = Some("secret")
    )

    val props = config.toProperties
    assertEquals(props.getProperty("user"), "myuser")
    assertEquals(props.getProperty("password"), "mypassword")
    assertEquals(props.getProperty("role"), "myrole")
    assertEquals(props.getProperty("warehouse"), "mywarehouse")
    assertEquals(props.getProperty("db"), "mydb")
    assertEquals(props.getProperty("schema"), "myschema")
    assertEquals(props.getProperty("authenticator"), "snowflake")
    assertEquals(props.getProperty("client_session_keep_alive"), "true")
    assertEquals(props.getProperty("application"), "snowflake4s-tests")
    assertEquals(props.getProperty("query_tag"), "unit-test")
    assertEquals(props.getProperty("ocsp_fail_open"), "true")
    assertEquals(props.getProperty("JDBC_QUERY_RESULT_FORMAT"), "ARROW")
    assertEquals(props.getProperty("queryTimeout"), "30")
    assertEquals(props.getProperty("networkTimeout"), "60")
    assertEquals(props.getProperty("use_cached_result"), "false")
    assertEquals(props.getProperty("fetchSize"), "123")
    assertEquals(props.getProperty("sessionParameters"), "{\"TIMEZONE\":\"UTC\"}")
    assertEquals(props.getProperty("private_key_file"), "/keys/test.p8")
    assertEquals(props.getProperty("private_key_file_pwd"), "secret")
  }

  test("ConnectionConfig should load from Config") {
    val configString = """
      snowflake4s {
        account = "testaccount"
        user = "testuser"
        password = "testpassword"
        role = "testrole"
        warehouse = "testwarehouse"
        application = "app"
        queryTag = "analytics"
        ocspFailOpen = true
        jdbcQueryResultFormat = "ARROW"
        queryTimeoutSec = 45
        networkTimeoutSec = 120
        useCachedResult = false
        fetchSize = 1000
        sessionParams {
          TIMEZONE = "UTC"
          QUOTED_IDENTIFIERS_IGNORE_CASE = "true"
        }
      }
    """

    val config = ConfigFactory.parseString(configString)
    val result = ConnectionConfig.fromConfig(config)

    assert(result.isRight)
    result match {
      case Right(connConfig) =>
        assertEquals(connConfig.account, "testaccount")
        assertEquals(connConfig.user, "testuser")
        assertEquals(connConfig.password, "testpassword")
        assertEquals(connConfig.role, Some("testrole"))
        assertEquals(connConfig.warehouse, Some("testwarehouse"))
        assertEquals(connConfig.application, Some("app"))
        assertEquals(connConfig.queryTag, Some("analytics"))
        assertEquals(connConfig.ocspFailOpen, Some(true))
        assertEquals(connConfig.jdbcQueryResultFormat, Some(ConnectionConfig.JdbcQueryResultFormat.Arrow))
        assertEquals(connConfig.queryTimeoutSec, Some(45))
        assertEquals(connConfig.networkTimeoutSec, Some(120))
        assertEquals(connConfig.useCachedResult, Some(false))
        assertEquals(connConfig.fetchSize, Some(1000))
        assertEquals(connConfig.sessionParams, Map("TIMEZONE" -> "UTC", "QUOTED_IDENTIFIERS_IGNORE_CASE" -> "true"))
      case Left(err) =>
        fail(s"Expected Right but got Left: ${err.message}")
    }
  }

  test("ConnectionConfig should fail when required fields are missing") {
    val configString = """
      snowflake4s {
        account = "testaccount"
      }
    """

    val config = ConfigFactory.parseString(configString)
    val result = ConnectionConfig.fromConfig(config)

    assert(result.isLeft)
    result match {
      case Left(_: SnowflakeError.ConfigError) => // success
      case Right(_)                            => fail("Expected Left but got Right")
      case Left(other)                         => fail(s"Expected ConfigError but got: $other")
    }
  }

  test("ConnectionConfig should be composable") {
    val config = ConnectionConfig(
      account = "account1",
      user = "user1",
      password = "pass1",
      role = Some("role1"),
      warehouse = Some("warehouse1")
    )

    assertEquals(config.account, "account1")
    assertEquals(config.user, "user1")
    assertEquals(config.password, "pass1")
    assertEquals(config.role, Some("role1"))
    assertEquals(config.warehouse, Some("warehouse1"))
  }

  test("encodeSessionParams should escape quotes and backslashes") {
    val params = Map("KEY" -> "value \"quoted\" \\ slash")
    val config = ConnectionConfig("a", "u", "p", sessionParams = params)
    val props = config.toProperties
    val json = props.getProperty("sessionParameters")
    assert(json.contains("\\\"quoted\\\""))
    assert(json.contains("\\\\ slash"))
    assert(json.startsWith("{\"KEY\":"))
    assert(json.endsWith("}"))
  }

  test("encodeSessionParams should escape control characters and unicode") {
    // String with actual control characters: newline, tab, form feed, plus a snowman unicode char
    val ctrlValue = "line\n tab\t form\f uni:\u2603"
    val params = Map("CTRL" -> ctrlValue)
    val config = ConnectionConfig("a", "u", "p", sessionParams = params)
    val json = config.toProperties.getProperty("sessionParameters")
    // Expect control characters escaped to unicode sequences and presence of snowman unicode.
    assert(json.contains("\\u2603"))
    assert(json.contains("\\u000a")) // newline
    assert(json.contains("\\u0009")) // tab
    assert(json.contains("\\u000c")) // form feed
    assert(!json.contains("\n"))
    assert(!json.contains("\t"))
    assert(!json.contains("\f"))
  }
}
