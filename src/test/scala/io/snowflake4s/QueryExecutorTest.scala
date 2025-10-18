package io.snowflake4s

import io.snowflake4s.core.QueryExecutor
import io.snowflake4s.jdbc.Read
import munit.FunSuite
import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData}
import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchersSugar

final case class User(id: Int, name: String, email: Option[String])

/** Unit tests for QueryExecutor using mocked JDBC components. These tests verify the query execution and result parsing
  * logic without requiring a real Snowflake connection.
  */
class QueryExecutorTest extends FunSuite with MockitoSugar with ArgumentMatchersSugar {

  test("executeQuery should parse single column results") {
    val mockConn = mock[Connection]
    val mockStmt = mock[PreparedStatement]
    val mockRs = mock[ResultSet]
    val mockMeta = mock[ResultSetMetaData]

    // Setup mock behavior
    when(mockConn.prepareStatement(any[String])).thenReturn(mockStmt)
    when(mockStmt.executeQuery()).thenReturn(mockRs)
    when(mockStmt.unwrap(classOf[net.snowflake.client.jdbc.SnowflakeStatement]))
      .thenThrow(new java.sql.SQLException("No query ID"))
    when(mockRs.getMetaData).thenReturn(mockMeta)
    when(mockMeta.getColumnCount).thenReturn(1)
    when(mockMeta.getColumnLabel(1)).thenReturn("id")

    // Mock result set iteration
    when(mockRs.next()).thenReturn(true, true, true, false)
    when(mockRs.getInt(1)).thenReturn(1, 2, 3)
    when(mockRs.wasNull()).thenReturn(false)

    val result = QueryExecutor.executeQuery[Int](mockConn, "SELECT id FROM users", Seq.empty)

    result match {
      case Right(queryResult) =>
        assertEquals(queryResult.rows, List(1, 2, 3))
        assertEquals(queryResult.queryId, None)
      case Left(err) =>
        fail(s"Expected Right but got Left: ${err.message}")
    }

    verify(mockStmt).close()
    verify(mockRs).close()
  }

  test("executeQuery should parse case class with derived Read instance") {
    implicit val userRead: Read[User] = Read.derived[User]

    val mockConn = mock[Connection]
    val mockStmt = mock[PreparedStatement]
    val mockRs = mock[ResultSet]
    val mockMeta = mock[ResultSetMetaData]

    // Setup mock behavior
    when(mockConn.prepareStatement(any[String])).thenReturn(mockStmt)
    when(mockStmt.executeQuery()).thenReturn(mockRs)
    when(mockRs.getMetaData).thenReturn(mockMeta)
    when(mockMeta.getColumnCount).thenReturn(3)
    when(mockMeta.getColumnLabel(1)).thenReturn("id")
    when(mockMeta.getColumnLabel(2)).thenReturn("name")
    when(mockMeta.getColumnLabel(3)).thenReturn("email")

    // Mock result set iteration - two rows
    when(mockRs.next()).thenReturn(true, true, false)

    // First row: User(1, "Alice", Some("alice@example.com"))
    when(mockRs.getInt(1)).thenReturn(1, 2)
    when(mockRs.getString(2)).thenReturn("Alice", "Bob")
    when(mockRs.getString(3)).thenReturn("alice@example.com", null)

    // Mock wasNull for handling Option - first call returns false, second returns true
    when(mockRs.wasNull()).thenReturn(false, false, false, false, false, true)

    val result = QueryExecutor.executeQuery[User](
      mockConn,
      "SELECT id, name, email FROM users",
      Seq.empty
    )

    result match {
      case Right(queryResult) =>
        assertEquals(queryResult.rows.length, 2)
        assertEquals(queryResult.rows.head.id, 1)
        assertEquals(queryResult.rows.head.name, "Alice")
        assertEquals(queryResult.rows.head.email, Some("alice@example.com"))
        assertEquals(queryResult.rows(1).id, 2)
        assertEquals(queryResult.rows(1).name, "Bob")
        assertEquals(queryResult.rows(1).email, None)
        assertEquals(queryResult.queryId, None)
      case Left(err) =>
        fail(s"Expected Right but got Left: ${err.message}")
    }

    verify(mockStmt).close()
    verify(mockRs).close()
  }

  test("executeUpdate should return affected row count") {
    val mockConn = mock[Connection]
    val mockStmt = mock[PreparedStatement]

    when(mockConn.prepareStatement(any[String])).thenReturn(mockStmt)
    when(mockStmt.executeUpdate()).thenReturn(3)

    val result = QueryExecutor.executeUpdate(
      mockConn,
      "INSERT INTO users (name) VALUES ('test')",
      Seq.empty
    )

    result match {
      case Right(updateResult) =>
        assertEquals(updateResult.updated, 3)
        assertEquals(updateResult.queryId, None)
      case Left(err) =>
        fail(s"Expected Right but got Left: ${err.message}")
    }

    verify(mockStmt).close()
  }

  test("executeQuery should handle SQL exceptions") {
    val mockConn = mock[Connection]
    val mockStmt = mock[PreparedStatement]

    when(mockConn.prepareStatement(any[String])).thenReturn(mockStmt)
    when(mockStmt.executeQuery()).thenThrow(
      new java.sql.SQLException("Table not found", "42S02", 2003)
    )

    val result = QueryExecutor.executeQuery[Int](
      mockConn,
      "SELECT * FROM non_existent",
      Seq.empty
    )

    assert(result.isLeft)
    result match {
      case Left(err) =>
        assert(err.message.contains("42S02"))
        assert(err.message.contains("2003"))
      case Right(_) =>
        fail("Expected Left but got Right")
    }

    verify(mockStmt).close()
  }
}
