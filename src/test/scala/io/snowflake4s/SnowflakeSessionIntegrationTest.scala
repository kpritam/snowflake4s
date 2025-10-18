package io.snowflake4s

import io.snowflake4s.SnowflakeError.QueryError
import io.snowflake4s.jdbc.Read
import io.snowflake4s.sql.{Command0, Fragment, Param, Query0}
import munit.FunSuite

import java.util.UUID

final case class IntegrationAccount(id: Int, balance: BigDecimal, active: Boolean)

class SnowflakeSessionIntegrationTest extends FunSuite {

  private val runIntegrationTests = sys.env.get("SNOWFLAKE_INTEGRATION_TEST").contains("true")

  override def munitIgnore: Boolean = !runIntegrationTests

  private implicit val userRead: Read[User] = Read.derived[User]
  private implicit val accountRead: Read[IntegrationAccount] = Read.derived[IntegrationAccount]

  test("SnowflakeClient.query executes simple select") {
    val result = withClient(_.query[Int]("SELECT 1"))
    assertRight(result)(res => assertEquals(res.rows, List(1)))
  }

  test("SnowflakeClient.execute and query decode case class") {
    val result = withClient { client =>
      withManagedTable(client, "users_read") { table =>
        for {
          _ <- client.execute(s"CREATE OR REPLACE TABLE $table (id INT, name STRING, email STRING)")
          _ <- client.execute(
            s"INSERT INTO $table (id, name, email) VALUES (?, ?, ?)",
            params = Seq(Param.of(1), Param.of("Alice"), Param.of("alice@example.com"))
          )
          _ <- client.execute(
            s"INSERT INTO $table (id, name, email) VALUES (?, ?, ?)",
            params = Seq(Param.of(2), Param.of("Bob"), Param.of(None: Option[String]))
          )
          rows <- client.query[User](s"SELECT id, name, email FROM $table ORDER BY id")
        } yield rows.rows
      }
    }

    assertRight(result) { rows =>
      assertEquals(rows, List(User(1, "Alice", Some("alice@example.com")), User(2, "Bob", None)))
    }
  }

  test("SnowflakeClient.query handles Query0 fragments") {
    val result = withClient { client =>
      withManagedTable(client, "query0_users") { table =>
        val query: Query0[User] =
          (Fragment.const(s"SELECT id, name, email FROM $table WHERE id = ") ++ Fragment.param(1)).query[User]
        for {
          _ <- client.execute(s"CREATE OR REPLACE TABLE $table (id INT, name STRING, email STRING)")
          _ <- client.execute(
            s"INSERT INTO $table (id, name, email) VALUES (?, ?, ?)",
            params = Seq(
              Param.of(1),
              Param.of("Carol"),
              Param.of[Option[String]](Some("carol@example.com"))
            )
          )
          row <- client.query(query)
        } yield row.rows
      }
    }

    assertRight(result)(rows => assertEquals(rows, List(User(1, "Carol", Some("carol@example.com")))))
  }

  test("SnowflakeClient.executeBatch inserts multiple rows") {
    val result = withClient { client =>
      withManagedTable(client, "batch_users") { table =>
        for {
          _ <- client.execute(s"CREATE OR REPLACE TABLE $table (id INT, name STRING, email STRING)")
          batch <- client.executeBatch(
            s"INSERT INTO $table (id, name, email) VALUES (?, ?, ?)",
            List(
              Seq(Param.of(10), Param.of("Batch-1"), Param.of[Option[String]](Some("b1@example.com"))),
              Seq(Param.of(11), Param.of("Batch-2"), Param.of[Option[String]](Some("b2@example.com"))),
              Seq(Param.of(12), Param.of("Batch-3"), Param.of(None: Option[String]))
            )
          )
          rows <- client.query[User](s"SELECT id, name, email FROM $table ORDER BY id")
        } yield (batch.updated, rows.rows)
      }
    }

    assertRight(result) { case (updated, rows) =>
      assertEquals(updated, List(1, 1, 1))
      assertEquals(rows.map(_.id), List(10, 11, 12))
      assertEquals(rows.find(_.id == 12).flatMap(_.email), None)
    }
  }

  test("SnowflakeClient.withTransaction commits on success") {
    val result = withClient { client =>
      withManagedTable(client, "txn_commit") { table =>
        for {
          _ <- client.execute(
            s"CREATE OR REPLACE TABLE $table (id INT PRIMARY KEY, name STRING, email STRING)"
          )
          _ <- client.withTransaction { tx =>
            for {
              _ <- tx.execute(
                s"INSERT INTO $table (id, name, email) VALUES (?, ?, ?)",
                params = Seq(
                  Param.of(1),
                  Param.of("Txn-A"),
                  Param.of[Option[String]](Some("txn-a@example.com"))
                )
              )
              _ <- tx.execute(
                s"INSERT INTO $table (id, name, email) VALUES (?, ?, ?)",
                params = Seq(
                  Param.of(2),
                  Param.of("Txn-B"),
                  Param.of(None: Option[String])
                )
              )
            } yield ()
          }
          rows <- client.query[User](s"SELECT id, name, email FROM $table ORDER BY id")
        } yield rows.rows
      }
    }

    assertRight(result)(rows => assertEquals(rows.map(_.id), List(1, 2)))
  }

  test("SnowflakeClient.withTransaction rolls back on failure") {
    val result = withClient { client =>
      withManagedTable(client, "txn_rollback") { table =>
        client
          .execute(
            s"CREATE OR REPLACE TABLE $table (id INT, name STRING, email STRING)"
          )
          .flatMap { _ =>
            client.withTransaction { tx =>
              for {
                _ <- tx.execute(
                  s"INSERT INTO $table (id, name, email) VALUES (?, ?, ?)",
                  params = Seq(
                    Param.of(1),
                    Param.of("Txn-Fail"),
                    Param.of[Option[String]](Some("fail@example.com"))
                  )
                )
                _ <- Left[SnowflakeError, Unit](QueryError.ResultSetError("forced failure", None))
              } yield ()
            }
            client
              .query[User](s"SELECT id, name, email FROM $table ORDER BY id")
              .map(result => (result.rows.isEmpty, result.rows))
          }
      }
    }

    assertRight(result) { case (txnOutcome, rows) =>
      assert(txnOutcome)
      assertEquals(rows, Nil)
    }
  }

  test("SnowflakeClient.withSession exposes SnowflakeSession read APIs") {
    val result = withClient { client =>
      client.withSession { session =>
        val table = randomTableName("session_accounts")
        val createTable = Command0(
          Fragment.const(
            s"CREATE TEMPORARY TABLE $table (id INT, balance NUMBER(10, 2), active BOOLEAN)"
          )
        )
        val insert = (id: Int, balance: BigDecimal, active: Boolean) =>
          sql"INSERT INTO ${Fragment.const(table)} (id, balance, active) VALUES (${id}, ${balance}, ${active})".update
        val selectAll =
          sql"SELECT id, balance, active FROM ${Fragment.const(table)} ORDER BY id".query[IntegrationAccount]
        val selectOne =
          sql"SELECT id, balance, active FROM ${Fragment.const(table)} WHERE id = ${1}".query[IntegrationAccount]
        val selectMissing =
          sql"SELECT id, balance, active FROM ${Fragment.const(table)} WHERE id = ${999}".query[IntegrationAccount]

        for {
          _ <- session.execute(createTable)
          _ <- session.execute(insert(1, BigDecimal(100.25), true))
          _ <- session.execute(insert(2, BigDecimal(250.00), false))
          list <- session.list(selectAll)
          option <- session.option(selectMissing)
          unique <- session.unique(selectOne)
          stream <- session.stream(selectAll)
        } yield (list, option, unique, stream)
      }
    }

    assertRight(result) { case (list, option, unique, stream) =>
      assertEquals(list.map(_.id), List(1, 2))
      assertEquals(option, None)
      assertEquals(unique.id, 1)
      assertEquals(stream.map(_.id).toList, List(1, 2))
    }
  }

  test("SnowflakeSession.transaction commits and rolls back within withSession") {
    val result = withClient { client =>
      client.withSession { session =>
        val table = randomTableName("session_txn")
        val createTable = Command0(
          Fragment.const(
            s"CREATE TEMPORARY TABLE $table (id INT, balance NUMBER(10, 2), active BOOLEAN)"
          )
        )
        val insert = (id: Int, balance: BigDecimal, active: Boolean) =>
          sql"INSERT INTO ${Fragment.const(table)} (id, balance, active) VALUES (${id}, ${balance}, ${active})".update
        val selectAll =
          sql"SELECT id, balance, active FROM ${Fragment.const(table)} ORDER BY id".query[IntegrationAccount]
        val selectById = (id: Int) =>
          sql"SELECT id, balance, active FROM ${Fragment.const(table)} WHERE id = ${id}".query[IntegrationAccount]

        session.execute(createTable).flatMap { _ =>
          session.execute(insert(1, BigDecimal(50.00), true)).flatMap { _ =>
            session
              .transaction { tx =>
                for {
                  _ <- tx.execute(insert(2, BigDecimal(75.00), true))
                  fetched <- tx.unique(selectById(2))
                } yield fetched
              }
              .flatMap { commitResult =>
                val rollbackResult = session.transaction { tx =>
                  for {
                    _ <- tx.execute(insert(3, BigDecimal(125.00), false))
                    _ <- Left[SnowflakeError, Unit](QueryError.ResultSetError("forced failure", None))
                  } yield ()
                }
                session.list(selectAll).map(rows => (commitResult, rollbackResult, rows))
              }
          }
        }
      }
    }

    assertRight(result) { case (commitResult, rollbackResult, rowsAfter) =>
      assertEquals(commitResult.id, 2)
      assert(rollbackResult.isLeft, "expected rollback failure")
      assertEquals(rowsAfter.map(_.id), List(1, 2))
    }
  }

  private def withClient[A](f: SnowflakeClient => Either[SnowflakeError, A]): Either[SnowflakeError, A] =
    ConnectionConfig.load().flatMap { config =>
      val client = SnowflakeClient.simple(config)
      val result = f(client)
      client.close() match {
        case Left(closeErr) => Left(closeErr)
        case Right(_)       => result
      }
    }

  private def withManagedTable[A](
      client: SnowflakeClient,
      prefix: String
  )(use: String => Either[SnowflakeError, A]): Either[SnowflakeError, A] = {
    val table = randomTableName(prefix)
    val result = use(table)
    val _ = client.execute(s"DROP TABLE IF EXISTS $table")
    result
  }

  private def randomTableName(prefix: String): String =
    s"${prefix}_${UUID.randomUUID().toString.replace('-', '_')}"

  private def assertRight[A](either: Either[SnowflakeError, A])(assertions: A => Unit): Unit =
    either match {
      case Right(value) => assertions(value)
      case Left(err)    => fail(s"Operation failed: ${err.message}")
    }
}
