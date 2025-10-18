package io.snowflake4s.examples

import io.snowflake4s.*
import io.snowflake4s.core.QueryExecutor.QueryResult

import java.time.Instant

/** Quick start example demonstrating snowflake4s usage. */
object QuickStartExample {

  final case class Product(
      id: Int,
      name: String,
      price: BigDecimal,
      description: Option[String],
      createdAt: Instant
  )

  implicit val productRead: Read[Product] = Read.derived[Product]

  def main(args: Array[String]): Unit = {
    val result = for {
      config <- ConnectionConfig.load()
      client = SnowflakeClient.pooled(config)
      _ <- client.withSession { session =>
        for {
          _ <- setupTable(session)
          _ <- insertProducts(session)
          products <- fetchProducts(session)
          _ <- printProducts(products.rows, products.queryId)
        } yield ()
      }
      _ <- client.close()
    } yield ()

    result match {
      case Right(_) =>
        println("\n✓ Example completed successfully")
      case Left(error) =>
        println(s"\n✗ Error: ${error.message}")
        sys.exit(1)
    }
  }

  private val createTable: Command0 =
    sql"""CREATE TEMPORARY TABLE products (
         |  id INTEGER,
         |  name VARCHAR(100),
         |  price DECIMAL(10, 2),
         |  description VARCHAR(500),
         |  created_at TIMESTAMP_NTZ
         |)""".stripMargin().update

  private def setupTable(session: SnowflakeSession): Either[SnowflakeError, Int] =
    session.execute(createTable).map { _ =>
      println("✓ Created temporary table")
      0
    }

  private def insertProducts(session: SnowflakeSession): Either[SnowflakeError, Unit] = {
    val now = Instant.now()
    val products = List(
      (1, "Laptop", BigDecimal("999.99"), Some("High-performance laptop"), now),
      (2, "Mouse", BigDecimal("29.99"), None, now),
      (3, "Keyboard", BigDecimal("79.99"), Some("Mechanical keyboard"), now)
    )

    products.foldLeft[Either[SnowflakeError, Unit]](Right(())) { case (acc, (id, name, price, desc, time)) =>
      acc.flatMap { _ =>
        session
          .execute(
            sql"""INSERT INTO products (id, name, price, description, created_at) VALUES ($id, $name, $price, $desc, $time)""".update
          )
          .map { _ =>
            println(s"✓ Inserted product: $name")
          }
      }
    }
  }

  private val selectProducts: Query0[Product] =
    sql"SELECT id, name, price, description, created_at FROM products ORDER BY id".query[Product]

  private def fetchProducts(session: SnowflakeSession): Either[SnowflakeError, QueryResult[Product]] =
    session.query(selectProducts)

  private def printProducts(products: List[Product], queryId: Option[String]): Either[SnowflakeError, Unit] = {
    println("\n--- All Products ---")
    products.foreach { product =>
      val desc = product.description.getOrElse("No description")
      println(f"${product.id}%2d. ${product.name}%-15s $$${product.price}%7.2f - $desc")
    }
    queryId.foreach(id => println(s"(queryId=$id)"))
    Right(())
  }
}
