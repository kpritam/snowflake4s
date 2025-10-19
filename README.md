<div align="center">

# snowflake4s

Lightweight, purely-functional JDBC toolkit for Snowflake in Scala 2.13. Focus: type-safe row decoding, safe resource & transaction handling, ergonomic SQL composition.

[![CI](https://github.com/kpritam/snowflake4s/actions/workflows/ci.yml/badge.svg)](https://github.com/kpritam/snowflake4s/actions/workflows/ci.yml)

</div>

## Why snowflake4s?

Snowflake's JDBC driver is powerful but low level. snowflake4s adds:

| Need                               | Provided By                                       |
| ---------------------------------- | ------------------------------------------------- |
| Declarative, type-safe row mapping | `Read` macro derivation for case classes          |
| Safe parameter encoding            | `Put` and `Param` types                           |
| Composable SQL building            | `sql"…"` interpolator + `Fragment` algebra        |
| Simple resource lifecycle          | `SnowflakeClient.withClient` and `withSession`    |
| Lightweight error model            | Unified `SnowflakeError` ADT returning `Either`   |
| Optional pooling                   | HikariCP integration via `SnowflakeClient.pooled` |

## Features

- Connectivity via config, env, or manual construction
- Type-safe decoders & encoders (`Get`, `Put`, `Read`) with macro derivation
- SQL fragments & interpolation with automatic parameter binding
- Transaction support (client & session level) with rollback safety
- Query IDs surfaced (when available) for observability
- Connection pooling (Hikari) or simple connection provider
- Zero implicit global state – everything is explicit

## Getting Started

### 1. Add the dependency

In `build.sbt` (replace version when releases are published):

```scala
libraryDependencies += "io.github.kpritam" %% "snowflake4s" % "0.1.0-SNAPSHOT"
```

### 2. Configure credentials

`application.conf` (preferred for local dev):

```hocon
snowflake4s {
  account  = "myaccount"
  user     = "myuser"
  password = "mypassword"
  role     = "myrole"       # optional
  warehouse = "mywarehouse" # optional
  database  = "mydb"        # optional
  schema    = "myschema"    # optional
}
```

Or environment variables:

```bash
export SNOWFLAKE_ACCOUNT=myaccount
export SNOWFLAKE_USER=myuser
export SNOWFLAKE_PASSWORD=mypassword
export SNOWFLAKE_ROLE=myrole
export SNOWFLAKE_WAREHOUSE=mywarehouse
export SNOWFLAKE_DATABASE=mydb
export SNOWFLAKE_SCHEMA=myschema
```

### 3. Idiomatic Query Flow (Recommended)

Use a pooled client + session for composability and clear scoping.

```scala
import io.snowflake4s._
import java.time.Instant

final case class User(id: Int, name: String, email: Option[String], createdAt: Instant)
implicit val userRead: Read[User] = Read.derived[User]

val program: Either[SnowflakeError, List[User]] = for {
  cfg    <- ConnectionConfig.load()
  client =  SnowflakeClient.pooled(cfg)
  rows   <- client.withSession { session =>
              session.list(sql"SELECT id, name, email, created_at FROM users WHERE id > ${100}".query[User])
            }
  _      <- client.close()
} yield rows

program.fold(err => println(s"✗ ${err.message}"), users => users.foreach(println))
```

### 4. Safe Composition with Fragments & Interpolator

The `sql"…"` interpolator performs parameter binding automatically – no string concatenation, no manual `?` ordering.

```scala
val base    = sql"SELECT id, name, email FROM users"
val activeF = sql" WHERE status = ${"active"}"
val idsF    = sql" AND id IN ${List(1,2,3)}"            // iterable expands to (?, ?, ?) with binding
val finalQ  = (base ++ activeF ++ idsF).query[User]

client.withSession(_.list(finalQ))
```

Benefits:

- Automatic parameter placement & type-safe binding
- Iterable expansion (`IN`) handled by `Fragment.in` via implicit conversion
- Whitespace handling via `fr` / `fr0` helpers when building multi-line queries

### 5. Transactions (session or client)

```scala
client.withSession { session =>
  session.transaction { tx =>
    for {
      _ <- tx.execute(sql"INSERT INTO audit (event) VALUES (${"start"})".update)
      _ <- tx.execute(sql"UPDATE accounts SET balance = balance - ${100} WHERE id = ${1}".update)
      _ <- tx.execute(sql"UPDATE accounts SET balance = balance + ${100} WHERE id = ${2}".update)
    } yield ()
  }
}
```

Failure inside the `for` short-circuits with a `Left` and triggers rollback.

### 6. Case Class Mapping

```scala
final case class Product(id: Int, name: String, price: BigDecimal, description: Option[String])
implicit val productRead: Read[Product] = Read.derived[Product]

session.list(sql"SELECT id, name, price, description FROM products".query[Product])
```

### 7. Targeted Row Access Helpers

```scala
session.unique(sql"SELECT COUNT(*) FROM products".query[Int])      // exactly one row or error
session.option(sql"SELECT id FROM products WHERE id = ${42}".query[Int]) // maybe one row
session.stream(sql"SELECT id FROM products".query[Int])            // LazyList
```

### 8. Batch Inserts

```scala
client.executeBatch(
  "INSERT INTO users (id, name) VALUES (?, ?)",
  List(Seq(Param(1), Param("Ada")), Seq(Param(2), Param("Grace")))
)
```

## Core Abstractions

| Abstraction              | Purpose                             | Notes                                                    |
| ------------------------ | ----------------------------------- | -------------------------------------------------------- |
| `ConnectionConfig`       | Declarative connection + tuning     | Load from config/env; supports session params, timeouts  |
| `ConnectionProvider`     | Pluggable acquisition strategy      | Simple vs Hikari; can extend for metrics                 |
| `SnowflakeClient`        | High-level entry point              | Offers transactions & session creation; returns `Either` |
| `SnowflakeSession`       | Scoped operations on one connection | Focused read/update helpers & transaction nesting        |
| `Query0[A]` / `Command0` | Prepared executable units           | Produced from `Fragment` + decoder/encoder               |
| `Get` / `Put` / `Read`   | Typeclasses for column, param, row  | Macro derivation for `Read` case classes                 |
| `Fragment`               | SQL + parameters vector             | Pure, composable, no side effects                        |

### Error Model

All public operations return `Either[SnowflakeError, A]` allowing safe composition via `for` comprehension. Categories include connection, query, decoding & transaction errors. Query IDs (when available) surface observability hooks without hard dependency on logging frameworks.

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                        User Code (Pure)                        │
├────────────────────────────────────────────────────────────────┤
│                 SnowflakeClient  ── creates ──┐                │
│                                               │ withSession    │
│                 SnowflakeSession (scoped) ◄───┘                │
├────────────────────────────────────────────────────────────────┤
│ Fragments & Interpolator  |  Typeclasses (Get/Put/Read)        │
├────────────────────────────────────────────────────────────────┤
│ QueryExecutor (JDBC ops + result decoding + query id capture)  │
├────────────────────────────────────────────────────────────────┤
│ ConnectionProvider (Simple | Hikari | Custom)                  │
├────────────────────────────────────────────────────────────────┤
│                Snowflake JDBC Driver & Network                 │
└────────────────────────────────────────────────────────────────┘
```

Sessions give you row-level helpers (`list`, `unique`, `option`, `stream`) and transaction scopes that nest cleanly inside a single borrowed connection.

### Extensibility Points

- Swap or wrap `ConnectionProvider` for metrics/tracing
- Decorate `QueryExecutor` for auditing or timing
- Provide extra `Get` / `Put` instances for custom Snowflake types (VARIANT, ARRAY, etc.)

## SQL Interpolator Highlights

- Values become `?` placeholders automatically
- Collections become parenthesized lists for `IN` clauses
- Accepts existing `Fragment` pieces for advanced composition
- Avoids manual off-by-one parameter mistakes

Multi-line convenience:

```scala
sql"""SELECT id, name
       |FROM users
       |WHERE created_at > ${Instant.now().minusSeconds(86400)}""".stripMargin().query[User]
```

## Running Tests

```bash
sbt test # unit tests
SNOWFLAKE_INTEGRATION_TEST=true sbt test # with integration tests
```

## Credits

Inspired heavily by the design philosophy of [Skunk](https://github.com/typelevel/skunk) (type-safe SQL, fragments, functional resource safety).

## License

MIT – see `LICENSE`.

---
