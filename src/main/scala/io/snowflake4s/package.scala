package io

import scala.language.implicitConversions

/** Main package for snowflake4s - A functional Scala library for Snowflake.
  *
  * Example:
  * {{{
  * import io.snowflake4s._
  * import java.time.Instant
  *
  * final case class User(id: Int, name: String, email: Option[String], createdAt: Instant)
  * implicit val userRead: Read[User] = Read.derived[User]
  *
  * val result: Either[SnowflakeError, QueryResult[User]] = for {
  *   cfg <- ConnectionConfig.load()
  *   client = SnowflakeClient.pooled(cfg)
  *   users <- client.withSession(
  *     _.query[User](
  *       "SELECT id, name, email, created_at FROM users WHERE id > ?",
  *       params = Seq(Param(100))
  *     )
  *   )
  *   _ <- client.close()
  * } yield users
  * }}}
  */
package object snowflake4s {

  // Re-export main types for convenience
  type Read[A] = jdbc.Read[A]
  val Read: jdbc.Read.type = jdbc.Read

  type Get[A] = jdbc.Get[A]
  val Get: jdbc.Get.type = jdbc.Get

  type Put[A] = jdbc.Put[A]
  val Put: jdbc.Put.type = jdbc.Put

  type Fragment = _root_.io.snowflake4s.sql.Fragment
  val Fragment: _root_.io.snowflake4s.sql.Fragment.type = _root_.io.snowflake4s.sql.Fragment

  type Command[A] = _root_.io.snowflake4s.sql.Command[A]
  val Command: _root_.io.snowflake4s.sql.Command.type = _root_.io.snowflake4s.sql.Command

  type Command0 = _root_.io.snowflake4s.sql.Command0
  val Command0: _root_.io.snowflake4s.sql.Command0.type = _root_.io.snowflake4s.sql.Command0

  type Query[A, B] = _root_.io.snowflake4s.sql.Query[A, B]
  val Query: _root_.io.snowflake4s.sql.Query.type = _root_.io.snowflake4s.sql.Query

  type Query0[A] = _root_.io.snowflake4s.sql.Query0[A]
  val Query0: _root_.io.snowflake4s.sql.Query0.type = _root_.io.snowflake4s.sql.Query0

  type Param = _root_.io.snowflake4s.sql.Param
  val Param: _root_.io.snowflake4s.sql.Param.type = _root_.io.snowflake4s.sql.Param

  implicit def sqlStringContext(sc: StringContext): _root_.io.snowflake4s.sql.SqlInterpolator =
    new _root_.io.snowflake4s.sql.SqlInterpolator(sc)
}
