package io.snowflake4s.sql

import io.snowflake4s.jdbc.{Put, Read}

/** Represents a SQL fragment with its parameters for prepared statements. */
final case class Fragment(sql: String, params: Vector[Param]) {

  /** Concatenates this fragment with another fragment.
    *
    * @param other
    *   the fragment to append
    * @return
    *   a new combined fragment
    */
  def ++(other: Fragment): Fragment =
    Fragment(sql + other.sql, params ++ other.params)

  /** Concatenates this fragment with another, adding a space if needed.
    *
    * @param other
    *   the fragment to append
    * @return
    *   a new combined fragment
    */
  def +~+(other: Fragment): Fragment = {
    val needsSpace = sql.nonEmpty && other.sql.nonEmpty && !sql.last.isWhitespace && !other.sql.head.isWhitespace
    val combinedSql = if (needsSpace) s"$sql ${other.sql}" else sql + other.sql
    Fragment(combinedSql, params ++ other.params)
  }

  /** Appends a string to this fragment.
    *
    * @param sqlPart
    *   the string to append
    * @return
    *   a new fragment with the string appended
    */
  def append(sqlPart: String): Fragment =
    if (sqlPart.isEmpty) this else Fragment(sql + sqlPart, params)

  /** Prepends a string to this fragment.
    *
    * @param sqlPart
    *   the string to prepend
    * @return
    *   a new fragment with the string prepended
    */
  def prepend(sqlPart: String): Fragment =
    if (sqlPart.isEmpty) this else Fragment(sqlPart + sql, params)

  /** Strips margin characters from the SQL string.
    *
    * @param marginChar
    *   the margin character to strip
    * @return
    *   a new fragment with margin stripped
    */
  def stripMargin(marginChar: Char = '|'): Fragment =
    Fragment(sql.stripMargin(marginChar), params)

  private[sql] def addParams(extra: Vector[Param]): Fragment =
    Fragment(sql, params ++ extra)

  /** Checks if this fragment is empty.
    *
    * @return
    *   true if both SQL and params are empty
    */
  def isEmpty: Boolean = sql.isEmpty && params.isEmpty

  /** Converts this fragment to a query.
    *
    * @param read
    *   the reader for the result type
    * @tparam A
    *   the result type
    * @return
    *   a query object
    */
  def query[A](implicit read: Read[A]): Query0[A] =
    Query0(this, read)

  /** Converts this fragment to a command.
    *
    * @return
    *   a command object
    */
  def update: Command0 =
    Command0(this)
}

object Fragment {

  /** An empty fragment. */
  val empty: Fragment = Fragment("", Vector.empty)

  /** Creates a fragment from a SQL string.
    *
    * @param sql
    *   the SQL string
    * @return
    *   a new fragment
    */
  def const(sql: String): Fragment = Fragment(sql, Vector.empty)

  /** Creates a fragment from a SQL string.
    *
    * @param sql
    *   the SQL string
    * @return
    *   a new fragment
    */
  def const0(sql: String): Fragment = Fragment(sql, Vector.empty)

  /** Creates a fragment with a single parameter.
    *
    * @param value
    *   the parameter value
    * @param put
    *   the encoder for the value type
    * @tparam A
    *   the value type
    * @return
    *   a new fragment
    */
  def param[A](value: A)(implicit put: Put[A]): Fragment =
    Fragment("?", Vector(Param(value)))

  /** Creates a fragment from a parameter.
    *
    * @param param
    *   the parameter
    * @return
    *   a new fragment
    */
  def fromParam(param: Param): Fragment = Fragment("?", Vector(param))

  /** Creates a fragment with multiple parameters.
    *
    * @param params
    *   the parameters
    * @return
    *   a new fragment
    */
  def params(params: Vector[Param]): Fragment =
    if (params.isEmpty) Fragment("", Vector.empty)
    else Fragment(Vector.fill(params.size)("?").mkString(", "), params)

  /** Creates a fragment for an IN clause.
    *
    * @param values
    *   the values for the IN clause
    * @param put
    *   the encoder for the value type
    * @tparam A
    *   the value type
    * @return
    *   a new fragment
    */
  def in[A](values: Iterable[A])(implicit put: Put[A]): Fragment = {
    val list = values.toList
    if (list.isEmpty) Fragment("(SELECT 1 WHERE 1 = 0)", Vector.empty)
    else Fragment(list.map(_ => "?").mkString("(", ", ", ")"), list.map(Param(_)).toVector)
  }

  private[sql] def apply(sql: String, params: List[Param]): Fragment = new Fragment(sql, params.toVector)
}
