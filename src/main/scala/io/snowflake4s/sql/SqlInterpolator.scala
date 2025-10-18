package io.snowflake4s.sql

import io.snowflake4s.jdbc.Put

import scala.annotation.tailrec
import scala.language.implicitConversions

/** String interpolator to build SQL fragments in a safe and composable fashion. */
final class SqlInterpolator(private val sc: StringContext) extends AnyVal {

  import SqlInterpolator.SingleFragment

  /** Builds a SQL fragment from the string context and arguments.
    *
    * @param args
    *   the fragment arguments
    * @return
    *   a SQL fragment
    */
  def sql(args: SingleFragment*): Fragment =
    mkFragment(args.toList, appendWhitespace = false)

  /** Builds a SQL fragment with automatic whitespace appending.
    *
    * @param args
    *   the fragment arguments
    * @return
    *   a SQL fragment
    */
  def fr(args: SingleFragment*): Fragment =
    mkFragment(args.toList, appendWhitespace = true)

  /** Builds a SQL fragment without automatic whitespace.
    *
    * @param args
    *   the fragment arguments
    * @return
    *   a SQL fragment
    */
  def fr0(args: SingleFragment*): Fragment =
    mkFragment(args.toList, appendWhitespace = false)

  private def mkFragment(args: List[SingleFragment], appendWhitespace: Boolean): Fragment = {
    val literals = sc.parts.toList.map(Fragment.const0)
    val fragments = args.map(_.fragment)

    @tailrec
    def loop(acc: Fragment, remaining: List[(Fragment, Fragment)]): Fragment =
      remaining match {
        case (literal, expr) :: tail =>
          val next = appendIfNonEmpty(acc, literal)
          val withExpr = appendIfNonEmpty(next, expr)
          loop(withExpr, tail)
        case Nil => acc
      }

    val combined = loop(Fragment.empty, literals.zipAll(fragments, Fragment.empty, Fragment.empty))
    if (appendWhitespace) combined +~+ Fragment.const0("") else combined
  }

  private def appendIfNonEmpty(base: Fragment, addition: Fragment): Fragment =
    if (addition.isEmpty) base else base ++ addition
}

object SqlInterpolator {

  final case class SingleFragment(fragment: Fragment) extends AnyVal

  object SingleFragment {
    val empty: SingleFragment = SingleFragment(Fragment.empty)

    implicit def fromFragment(fragment: Fragment): SingleFragment = SingleFragment(fragment)

    implicit def fromValue[A](value: A)(implicit put: Put[A]): SingleFragment =
      SingleFragment(Fragment.param(value))

    implicit def fromIterable[A](values: Iterable[A])(implicit put: Put[A]): SingleFragment =
      SingleFragment(Fragment.in(values))
  }

  trait ToSqlInterpolator {
    implicit def toSqlInterpolator(sc: StringContext): SqlInterpolator =
      new SqlInterpolator(sc)
  }

  object syntax extends ToSqlInterpolator

  implicit def stringContextToSqlInterpolator(sc: StringContext): SqlInterpolator =
    new SqlInterpolator(sc)
}
