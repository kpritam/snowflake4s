import sbt.*

object Dependencies {
  lazy val snowflakeJdbc = "net.snowflake" % "snowflake-jdbc" % "3.27.0"
  lazy val typesafeConfig = "com.typesafe" % "config" % "1.4.5"
  lazy val scalaReflect = "org.scala-lang" % "scala-reflect" % "2.13.17"
  lazy val hikariCP = "com.zaxxer" % "HikariCP" % "5.1.0"
  lazy val munit = "org.scalameta" %% "munit" % "1.2.1"
  lazy val mockitoScala = "org.mockito" %% "mockito-scala" % "2.0.0"
}
