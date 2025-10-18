import Dependencies._

ThisBuild / scalaVersion := "2.13.17"
ThisBuild / organization := "io.github.kpritam"
ThisBuild / organizationName := "snowflake4s"

lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-encoding",
    "utf8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Xlint",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Xsource:3"
  ),
  Compile / javacOptions ++= Seq(
    "-source",
    "11",
    "-target",
    "11"
  )
)

lazy val macros = (project in file("macros"))
  .settings(commonSettings)
  .settings(
    name := "snowflake4s-macros",
    libraryDependencies ++= Seq(
      scalaReflect
    )
  )

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "snowflake4s",
    libraryDependencies ++= Seq(
      snowflakeJdbc,
      typesafeConfig,
      hikariCP,
      munit % Test,
      mockitoScala % Test
    ),
    Test / publishArtifact := false,
    pomIncludeRepository := { _ => false },
    licenses += ("MIT", url("https://opensource.org/licenses/MIT")),
    homepage := Some(url("https://github.com/kpritam/snowflake4s")),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/kpritam/snowflake4s"),
        "scm:git:git@github.com:kpritam/snowflake4s.git"
      )
    ),
    developers := List(
      Developer(
        id = "kpritam",
        name = "Pritam Kadam",
        email = "phkdam2008@gmail.com",
        url = url("https://github.com/kpritam")
      )
    )
  )
  .dependsOn(macros)

lazy val examples = (project in file("examples"))
  .settings(commonSettings)
  .settings(
    name := "snowflake4s-examples",
    publish / skip := true,
    libraryDependencies ++= Seq(
      munit % Test
    )
  )
  .dependsOn(root)
