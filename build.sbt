ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided",
  "mysql" % "mysql-connector-java" % "8.0.32",
  "com.typesafe" % "config" % "1.4.2"
)

lazy val root = (project in file("."))
  .settings(
    name := "big-data-engineer-hw"
  )
