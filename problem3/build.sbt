ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.4" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.2.4" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.2.4" % "provided",
)

lazy val root = (project in file("."))
  .settings(
    name := "OutlierDetection"
  )
