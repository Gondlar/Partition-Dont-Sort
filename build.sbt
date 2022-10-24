
// Define some versions
val sparkVersion = "3.2.0"
val scalatestVersion = "3.2.11"
scalaVersion := "2.12.15"

// Project Metadata
name := "Waves"
organization := "de.unikl.cs.dbis"
version := "1.0"

// Full Stack Tests
lazy val FullStackTest = config("fullstack") extend(Test)
lazy val root = (project in file("."))
  .configs(FullStackTest)
  .settings(inConfig(FullStackTest)(Defaults.testSettings))
FullStackTest / parallelExecution := false

// Dependancies
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % "test,fullstack"
libraryDependencies += "org.scalatest" %% "scalatest-wordspec" % scalatestVersion % "test,fullstack"

// Compile settings
scalacOptions ++= Seq("-unchecked", "-deprecation")

// Needed for spark, Otherwise the ClassLoader does not find parquet
classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat

//testOptions += Tests.Argument("-oF")

Test / logBuffered := false

// Overage settings
coverageExcludedFiles := Seq(
    ".*\\/deprecated\\/.*",
    ".*\\/Main",
    ".*\\/testjobs/.*"
).mkString(";")
