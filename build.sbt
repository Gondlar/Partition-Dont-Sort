import bloop.integrations.sbt.BloopDefaults

// Define some versions
val sparkVersion = "3.2.0"
val scalatestVersion = "3.2.11"

// Project Metadata

ThisBuild / organization := "de.unikl.cs.dbis"
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / version := "1.0"

lazy val FullStackTest = config("fullstack") extend(Test)

lazy val waves = project
  .in(file("."))
  .configs(FullStackTest)
  .settings(
    name := "waves",

    // Full Stack Tests
    inConfig(FullStackTest)(Defaults.testSettings ++ BloopDefaults.configSettings),
    FullStackTest / fork := true,
    FullStackTest / testGrouping := (FullStackTest / definedTests).value.map { suite =>
      Tests.Group(suite.name, Seq(suite), Tests.SubProcess(ForkOptions()))
    },

    // Dependancies
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.scalatest" %% "scalatest" % scalatestVersion % "test,fullstack",
      "org.scalatest" %% "scalatest-wordspec" % scalatestVersion % "test,fullstack"
    ),

    //testOptions += Tests.Argument("-oF"),
    Test / logBuffered := false,

    // Compile settings
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      //"-Xdisable-assertions", // might want to disable this for testing
    ),

    // Needed for spark, Otherwise the ClassLoader does not find parquet
    classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,

    // Coverage settings
    coverageExcludedFiles := Seq(
        ".*\\/deprecated\\/.*",
        ".*\\/Main"
    ).mkString(";")
  )
