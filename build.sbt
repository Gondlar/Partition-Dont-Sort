
// Define some versions
val sparkVersion = "3.2.0"
val scalatestVersion = "3.2.11"
scalaVersion := "2.12.15"

// Project Metadata
name := "Waves"
organization := "de.uni-kl.cs.dbis"
version := "1.0"

// Dependancies
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestVersion % "test"
libraryDependencies += "org.scalatest" %% "scalatest-wordspec" % scalatestVersion % "test"

// Compile settings
scalacOptions ++= Seq("-unchecked", "-deprecation")

logBuffered in Test := false
classLoaderLayeringStrategy in Test := ClassLoaderLayeringStrategy.Flat // Needed for coverage tests with spark
                                                                        // Otherwise the ClassLoader does not find parquet

// Overage settings
coverageExcludedFiles := Seq(
    ".*\\/deprecated\\/.*",
    ".*\\/Main",
    ".*\\/testjobs/.*"
).mkString(";")
