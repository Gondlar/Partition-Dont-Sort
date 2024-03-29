package de.unikl.cs.dbis.waves.testjobs.query

import org.apache.spark.sql.{SparkSession,DataFrame}

import de.unikl.cs.dbis.waves.util.Logger
import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.WavesTable._
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.partitions.visitors.operations._

trait QueryRunner {

  def run(spark: SparkSession, jobConfig: JobConfig, query: DataFrame => String): Unit
    = run(spark, jobConfig, Seq(query))

  def run(spark: SparkSession, jobConfig: JobConfig, queries: Seq[DataFrame => String]) = {
    // Prepare the stuff we do not want to measure
    val basePath = jobConfig.wavesPath
    val tree = PartitionTreeHDFSInterface(spark, basePath).read().get
    val folders = tree.folders(basePath).map(_.filename)
    val useWaves = jobConfig.useWaves
    val schemaModificationsEnabled = jobConfig.modifySchema

    // Create the dataframe
    Logger.log("query-start", getClass().getName())
    val df = if (useWaves) {
      spark.read.waves(basePath)
     } else {
      if (schemaModificationsEnabled) {
        spark.read.option("mergeSchema", true).parquet(folders:_*)
      } else {
        spark.read.parquet(folders:_*)
      }
    }

    // Run the query
    Logger.log("parameter-use-waves", useWaves)
    for ((query, index) <- queries.zipWithIndex) {
      Logger.log(s"query-run-$index")
      val toLog = query(df)
      Logger.log(s"query-end-$index", toLog)
    }

    // Log finished and perform cleanup
    Logger.flush(spark.sparkContext.hadoopConfiguration)
    spark.stop()
  }

  def runWithVoid(spark: SparkSession, jobConfig: JobConfig, queries: Seq[DataFrame => Unit]): Unit
    = run(spark, jobConfig, queries.map(query => (df: DataFrame) => {query(df); ""}))
}
