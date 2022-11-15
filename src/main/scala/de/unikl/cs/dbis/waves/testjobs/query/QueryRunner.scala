package de.unikl.cs.dbis.waves.testjobs.query

import org.apache.spark.sql.{SparkSession,DataFrame}

import de.unikl.cs.dbis.waves.util.Logger
import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.WavesTable._
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.partitions.visitors.operations._

trait QueryRunner {

  def run(spark: SparkSession, jobConfig: JobConfig, query: DataFrame => String) = {
    // Prepare the stuff we do not want to measure
    val basePath = jobConfig.wavesPath
    val tree = PartitionTreeHDFSInterface(spark, basePath).read().get
    val folders = tree.buckets.map(_.folder(basePath).filename)
    val useWaves = jobConfig.useWaves

    // Create the dataframe
    Logger.log("query-start", getClass().getName())
    val df = if (useWaves) {
      spark.read.waves(basePath)
     } else {
      spark.read.parquet(folders:_*)
    }

    // Run the query
    Logger.log("query-run", useWaves)
    val toLog = query(df)

    // Log finished and perform cleanup
    Logger.log("query-end", toLog)
    Logger.flush(spark.sparkContext.hadoopConfiguration)
    spark.stop()
  }
}
