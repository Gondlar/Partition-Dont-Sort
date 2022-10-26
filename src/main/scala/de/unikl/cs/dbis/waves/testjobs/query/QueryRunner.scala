package de.unikl.cs.dbis.waves.testjobs.query

import org.apache.spark.sql.{SparkSession,DataFrame}

import de.unikl.cs.dbis.waves.util.Logger
import de.unikl.cs.dbis.waves.split.Splitter
import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.WavesTable._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._

trait QueryRunner {

  def run(spark: SparkSession, jobConfig: JobConfig, query: DataFrame => String) = {
    Logger.log("query-start", getClass().getName())

    val wavesDf = spark.read.waves(jobConfig.wavesPath)
    val df = if (jobConfig.useWaves) wavesDf else {
      val table = wavesDf.getWavesTable.get
      val folders = table.partitionTree.buckets.map(_.folder(table.basePath).filename)
      spark.read.parquet(folders:_*)
    }

    Logger.log("query-run", jobConfig.useWaves)
    val toLog = query(df)

    Logger.log("query-end", toLog)
    Logger.flush(spark.sparkContext.hadoopConfiguration)
    spark.stop()
  }
}
