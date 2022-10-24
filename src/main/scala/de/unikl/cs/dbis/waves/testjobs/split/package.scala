package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.sql.{SparkSession,DataFrame}

import de.unikl.cs.dbis.waves.util.Logger
import de.unikl.cs.dbis.waves.split.Splitter
import de.unikl.cs.dbis.waves.WavesTable._
import org.apache.spark.sql.SaveMode

package object split {
  def runSplitter[T](spark: SparkSession, jobConfig: JobConfig, splitter: Splitter[T])
    = run(spark, jobConfig, (df) => df.saveAsWaves(splitter, jobConfig.wavesPath))

  def runPlain(spark: SparkSession, jobConfig: JobConfig)
    = run(spark, jobConfig, (df) => df.write.mode(SaveMode.Overwrite).waves(jobConfig.wavesPath, df.schema))

  def run(spark: SparkSession, jobConfig: JobConfig, job: DataFrame => Unit) = {
    Logger.log("split-start")
    val df = spark.read.json(jobConfig.inputPath)
    job(df)
    val relation = spark.read.waves(jobConfig.wavesPath).getWavesTable.get
    Logger.log("split-done", relation.diskSize())
    relation.defrag()
    relation.vacuum()
    Logger.log("split-cleanup-end", relation.diskSize())

    Logger.flush(spark.sparkContext.hadoopConfiguration)
    spark.stop()
  }
}
