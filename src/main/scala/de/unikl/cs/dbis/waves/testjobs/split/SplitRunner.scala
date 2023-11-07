package de.unikl.cs.dbis.waves.testjobs.split

import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.Path

import de.unikl.cs.dbis.waves.util.Logger
import de.unikl.cs.dbis.waves.split.Splitter
import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.WavesTable._
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface

trait SplitRunner {
  def runSplitter[T](spark: SparkSession, jobConfig: JobConfig, splitter: Splitter[T])
    = run(spark, jobConfig, (df) => df.saveAsWaves(splitter.modifySchema(jobConfig.modifySchema), jobConfig.wavesPath))

  def runPlain(spark: SparkSession, jobConfig: JobConfig)
    = run(spark, jobConfig, (df) => df.write.mode(SaveMode.Overwrite).waves(jobConfig.wavesPath, df.schema))

  def run(spark: SparkSession, jobConfig: JobConfig, job: DataFrame => Unit) = {
    if (jobConfig.cleanWavesPath)
      PartitionTreeHDFSInterface(spark, jobConfig.wavesPath).fs
        .delete(new Path(jobConfig.wavesPath), true)
    Logger.log("read-dataframe", jobConfig.inputPath)
    val df = spark.read.json(jobConfig.inputPath)
    Logger.log("split-start", this.getClass().getName())
    job(df)
    Logger.log("split-done")
    val relation = spark.read.waves(jobConfig.wavesPath).getWavesTable.get
    Logger.log("metadata-bytesize", relation.diskSize())

    Logger.flush(spark.sparkContext.hadoopConfiguration)
    spark.stop()
  }
}
