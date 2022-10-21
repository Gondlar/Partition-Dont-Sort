package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.util.Logger

import de.unikl.cs.dbis.waves.WavesTable._

object PartialScanWaves {
  def main(args: Array[String]) : Unit = {
      val jobConfig = JobConfig.fromArgs(args)

      Logger.log("job-start")
      val spark = jobConfig.makeSparkSession("PartialScanWaves")
      
      Logger.log("partialWaves-start")
      val df = spark.read.waves(jobConfig.wavesPath)
      val count = df.filter(col(jobConfig.partialScanColumn).startsWith(jobConfig.scanValue)).count()
      Logger.log("partialWaves-end", count)

      Logger.log("job-end")
      Logger.flush(spark.sparkContext.hadoopConfiguration)
      spark.stop()
  }
}
