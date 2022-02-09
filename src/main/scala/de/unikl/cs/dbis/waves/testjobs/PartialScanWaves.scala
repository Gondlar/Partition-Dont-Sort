package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.util.Logger

object PartialScanWaves {
  def main(args: Array[String]) : Unit = {
      Logger.log("job-start")
      val appName = "SystemSetupTestJob"
      val conf = new SparkConf().setAppName(appName)
      conf.setMaster("local") // comment this line to run on the cluster
      val spark = SparkSession.builder().config(conf).getOrCreate()
      
      Logger.log("partialWaves-start")
      val df = spark.read.format(JobConfig.wavesFormat).load(JobConfig.wavesPath)
      val count = df.filter(col(JobConfig.partialScanColumn).startsWith(JobConfig.partialScanValue)).count()
      Logger.log("partialWaves-end", count)

      Logger.log("job-end")
      Logger.flush(spark.sparkContext.hadoopConfiguration)
      spark.stop()
  }
}
