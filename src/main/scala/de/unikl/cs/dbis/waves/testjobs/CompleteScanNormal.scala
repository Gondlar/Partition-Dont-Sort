package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.util.Logger

object CompleteScanNormal {
  def main(args: Array[String]) : Unit = {
      Logger.log("job-start")
      val appName = "SystemSetupTestJob"
      val conf = new SparkConf().setAppName(appName)
      conf.setMaster("local") // comment this line to run on the cluster
      val spark = SparkSession.builder().config(conf).getOrCreate()
      
      Logger.log("completeParquet-start")
      val df = spark.read.format(JobConfig.parquetFormat).load(JobConfig.parquetPath)
      val count = df.filter(col(JobConfig.completeScanColumn).startsWith(JobConfig.completeScanValue)).count()
      Logger.log("completeParquet-end", count)

      Logger.log("job-end")
      Logger.flush(spark.sparkContext.hadoopConfiguration)
      spark.stop()
  }
}
