package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.util.Logger

object CompleteScanNormal {
  def main(args: Array[String]) : Unit = {
      val jobConfig = JobConfig.fromArgs(args)

      Logger.log("job-start")
      val spark = jobConfig.makeSparkSession("CompleteScanNormal")
      
      Logger.log("completeParquet-start")
      val df = spark.read.parquet(jobConfig.parquetPath)
      val count = df.filter(col(jobConfig.completeScanColumn).startsWith(jobConfig.scanValue)).count()
      Logger.log("completeParquet-end", count)

      Logger.log("job-end")
      Logger.flush(spark.sparkContext.hadoopConfiguration)
      spark.stop()
  }
}
