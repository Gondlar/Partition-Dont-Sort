package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.util.Logger

object PartialScanNormal {
  def main(args: Array[String]) : Unit = {
      val jobConfig = JobConfig.fromArgs(args)

      Logger.log("job-start")
      val spark = jobConfig.makeSparkSession("PartialScanNormal")
      
      Logger.log("partialParquet-start")
      val df = spark.read.parquet(jobConfig.parquetPath)
      val count = df.filter(col(jobConfig.partialScanColumn).startsWith(jobConfig.scanValue)).count()
      Logger.log("partialParquet-end", count)

      Logger.log("job-end")
      Logger.flush(spark.sparkContext.hadoopConfiguration)
      spark.stop()
  }
}
