package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.util.Logger

object DeletedTweetsPerUser {
  def main(args: Array[String]) : Unit = {
      Logger.log("job-start")
      val appName = "DeletedTweetsPerUser"
      val conf = new SparkConf().setAppName(appName)
      // conf.setMaster("local") // comment this line to run on the cluster
      val spark = SparkSession.builder().config(conf).getOrCreate()
      
      Logger.log("deleted-start")
      val df = spark.read.format(JobConfig.parquetFormat).load(JobConfig.parquetPath)
      
      val count = df.filter(col("delete.status.user_id").isNotNull)
                    .groupBy(col("delete.status.user_id"))
                    .count()
                    .collect().size // make sparkactuallyrun the query

      Logger.log("deleted-end", count)

      Logger.log("job-end")
      Logger.flush(spark.sparkContext.hadoopConfiguration)
      spark.stop()
  }
}
