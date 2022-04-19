package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.util.Logger

object Toptweeter {
  def main(args: Array[String]) : Unit = {
      Logger.log("job-start")
      val appName = "Toptweeter"
      val conf = new SparkConf().setAppName(appName)
      // conf.setMaster("local") // comment this line to run on the cluster
      val spark = SparkSession.builder().config(conf).getOrCreate()
      
      Logger.log("toptweeter-start")
      val df = spark.read.format(JobConfig.parquetFormat).load(JobConfig.parquetPath)
      df.createOrReplaceTempView("twitter")

      val count = spark.sql("""
        WITH retweets AS (
          SELECT SUBSTR(created_at, 0, 10) as day,
                 retweeted_status.user.id as retweeter,
                 count(*) as retweetcount
          FROM twitter
          WHERE retweeted_status.user.id IS NOT NULL
          GROUP BY day, retweeter
        ), toptweets AS (
          SELECT day, MAX(retweetcount) as top
          FROM retweets
          GROUP BY day
        )
        SELECT retweets.day, retweeter, top
        FROM retweets, toptweets
        WHERE top = retweetcount
        AND retweets.day = toptweets.day""").collect().size

      Logger.log("toptweeter-end", count)

      Logger.log("job-end")
      Logger.flush(spark.sparkContext.hadoopConfiguration)
      spark.stop()
  }
}