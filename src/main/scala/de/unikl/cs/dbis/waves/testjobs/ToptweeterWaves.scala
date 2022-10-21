package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.util.Logger

import de.unikl.cs.dbis.waves.WavesTable._

object ToptweeterWaves {
  def main(args: Array[String]) : Unit = {
      val jobConfig = JobConfig.fromArgs(args)

      Logger.log("job-start")
      val spark = jobConfig.makeSparkSession("ToptweeterWaves")
      
      Logger.log("toptweeterWaves-start")
      val df = spark.read.waves(jobConfig.wavesPath)
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

      Logger.log("toptweeterWaves-end", count)

      Logger.log("job-end")
      Logger.flush(spark.sparkContext.hadoopConfiguration)
      spark.stop()
  }
}
