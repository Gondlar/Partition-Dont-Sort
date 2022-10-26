package de.unikl.cs.dbis.waves.testjobs.query

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.util.Logger

object Toptweeter extends QueryRunner {
  def main(args: Array[String]) : Unit = {
      val jobConfig = JobConfig.fromArgs(args)
      val spark = jobConfig.makeSparkSession("Toptweeter")
      run(spark, jobConfig, df => {
        df.createOrReplaceTempView("twitter")
        spark.sql("""
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
          AND retweets.day = toptweets.day""").collect().head.getLong(2).toString()
      })
  }
}
