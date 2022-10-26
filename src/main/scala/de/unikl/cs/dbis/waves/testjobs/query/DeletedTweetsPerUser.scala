package de.unikl.cs.dbis.waves.testjobs.query

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.util.Logger

object DeletedTweetsPerUser extends QueryRunner{
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession("DeletedTweetsPerUser")
    run(spark, jobConfig, df => {
      df.filter(col("delete.status.user_id").isNotNull)
        .groupBy(col("delete.status.user_id"))
        .count()
        .collect()
        .size
        .toString
    })
  }
}
