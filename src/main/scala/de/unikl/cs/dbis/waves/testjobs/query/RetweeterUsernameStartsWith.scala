package de.unikl.cs.dbis.waves.testjobs.query

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.util.Logger

object RetweeterUsernameStartsWith extends QueryRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession("RetweeterUsernameStartsWith")
    run(spark, jobConfig, df => {
      df.filter(col(jobConfig.partialScanColumn).startsWith(jobConfig.scanValue)).count().toString
    })
  }
}
