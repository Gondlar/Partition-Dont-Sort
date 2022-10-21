package de.unikl.cs.dbis.waves

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.Suite
import org.scalactic.source.Position
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkFixture extends BeforeAndAfterAll { this: Suite =>

  var spark : SparkSession = null

  override protected def beforeAll(): Unit = {
    val conf = new SparkConf().setAppName(SparkFixture.TEST_SESSION_NAME)
    spark = SparkSession.builder().config(conf).master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    super.beforeAll() // To be stackable, must call super.beforeAll
  }

  override protected def afterAll(): Unit = {
    // Do not close the spark session, it gets reused by the Session builder
    // If we do close it here, tests may fail because the builder chose to reuse
    // the session while we asynchronously close it here
    // spark.close()

    super.afterAll() // To be stackable, must call super.afterAll
  }
}

object SparkFixture {
  val TEST_SESSION_NAME = "waves-scalatest"
}
