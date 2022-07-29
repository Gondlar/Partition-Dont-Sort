package de.unikl.cs.dbis.waves

import org.scalatest.Suite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

trait DataFrame extends Spark with Schema { this: Suite =>

  var df : org.apache.spark.sql.DataFrame = null
  var emptyDf : org.apache.spark.sql.DataFrame = null

  override def beforeEach(): Unit = {
    super.beforeEach()

    val rdd : RDD[Row] = spark.sparkContext.parallelize(data, 2)
    df = spark.sqlContext.createDataFrame(rdd, schema)
    val emptyRdd : RDD[Row] = spark.sparkContext.parallelize(Seq[Row](), 2)
    emptyDf = spark.sqlContext.createDataFrame(emptyRdd, schema)
  }
}
