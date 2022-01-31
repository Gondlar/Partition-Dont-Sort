import SchemaAssumptionFactory._

import de.unikl.cs.dbis.waves.{DefaultSource,WavesRelation}
import de.unikl.cs.dbis.waves.util.Logger

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.functions.col

object Main extends App {
  val conf = new SparkConf().setAppName("waves-test")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()
  Logger.log("job-start")

  //val df = spark.read.format("json").load("/home/patrick/Twitterdaten/twitter")
  //println(df.agg("user.id" -> "avg").head().apply(0))    // 269260424025919552
  //df.write.mode(SaveMode.Overwrite).format("de.unikl.cs.dbis.waves").save("out/")

  // val relation = new DefaultSource().createRelation(spark.sqlContext, Map("path" -> "out")).asInstanceOf[WavesRelation]
  // Logger.log("repartition-start")
  // relation.repartition("spill", "quoted_status")
  // Logger.log("repartition-end")
  
  val df2 = spark.read.format("de.unikl.cs.dbis.waves").load("out/")
  //val realCount = df.count()
  // Logger.log("count-start")
  // val myCount = df2.count()
  // Logger.log("count-end", myCount)
  
  Logger.log("countfilter-start")
  val countSome = df2.filter(col("quoted_status.user.name").startsWith("xx")).count()
  Logger.log("countfilter-end", countSome)
  //println(s"count on JSON: $realCount\ncount on waves: $myCount")

  Logger.log("job-end")
  Logger.printToStdout()
  spark.close()
}