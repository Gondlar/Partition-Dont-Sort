import SchemaAssumptionFactory._

import de.unikl.cs.dbis.waves.{DefaultSource,WavesRelation}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.functions.col

object Main extends App {
  val conf = new SparkConf().setAppName("waves-test")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

  val df = spark.read.format("json").load("/home/patrick/Twitterdaten/twitter")
  println(df.agg("user.id" -> "avg").head().apply(0))    // 269260424025919552
  df.write.mode(SaveMode.Overwrite).format("de.unikl.cs.dbis.waves").save("out/")

  val relation = new DefaultSource().createRelation(spark.sqlContext, Map("path" -> "out")).asInstanceOf[WavesRelation]
  relation.repartition("spill", "quoted_status")
  
  val df2 = spark.read.format("de.unikl.cs.dbis.waves").load("out/")
  val realCount = df.count()
  val myCount = df2.count()
  
  val countSome = df2.filter(col("quoted_status.user.name").startsWith("xx")).count()
  println(s"countSome = $countSome")
  println(s"count on JSON: $realCount\ncount on waves: $myCount")

  spark.close()
}