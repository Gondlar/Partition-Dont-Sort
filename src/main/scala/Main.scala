import de.unikl.cs.dbis.waves.WavesTable
import de.unikl.cs.dbis.waves.util.Logger
import de.unikl.cs.dbis.waves.util.SchemaMetric

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object Main extends App {
  val conf = new SparkConf().setAppName("waves-test")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()
  Logger.log("job-start")

  // val df = spark.read.format("json").load("/home/patrick/Twitterdaten/twitter")
  // println(df.agg("user.id" -> "avg").head().apply(0))    // 269260424025919552
  // df.write.mode(SaveMode.Overwrite).format("de.unikl.cs.dbis.waves").save("out/")

  // val relation = WavesTable("Repartition out/", spark, "out/", CaseInsensitiveStringMap.empty())
  // Logger.log("repartition-start", relation.diskSize())
  // relation.partition( 2*1024*1024 //spark.sparkContext.hadoopConfiguration.getLong("dfs.blocksize", 128*1024*1024)
  //                   , 10*1024*1024
  //                   , SchemaMetric.switchMetric _)
  // relation.repartition("quoted_status")
  // relation.repartition("retweeted_status", "absent")
  // relation.repartition("delete", "absent", "absent")
  // Logger.log("repartition-end", relation.diskSize())
  
  val df2 = spark.read.format("de.unikl.cs.dbis.waves").load("out/")
  // val realCount = df.count()
  // Logger.log("count-start")
  // val myCount = df2.count()
  // Logger.log("count-end", myCount)
  
  Logger.log("countfilter-start")
  val countSome = df2.filter(col("quoted_status.user.name").startsWith("xx")).count()
  Logger.log("countfilter-end", countSome)
  // println(s"count on JSON: $realCount\ncount on waves: $myCount")

  Logger.log("job-end")
  Logger.printToStdout()
  spark.close()
}
