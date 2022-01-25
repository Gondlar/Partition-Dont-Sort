import SchemaAssumptionFactory._

import de.unikl.cs.dbis.waves.DefaultSource
import de.unikl.cs.dbis.waves.LocalSchemaWriteSupport
import de.unikl.cs.dbis.waves.SchemaTransforms

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession,SaveMode}
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.parquet.hadoop.ParquetOutputCommitter
import org.apache.spark.TaskContext
import org.apache.spark.internal.io.HadoopMapReduceCommitProtocol
import org.apache.parquet.hadoop.ParquetRecordWriter
import org.apache.hadoop.mapreduce.TaskAttemptContext
import de.unikl.cs.dbis.waves

// class PartitionedIndex(
//   val base : String,
//   var partitions: Array[String],
//   var schemata: Array[SchemaAssumptions]
// ) {
//   private def loadPartition(idx: Integer) {
//     println("Loading " ++ base ++ "/" ++ partitions(idx))
//   }

//   def load() {
//     for (idx <- 0 to partitions.length-1) {
//       loadPartition(idx);
//     }
//   }

//   def load(columns: String*) {
//     for (idx <- 0 to partitions.length-1) {
//       breakable {
//         for (col <- columns) {
//           if (schemata(idx).present(col)) {
//             loadPartition(idx)
//             break
//           }
//         }
//       }
//     }
//   }
// }

object Main extends App {
  val conf = new SparkConf().setAppName("waves-test")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

  //val df = spark.read.format("json").load("/media/ssd/json-benchmark/twitter-300")
  //println(df.agg("user.id" -> "avg").head().apply(0))    // 269260424025919552
  // df.write.mode(SaveMode.Overwrite).format("de.unikl.cs.dbis.waves").save("out/")
  // println("=============")
  // val df2 = spark.read.format("de.unikl.cs.dbis.waves").load("out/")
  // println(df.count())
  // println(df2.count())
  // Exp 1
  // val partitionColumn = "retweeted_status"
  // val pred = (row : org.apache.spark.sql.Row) => {
  //   val index = row.fieldIndex("retweeted_status")
  //   row.isNullAt(index)
  // }
  // Exp 2
  // val partitionColumn = "user.id"
  // val pred : (org.apache.spark.sql.Row) => Boolean = (row : org.apache.spark.sql.Row) => {
  //   val index = row.fieldIndex("user")
  //   if (row.isNullAt(index)) false
  //   else {
  //     val innerRow = row.getStruct(index)
  //     val innerIndex = innerRow.fieldIndex("id")
  //     if (innerRow.isNullAt(innerIndex)) false
  //     else innerRow.getLong(innerIndex) < 269260424025919552l
  //   }
  // }
  // val schema = df2.schema // here
  //println(spark.read.format("parquet").load("out/exp3/").count())
  val relation = new waves.DefaultSource().createRelation(spark.sqlContext, Map("path" -> "out")).asInstanceOf[waves.WavesRelation]
  relation.repartition("spill", "retweeted_status")

  spark.close()
}