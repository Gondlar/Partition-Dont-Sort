package de.unikl.cs.dbis.waves.testjobs

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

import de.unikl.cs.dbis.waves.split.recursive.RowwiseCalculator
import de.unikl.cs.dbis.waves.util.PathKey

import org.apache.hadoop.fs.Path
import java.nio.charset.StandardCharsets

/**
  * Implement schema extraction based on the simplified structure identification
  * graph [1] adapted for the Spark scenario where spark already gives us the
  * schema without presence information. This is inefficient in practice because
  * it required two scans, but serves our needs and is easier to implement.
  * 
  * [1] Klettke et al. 2015. Schema Extraction and Structural Outlier Detection for JSON-based NoSQL Data Stores. BTW'15
  */
object ExtractSchema {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession(s"ExtractSchema ${jobConfig.inputPath}")

    val df = spark.read.json(jobConfig.inputPath)
    // spark infers all-optional schemas, so the following will check all nodes
    val calc = RowwiseCalculator()
    val (total, presentCount, _) = calc.calcRaw(df)
    val paths = calc.paths(df).toSeq
    val pathCounts = presentCount.toMap(paths)

    var schema = df.schema
    for(path <- paths) {
      val myOccurences = pathCounts(path)
      val parentOccurences = if (path.isNested) pathCounts(path.parent) else total
      if (parentOccurences == myOccurences) {
        schema = markRequired(schema, path)
      }
    }

    val path = new Path("extractedSchema.json")
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val out = fs.create(path)
    out.write(schema.json.getBytes(StandardCharsets.UTF_8))
    out.close()
  }

  /**
    * Mark exactly the given path as required in the schema
    *
    * @param schema the schema
    * @param path the path
    * @return the modified schema
    */
  def markRequired(schema: StructType, path: PathKey): StructType = {
    val op: StructField => StructField
      = if (!path.isNested) (field => field.copy(nullable = false))
        else field => (field.copy(dataType = markRequired(field.dataType.asInstanceOf[StructType], path.tail)))
    StructType(
      for(field <- schema.fields) yield
        if (field.name == path.head) op(field) else field
    )
  }
}
