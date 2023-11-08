package de.unikl.cs.dbis.waves.testjobs

import org.apache.hadoop.fs.Path
import java.nio.charset.StandardCharsets

/**
  * Run Spark's schema detection and store the schema to a file as json
  */
object StoreSchema {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession(s"StoreSchema ${jobConfig.inputPath} to ${jobConfig.inputSchemaPath.get}")

    val path = new Path(jobConfig.inputSchemaPath.get)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val out = fs.create(path)

    val schema = spark.read.json(jobConfig.inputPath).schema

    out.write(schema.json.getBytes(StandardCharsets.UTF_8))
    out.close()
  }
}
