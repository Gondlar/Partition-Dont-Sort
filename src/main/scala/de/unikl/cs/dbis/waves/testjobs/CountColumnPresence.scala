package de.unikl.cs.dbis.waves.testjobs

import de.unikl.cs.dbis.waves.partitions._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.util.nested.schemas._
import de.unikl.cs.dbis.waves.testjobs.split.SplitRunner

import org.apache.hadoop.fs.Path
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.functions.{count, when}

object CountColumnPresence {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession(s"CountColumnPresence for ${jobConfig.wavesPath}")

    val optionalSchema = jobConfig.inputSchemaPath.map(SplitRunner.readSchema(_, spark))
    val df = optionalSchema match {
      case None => spark.read.json(jobConfig.inputPath)
      case Some(schema) => spark.read.schema(schema).json(jobConfig.inputPath)
    }

    val leafQueries = df.schema.leafPaths.map(key => count(key.toCol))
    val resultRow = df.agg(count("*"), leafQueries:_*).collect()(0)
    val rowcount = resultRow.getLong(0)
    val columnCount = (1 until resultRow.length).map(i => resultRow.getLong(i))

    val sb = new StringBuilder()
    sb ++= "rowcount"
    for (i <- columnCount.indices.tail) {
      sb ++= ",col"
      sb ++= i.toString()
    }
    sb += '\n'
    sb ++= rowcount.toString()
    sb += ','
    sb ++= columnCount.mkString(",")
    sb += '\n'

    val path = new Path(jobConfig.getString("reportName").getOrElse("columnCounts.csv"))
    val hdfs = PartitionTreeHDFSInterface(spark, path.toString)
    implicit val fs = hdfs.fs
    val out = fs.create(path)
    out.write(sb.toString.getBytes(StandardCharsets.UTF_8))
    out.close()
  }
}
