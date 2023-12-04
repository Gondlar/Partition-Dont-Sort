package de.unikl.cs.dbis.waves.testjobs

import de.unikl.cs.dbis.waves.partitions._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.util.nested.schemas._

import org.apache.hadoop.fs.Path
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.functions.{count, when}

object EvaluateAllocation {
  import PartitionFolder.RemoteWrapper

  type NamedTreePath = Seq[(PartitionTreePath, PathKey)]

  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession(s"EvaluateAllocation for ${jobConfig.wavesPath}")

    val hdfs = PartitionTreeHDFSInterface(spark, jobConfig.wavesPath)
    implicit val fs = hdfs.fs
    val tree = hdfs.read().get
    val leafContainedQueries = tree.globalSchema.leafPaths.map(key => count(key.toCol) > 0)

    val sb = new StringBuilder()
    sb ++= "bucket,file,disksize,rowcount"
    for (i <- leafContainedQueries.indices.tail) {
      sb ++= ",col"
      sb ++= i.toString()
    }
    sb += '\n'
    val results = for {
      (bucket, bucketIndex) <- tree.buckets.zipWithIndex
      (file, fileIndex) <- bucket.folder(jobConfig.wavesPath).parquetFiles.zipWithIndex
    } {
      val diskSize = fs.getContentSummary(file).getLength()
      val resultRow = spark.read.schema(tree.globalSchema).parquet(file.toString())
        .agg(count("*"), leafContainedQueries:_*).collect()(0)
      val rowcount = resultRow.getLong(0)
      val columnContained = (1 until resultRow.length).map(i => resultRow.getBoolean(i))
      
      sb ++= bucketIndex.toString()
      sb += ','
      sb ++= fileIndex.toString()
      sb += ','
      sb ++= diskSize.toString()
      sb += ','
      sb ++= rowcount.toString()
      sb += ','
      sb ++= columnContained.mkString(",")
      sb += '\n'
    }
    val path = new Path(jobConfig.getString("reportName").getOrElse("partitionAllocation.csv"))
    val out = fs.create(path)
    out.write(sb.toString.getBytes(StandardCharsets.UTF_8))
    out.close()
  }
}
