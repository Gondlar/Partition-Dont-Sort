package de.unikl.cs.dbis.waves.testjobs.split

import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.types.{DataType,StructType}
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.Path

import de.unikl.cs.dbis.waves.util.Logger
import de.unikl.cs.dbis.waves.split.Splitter
import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.WavesTable._
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.partitions.PartitionTree

import java.nio.charset.StandardCharsets
import java.util.Scanner
import java.util.UUID

trait SplitRunner {
  def runSplitter[T](spark: SparkSession, jobConfig: JobConfig, splitter: Splitter[T])
    = run(spark, jobConfig, (df) => df.saveAsWaves(splitter.modifySchema(jobConfig.modifySchema), jobConfig.wavesPath))

  def runPlain(spark: SparkSession, jobConfig: JobConfig)
    = run(spark, jobConfig, (df) => df.write.mode(SaveMode.Overwrite).waves(jobConfig.wavesPath, df.schema))

  def run(spark: SparkSession, jobConfig: JobConfig, job: DataFrame => Unit) = {
    if (jobConfig.cleanWavesPath)
      PartitionTreeHDFSInterface(spark, jobConfig.wavesPath).fs
        .delete(new Path(jobConfig.wavesPath), true)
    val optionalSchema = jobConfig.inputSchemaPath.map(readSchema(_, spark))
    Logger.log("read-dataframe", jobConfig.inputPath)
    val df = optionalSchema match {
      case None => spark.read.json(jobConfig.inputPath)
      case Some(schema) => spark.read.schema(schema).json(jobConfig.inputPath)
    }
    Logger.log("split-start", this.getClass().getName())
    job(df)
    Logger.log("split-done")
    val relation = spark.read.waves(jobConfig.wavesPath).getWavesTable.get
    Logger.log("metadata-bytesize", relation.diskSize())
    val treeLocation = storeTree(relation.partitionTree, jobConfig.treeStorageDirectory, spark)
    Logger.log("metadata-treeLocation", treeLocation)

    Logger.flush(spark.sparkContext.hadoopConfiguration)
    spark.stop()
  }

  private def readSchema(path: String, spark: SparkSession) = {
    val hdfsPath = new Path(path)
    val in = hdfsPath.getFileSystem(spark.sparkContext.hadoopConfiguration).open(hdfsPath)
    try {
      val json = new Scanner(in, StandardCharsets.UTF_8.displayName()).useDelimiter("\\Z").next()
      DataType.fromJson(json).asInstanceOf[StructType]
    } finally in.close()
  }

  private def storeTree(tree: PartitionTree[String], directory: String, spark: SparkSession) = {
    val name = s"$directory/${UUID.randomUUID()}.json"
    val path = new Path(name)
    val out = path.getFileSystem(spark.sparkContext.hadoopConfiguration).create(path)
    out.write(tree.toJson.getBytes(StandardCharsets.UTF_8))
    out.close()
    name
  }
}
