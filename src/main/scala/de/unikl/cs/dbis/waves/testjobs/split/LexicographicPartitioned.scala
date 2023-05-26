package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.partitions.{PartitionTree,Bucket}
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.sort.NoSorter
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.util.operators.DefinitionLevelGrouper
import org.apache.spark.sql.functions.count_distinct

import de.unikl.cs.dbis.waves.util.nested.schemas._

object LexicographicPartitioned extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val numPartitions = jobConfig.getInt("numPartitions").getOrElse(8)
    val spark = jobConfig.makeSparkSession(s"Autopartition Lexicographic Partitionwise $numPartitions")

    run(spark, jobConfig, df => {
      // Find sort order
      val dls = df.withColumn(DefinitionLevelGrouper.GROUP_COLUMN, DefinitionLevelGrouper(df.schema))
      val leafCount = df.schema.optionalLeafCount()
      val columns = for (column <- 0 until leafCount) yield {
        count_distinct(DefinitionLevelGrouper.GROUP_COLUMN(column))
      }
      val cardinalities = dls.agg(columns.head, columns.tail:_*).head()
      val order = (0 until leafCount)
        .map(cardinalities.getLong(_))
        .zipWithIndex
        .sortBy(_._1)
        .map(c => DefinitionLevelGrouper.GROUP_COLUMN(c._2).asc)

      // Partition and sort
      val folder = PartitionFolder.makeFolder(jobConfig.wavesPath, false)
      dls.sort(order:_*)
        .drop(DefinitionLevelGrouper.GROUP_COLUMN.col())
        .repartition(numPartitions)
        .write.parquet(folder.filename)
      
      // write schema
      val tree = new PartitionTree(df.schema, NoSorter, Bucket(folder.name))
      PartitionTreeHDFSInterface.apply(df.sparkSession, jobConfig.wavesPath).write(tree)
    })
  }
}
