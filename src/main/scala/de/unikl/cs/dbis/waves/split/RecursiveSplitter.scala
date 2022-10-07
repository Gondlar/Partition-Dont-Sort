package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.DataFrame

import de.unikl.cs.dbis.waves.WavesTable
import de.unikl.cs.dbis.waves.WavesTable.implicits
import de.unikl.cs.dbis.waves.partitions.{Bucket, PartitionTreePath, SplitByPresencePath, Present, Absent}
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import de.unikl.cs.dbis.waves.util.{PathKey, Logger, PartitionFolder}
import de.unikl.cs.dbis.waves.split.recursive.{Heuristic, RowwiseCalculator}
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.sort.{Sorter,NoSorter}
import de.unikl.cs.dbis.waves.DefaultSource
import org.apache.spark.sql.SaveMode

import WavesTable._

/**
  * Implements recursive splitting as described in https://doi.org/10.1145/3530050.3532923
  *
  * @param threshold the threshold min in byte
  * @param sampleSize the sample size in byte
  * @param metric the heuristic to use
  */
final case class RecursiveSplitter(
    threshold : Long,
    sampleSize : Long,
    heursitic: Heuristic
) extends Splitter[PartitionFolder] with Sampler[PartitionFolder] {

    val calc = RowwiseCalculator()

    private var table: WavesTable = null

    def prepare(table: WavesTable) = { this.table = table; this }

    override def prepare(df: DataFrame, path: String) = {
      val newTable = df.getWavesTable.filter(table => table.basePath == path).getOrElse({
          df.write.mode(SaveMode.Overwrite).waves(path, df.schema)
          val options = new java.util.HashMap[String, String](1)
          options.put("path", path)
          DefaultSource().getTable(df.schema, Array.empty, options).asInstanceOf[WavesTable]
      })
      prepare(newTable)
    }

    override def isPrepared = table != null

    override def getPath = { assertPrepared; table.basePath }

    override def sortWith(sorter: Sorter) = {
      if (sorter != NoSorter)
        throw new IllegalArgumentException("sorter not supported")
      this
    }

    /**
      * @return the table this splitter writes to
      */
    def getTable = { assertPrepared; table }

    override protected def load(context: PartitionFolder)
        = table.spark.read.format("parquet")
                          .schema(table.schema)
                          .load(context.filename)

    override protected def sampleRate(context: PartitionFolder)
        = sampleSize.toDouble/context.diskSize(table.fs)

    override def partition(): Unit = {
        assertPrepared
        assert(table.partitionTree.root.isInstanceOf[Bucket[String]])

        partition(PartitionMetadata(), Seq.empty)
    }

    private def partition(
        metadata : PartitionMetadata,
        path: Seq[PartitionTreePath]
    ) : Unit = {
        // Get Current Partition data
        val currentFolder = getFolder(table, path)
        val cutoff = (threshold*0.9)/currentFolder.diskSize(table.fs)

        val best = heursitic.choose(calc, data(currentFolder), metadata, cutoff)
        best match {
            case None => Logger.log("partition-abort", "metric shows no improvement")
            case Some(best) => {
                Logger.log("partition-by", best.toString)
                table.split(best.toString, path:_*)
                recurse(path, Present, metadata, best)
                recurse(path, Absent, metadata, best)
            }
        }
    }

    /**
      * Recursively call [[partition]] if the partition is still large enough
      *
      * @param oldPath the path to the partition we just split
      * @param newStep the step we are taking towards one of its new children
      * @param oldMetadata the metadata from partition we just split
      * @param newKey the key we used to split the partition
      */
    private def recurse(
        oldPath: Seq[PartitionTreePath],
        newStep: SplitByPresencePath,
        oldMetadata: PartitionMetadata,
        newKey: PathKey
    ) : Unit = {
        val path = oldPath :+ newStep
        val size = getFolder(table, path).diskSize(table.fs)
        Logger.log(s"partiton-${newKey.toString.toLowerCase}", size/threshold.toDouble)
        if (size > threshold) {
            val metadata = oldMetadata.clone
            metadata.add(newStep, newKey)
            partition(metadata, path)
        }
    }

    private def getFolder(table: WavesTable, path: Seq[PartitionTreePath]) : PartitionFolder
        = table.partitionTree.find(path).get.asInstanceOf[Bucket[String]].folder(table.basePath)

}
