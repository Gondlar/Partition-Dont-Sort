package de.unikl.cs.dbis.waves.split

import org.apache.spark.sql.DataFrame

import de.unikl.cs.dbis.waves.WavesTable
import de.unikl.cs.dbis.waves.partitions.{Bucket, PartitionTreePath, Present, Absent}
import de.unikl.cs.dbis.waves.util.{PathKey, Logger, PartitionFolder}
import de.unikl.cs.dbis.waves.split.recursive.{Heuristic, RowwiseCalculator}

/**
  * Implements recursive splitting as described in https://doi.org/10.1145/3530050.3532923
  *
  * @param table the table to split
  * @param threshold the threshold min in byte
  * @param sampleSize the sample size in byte
  * @param metric the heuristic to use
  */
final case class RecursiveSplitter(
    table: WavesTable,
    threshold : Long,
    sampleSize : Long,
    heursitic: Heuristic
) extends Splitter[PartitionFolder] with Sampler[PartitionFolder] {

    val calc = RowwiseCalculator()

    override protected def load(context: PartitionFolder)
        = table.spark.read.format("parquet")
                          .schema(table.schema)
                          .load(context.filename)

    override protected def sampleRate(context: PartitionFolder)
        = sampleSize.toDouble/context.diskSize(table.fs)

    override def partition(): Unit = {
        assert(table.partitionTree.root.isInstanceOf[Bucket[String]])

        partition(Seq.empty, Seq.empty, Seq.empty)
    }

    private def partition(
        knownAbsent : Seq[PathKey],
        knownPresent: Seq[PathKey],
        path: Seq[PartitionTreePath]
    ) : Unit = {
        // Get Current Partition data
        val currentFolder = getFolder(table, path)
        val cutoff = (threshold*0.9)/currentFolder.diskSize(table.fs)

        val best = heursitic.choose(calc, data(currentFolder), knownAbsent, knownPresent, cutoff)
        best match {
            case None => Logger.log("partition-abort", "metric shows no improvement")
            case Some(best) => {
                Logger.log("partition-by", best.toString)
                table.repartition(best.toString, path:_*)
                recurse(path :+ Present, knownAbsent, knownPresent :+ best)
                recurse(path :+ Absent, knownAbsent :+ best, knownPresent)
            }
        }
    }

    /**
      * Recursively call [[partition]] if the partition is still large enough
      *
      * @param path the path to the partition to recursively split
      * @param knownAbsent the known absent paths in the recursive step
      * @param knownPresent the known present paths in the recursive step
      */
    private def recurse(
        path: Seq[PartitionTreePath],
        knownAbsent : Seq[PathKey],
        knownPresent: Seq[PathKey]
    ) : Unit = {
        val size = getFolder(table, path).diskSize(table.fs)
        Logger.log(s"partiton-${path.last.toString.toLowerCase}", size/threshold.toDouble)
        if (size > threshold)
            partition(knownAbsent, knownPresent, path)
    }

    private def getFolder(table: WavesTable, path: Iterable[PartitionTreePath]) : PartitionFolder
        = table.partitionTree.find(path).get.asInstanceOf[Bucket[String]].folder(table.basePath)

}
