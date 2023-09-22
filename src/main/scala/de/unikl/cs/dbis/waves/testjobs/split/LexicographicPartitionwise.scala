package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.partitions.{PartitionTree,Bucket}
import de.unikl.cs.dbis.waves.pipeline._

import de.unikl.cs.dbis.waves.util.nested.schemas._

object LexicographicPartitionwise extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val numPartitions = jobConfig.getInt("numPartitions").getOrElse(8)
    val exact = jobConfig.getBool("exact").getOrElse(false)
    val spark = jobConfig.makeSparkSession(s"Autopartition Lexicographic Partitionwise $numPartitions$numPartitions ${if (exact) " (exact)" else ""}")

    val splitter = new Pipeline(Seq(
      split.ParallelEvenBuckets(numPartitions),
      sort.GlobalOrder(if (exact) sort.ExactCardinalities else sort.EstimatedCardinalities),
      sort.ParallelSorter),
      sink.ParallelSink.byPartition
    )

    runSplitter(spark, jobConfig, splitter)
  }
}
