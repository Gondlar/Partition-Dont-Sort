package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.partitions.{PartitionTree,Bucket}
import de.unikl.cs.dbis.waves.pipeline._

import de.unikl.cs.dbis.waves.util.nested.schemas._

object LexicographicPartitionwise extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val numPartitions = jobConfig.numPartitions.getOrElse(8)
    val exact = jobConfig.useExactCardinalities
    val sampler = jobConfig.useSampler
    val spark = jobConfig.makeSparkSession(s"Autopartition Lexicographic Partitionwise $numPartitions${if (exact) " (exact)" else ""}")

    val splitter = new Pipeline(Seq(
      sort.GlobalOrder(if (exact) sort.ExactCardinalities(sampler) else sort.EstimatedCardinalities(sampler)),
      split.ParallelEvenBuckets(numPartitions),
      util.Preshuffled,
      util.FlatShapeBuilder,
      sort.ParallelSorter),
      sink.ParallelSink
    )

    runSplitter(spark, jobConfig, splitter)
  }
}
