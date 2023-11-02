package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.pipeline._

object LexicographicMulti extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val numPartitions = jobConfig.numPartitions.getOrElse(8)
    val exact = jobConfig.useExactCardinalities
    val sampler = jobConfig.useSampler
    val spark = jobConfig.makeSparkSession(s"Autopartition Lexicographic Multi $numPartitions ${if (exact) "(exact)" else ""}")

    val splitter = new Pipeline(Seq(
      split.EvenBuckets(numPartitions),
      util.FlatShapeBuilder,
      sort.LocalOrder(if (exact) sort.ExactCardinalities(sampler) else sort.EstimatedCardinalities(sampler)),
      sort.DataframeSorter),
      sink.DataframeSink
    )

    runSplitter(spark, jobConfig, splitter)
  }
}
