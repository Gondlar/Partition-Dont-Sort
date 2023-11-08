package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.Logger

object LexicographicMono extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val exact = jobConfig.useExactCardinalities
    val sampler = jobConfig.useSampler
    val spark = jobConfig.makeSparkSession(s"Autopartition Lexicographic Mono${if (exact) " (exact)" else ""}")

    spark.conf.set("spark.sql.shuffle.partitions", jobConfig.numPartitions.getOrElse(8))
    Logger.log("parameter-shufflePartitions", jobConfig.numPartitions.getOrElse(8))

    val splitter = new Pipeline(Seq(
      split.SingleBucket,
      sort.GlobalOrder(if (exact) sort.ExactCardinalities(sampler) else sort.EstimatedCardinalities(sampler)),
      sort.DataframeSorter),
      sink.DataframeSink
    ).doFinalize(false)

    runSplitter(spark, jobConfig, splitter)
  }
}