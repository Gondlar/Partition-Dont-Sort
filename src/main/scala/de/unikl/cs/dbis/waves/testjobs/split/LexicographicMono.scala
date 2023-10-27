package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.pipeline._

object LexicographicMono extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val exact = jobConfig.useExactCardinalities
    val spark = jobConfig.makeSparkSession(s"Autopartition Lexicographic Mono${if (exact) " (exact)" else ""}")

    val splitter = new Pipeline(Seq(
      split.SingleBucket,
      sort.GlobalOrder(if (exact) sort.ExactCardinalities else sort.EstimatedCardinalities),
      sort.DataframeSorter),
      sink.DataframeSink
    )

    runSplitter(spark, jobConfig, splitter)
  }
}