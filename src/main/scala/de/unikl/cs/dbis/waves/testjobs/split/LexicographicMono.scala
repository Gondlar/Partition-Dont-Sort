package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.pipeline._

object LexicographicMono extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession("Autopartition Lexicographic Mono")

    val splitter = new Pipeline(Seq(
      split.SingleBucket,
      sort.GlobalOrder(sort.ExactCardinalities),
      sort.DataframeSorter),
      sink.DataframeSink
    )

    runSplitter(spark, jobConfig, splitter)
  }
}