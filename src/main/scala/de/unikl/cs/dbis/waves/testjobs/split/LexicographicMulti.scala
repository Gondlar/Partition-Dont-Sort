package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.pipeline._

object LexicographicMulti extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val numPartitions = jobConfig.getInt("numPartitions").getOrElse(8)
    val exact = jobConfig.getBool("exact").getOrElse(false)
    val spark = jobConfig.makeSparkSession(s"Autopartition Lexicographic Multi $numPartitions ${if (exact) "(exact)" else ""}")

    val splitter = new Pipeline(Seq(
      split.EvenBuckets(numPartitions),
      util.FlatShapeBuilder,
      sort.LocalOrder(if (exact) sort.ExactCardinalities else sort.EstimatedCardinalities),
      sort.DataframeSorter),
      sink.DataframeSink
    )

    runSplitter(spark, jobConfig, splitter)
  }
}
