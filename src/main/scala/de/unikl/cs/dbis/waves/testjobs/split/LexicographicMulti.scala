package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.pipeline._

object LexicographicMulti extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val numPartitions = jobConfig.getInt("numPartitions").getOrElse(8)
    val spark = jobConfig.makeSparkSession(s"Autopartition Lexicographic Multi $numPartitions")

    val splitter = new Pipeline(Seq(
      split.EvenBuckets(numPartitions),
      util.FlatShapeBuilder,
      sort.LocalOrder(sort.ExactCardinalities),
      sort.DataframeSorter),
      sink.DataframeSink
    )

    runSplitter(spark, jobConfig, splitter)
  }
}
