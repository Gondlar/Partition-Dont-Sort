package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.pipeline._

/**
 * This is extremely slow, do not run this on large or wide datasets
 */
object ExactGini extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val numPartitions = jobConfig.numPartitions.getOrElse(8)
    val spark = jobConfig.makeSparkSession(s"Autopartition Exact Gini $numPartitions")

    val splitter = new Pipeline(Seq(
      split.ExactGini(numPartitions)),
      sink.DataframeSink
    )

    runSplitter(spark, jobConfig, splitter)
  }
}
