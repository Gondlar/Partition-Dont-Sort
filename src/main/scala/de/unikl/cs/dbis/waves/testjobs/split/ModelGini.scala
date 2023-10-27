package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.pipeline._

/**
 * This is extremely slow, do not run this on large or wide datasets
 */
object ModelGini extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val numPartitions = jobConfig.numPartitions.getOrElse(8)
    val useColumnSplits = jobConfig.useColumnSplits
    val spark = jobConfig.makeSparkSession(s"Autopartition Model Gini $numPartitions")

    val splitter = new Pipeline(Seq(
      util.CalculateTotalFingerprint,
      split.ModelGini(1.0/numPartitions, useColumnSplits),
      util.ShuffleByShape),
      sink.PrioritySink(sink.ParallelSink.byShape, sink.DataframeSink)
    )

    runSplitter(spark, jobConfig, splitter)
  }
}
