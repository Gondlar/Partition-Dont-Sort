package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.pipeline._

/**
 * This is extremely slow, do not run this on large or wide datasets
 */
object ModelGiniWithSort extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val numPartitions = jobConfig.getInt("numPartitions").getOrElse(8)
    val spark = jobConfig.makeSparkSession(s"Autopartition Model Gini $numPartitions")

    val splitter = new Pipeline(Seq(
      util.CalculateRSIGraph,
      split.ModelGini(numPartitions),
      util.ShuffleByShape,
      sort.GlobalOrder(sort.RSIGRaphCardinalities),
      util.PriorityStep(sort.ParallelSorter, sort.DataframeSorter)),
      sink.PrioritySink(sink.ParallelSink, sink.DataframeSink)
    )

    runSplitter(spark, jobConfig, splitter)
  }
}
