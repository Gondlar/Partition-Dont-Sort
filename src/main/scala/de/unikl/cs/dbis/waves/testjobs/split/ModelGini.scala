package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.pipeline.sink.ParallelSink
import de.unikl.cs.dbis.waves.pipeline.sink.DataframeSink

/**
 * This is extremely slow, do not run this on large or wide datasets
 */
object ModelGini extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val numPartitions = jobConfig.getInt("numPartitions").getOrElse(8)
    val spark = jobConfig.makeSparkSession(s"Autopartition Model Gini $numPartitions")

    val splitter = new Pipeline(Seq(
      split.ModelGini(numPartitions),
      util.ShuffleByShape),
      sink.PrioritySink(ParallelSink, DataframeSink)
    )

    runSplitter(spark, jobConfig, splitter)
  }
}
