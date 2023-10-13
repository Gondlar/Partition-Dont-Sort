package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.pipeline.sort.StructuralMetadataCardinalities

/**
 * This is extremely slow, do not run this on large or wide datasets
 */
object ModelGiniWithSort extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val numPartitions = jobConfig.getInt("numPartitions").getOrElse(8)
    val spark = jobConfig.makeSparkSession(s"Autopartition Model Gini (Sorted) $numPartitions")

    val splitter = new Pipeline(Seq(
      util.CalculateVersionTree,
      split.ModelGini(numPartitions),
      util.ShuffleByShape,
      sort.GlobalOrder(StructuralMetadataCardinalities),
      util.PriorityStep(sort.ParallelSorter, sort.DataframeSorter)),
      sink.PrioritySink(sink.ParallelSink.byShape, sink.DataframeSink)
    )

    runSplitter(spark, jobConfig, splitter)
  }
}
