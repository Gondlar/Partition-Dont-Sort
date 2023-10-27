package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.pipeline._

object JustSplit extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val numPartitions = jobConfig.numPartitions.getOrElse(8)
    val spark = jobConfig.makeSparkSession("Autopartition Split")

    val splitter = new Pipeline(Seq(
      split.ParallelEvenBuckets(numPartitions),
      util.FlatShapeBuilder
      ),
      sink.ParallelSink.byPartition
    )

    runSplitter(spark, jobConfig, splitter)
  }
}
