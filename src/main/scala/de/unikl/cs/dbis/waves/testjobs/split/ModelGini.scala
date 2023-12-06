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
    val maxSize = 1.0/numPartitions
    val minSize = maxSize * jobConfig.relativeMinSize
    val useColumnSplits = jobConfig.useColumnSplits
    val useSearchSpacePruning = jobConfig.useSearchSpacePruning
    val useFingerprintPruning = jobConfig.useFingerprintPruning
    val sampler = jobConfig.useSampler
    val spark = jobConfig.makeSparkSession(s"Autopartition Model Gini $numPartitions")

    val splitter = new Pipeline(Seq(
      util.CalculateTotalFingerprint(sampler, useFingerprintPruning),
      split.ModelGini(maxSize, minSize, useColumnSplits, useSearchSpacePruning))
      ++ (if (jobConfig.modifySchema) Seq.empty else Seq(
        util.ShuffleByShape,
        util.Shuffle
      )),
      sink.PrioritySink(sink.ParallelSink, sink.DataframeSink)
    )

    runSplitter(spark, jobConfig, splitter)
  }
}
