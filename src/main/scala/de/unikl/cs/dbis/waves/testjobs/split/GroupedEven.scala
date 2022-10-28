package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.split.HeuristicSplitter
import de.unikl.cs.dbis.waves.split.recursive.EvenHeuristic

object GroupedEven extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession("GroupedEven")

    // 1 Tweet is about 1KB
    val blocksize = spark.sparkContext.hadoopConfiguration.getLong("dfs.blocksize", jobConfig.fallbackBlocksize)
    val splitter = new HeuristicSplitter(blocksize/1024, EvenHeuristic)

    runSplitter(spark, jobConfig, splitter)
  }
}