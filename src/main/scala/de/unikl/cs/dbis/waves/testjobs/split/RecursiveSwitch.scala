package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.split.RecursiveSplitter
import de.unikl.cs.dbis.waves.split.recursive.SwitchHeuristic

object RecursiveSwitch {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession("Autopartition WavesData Switch")

    val blocksize = spark.sparkContext.hadoopConfiguration.getLong("dfs.blocksize", jobConfig.fallbackBlocksize)
    val splitter = RecursiveSplitter( blocksize, jobConfig.sampleSize, SwitchHeuristic)

    runSplitter(spark, jobConfig, splitter)
  }
}