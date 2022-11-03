package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.split.NonSplitter
import de.unikl.cs.dbis.waves.sort.LexicographicSorter
import de.unikl.cs.dbis.waves.split.EvenSplitter

object LexicographicMulti extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession("Autopartition WavesData Switch")

    val splitter = new EvenSplitter(8).sortWith(LexicographicSorter)

    runSplitter(spark, jobConfig, splitter)
  }
}
