package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.split.NonSplitter
import de.unikl.cs.dbis.waves.sort.LexicographicSorter
import de.unikl.cs.dbis.waves.split.EvenSplitter

object LexicographicMulti extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val numPartitions = jobConfig.getInt("numPartitions").getOrElse(8)
    val spark = jobConfig.makeSparkSession(s"Autopartition Lexicographic Multi $numPartitions")

    val splitter = new EvenSplitter(numPartitions).sortWith(LexicographicSorter)

    runSplitter(spark, jobConfig, splitter)
  }
}
