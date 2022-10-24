package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.partitions.{Bucket, SplitByPresence}
import de.unikl.cs.dbis.waves.split.PredefinedSplitter

object Plain {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession("Autopartition WavesData Even")

    runPlain(spark, jobConfig)
  }
}
