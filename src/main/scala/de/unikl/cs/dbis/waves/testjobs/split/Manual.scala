package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.partitions.{Bucket, SplitByPresence}
import de.unikl.cs.dbis.waves.split.PredefinedSplitter

object Manual extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession("Autopartition WavesData Even")

    val manualShape = SplitByPresence( "quoted_status"
                                     , Bucket("quotes")
                                     , SplitByPresence( "retweeted_status"
                                                      , Bucket("retweets")
                                                      , SplitByPresence( "delete"
                                                                       , "deletes"
                                                                       , "normal"
                                                                       )
                                                      )
                                     )
    val splitter = new PredefinedSplitter(manualShape)

    runSplitter(spark, jobConfig, splitter)
  }
}
