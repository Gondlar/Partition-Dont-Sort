package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.JobConfig
import de.unikl.cs.dbis.waves.partitions.{Bucket, SplitByPresence}
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.sort.NoSorter
import de.unikl.cs.dbis.waves.pipeline._

object Manual extends SplitRunner {
  def main(args: Array[String]) : Unit = {
    val jobConfig = JobConfig.fromArgs(args)
    val spark = jobConfig.makeSparkSession("Manual Partition")

    val defaultShape = SplitByPresence( "quoted_status"
                                       , Bucket("quotes")
                                       , SplitByPresence( "retweeted_status"
                                                        , Bucket("retweets")
                                                        , SplitByPresence( "delete"
                                                                         , "deletes"
                                                                         , "normal"
                                                                         )
                                                        )
                                       )
    val (manualShape, manualSorter) = jobConfig.getString("knownSchemaPath").map { path =>
      val loadedTree = PartitionTreeHDFSInterface.withExactLocation(spark, path).read().get
      (loadedTree.root, loadedTree.sorter)
    }.getOrElse((defaultShape, NoSorter))
    val splitter = new Pipeline(Seq(
      split.Predefined(manualShape.shape),
      if (jobConfig.modifySchema) util.BucketsFromShape else util.ShuffleByShape),
      sink.PrioritySink(sink.ParallelSink.byShape, sink.DataframeSink)
    )

    runSplitter(spark, jobConfig, splitter)
  }
}
