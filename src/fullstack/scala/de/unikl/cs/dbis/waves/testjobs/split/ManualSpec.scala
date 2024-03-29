package de.unikl.cs.dbis.waves.testjobs.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeMatchers

import de.unikl.cs.dbis.waves.partitions.{PartitionTree, SplitByPresence, Bucket}
import de.unikl.cs.dbis.waves.sort.NoSorter

import org.apache.spark.sql.types.StructType

class ManualSpec extends WavesSpec
  with SplitFixture
  with PartitionTreeMatchers {

  "The Manual Split job" when {
    "not using schema modifications" should {
      behave like split({
        Manual.main(args)
      }, specificTests(false))
    }
    "using schema modifications" should {
      behave like split({
        Manual.main(args :+ "modifySchema=true")
      }, specificTests(true), usesSchemaModifications = true)
    }
  }

  def specificTests(
    modifySchema: Boolean
  )(
    inputSchema: StructType,
    partitionSchema: PartitionTree[String],
    events: Seq[String],
    data: Seq[String]
  ) = {
    And("the partition schema has the correct shape")
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
    val tree = new PartitionTree(inputSchema, NoSorter, manualShape)
    partitionSchema should haveTheSameStructureAs(tree)

    And("the log contains what happened")
    events should contain theSameElementsInOrderAs (Seq(
      "'read-dataframe'",
      "'split-start'",
      "'start-Predefined'", "'end-Predefined'",
    ) ++ (if (modifySchema) Seq(
      "'start-BucketsFromShape'", "'end-BucketsFromShape'",
      "'start-SchemaModifier'", "'end-SchemaModifier'",
      "'start-Finalizer'", "'end-Finalizer'",
      "'start-DataframeSink'", "'end-DataframeSink'"
    ) else Seq(
      "'start-ShuffleByShape'", "'end-ShuffleByShape'",
      "'start-Shuffle'", "'end-Shuffle'",
      "'start-ParallelSink'", "'end-ParallelSink'"
    )) ++ Seq(
      "'metadata-bucketCount'",
      "'split-done'",
      "'metadata-bytesize'",
      "'metadata-treeLocation'"
    ))
  }
}