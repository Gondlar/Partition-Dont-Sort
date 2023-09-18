package de.unikl.cs.dbis.waves.testjobs.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeMatchers

import de.unikl.cs.dbis.waves.partitions.{PartitionTree, Bucket}
import de.unikl.cs.dbis.waves.sort.LexicographicSorter

import org.apache.spark.sql.types.StructType

class LexicographicMonoSpec extends WavesSpec
  with SplitFixture
  with PartitionTreeMatchers {

  "The LexicographicMono Split job" when {
    "not using schema modifications" should {
      behave like split({
        LexicographicMono.main(args)
      }, specificTests)
    }
  }

  def specificTests(
    inputSchema: StructType,
    partitionSchema: PartitionTree[String],
    events: Seq[String],
    data: Seq[String]
  ) = {
    And("the partition tree has the right shape")
    val manualShape = Bucket("spill")
    val tree = new PartitionTree(inputSchema, LexicographicSorter, manualShape)
    partitionSchema should haveTheSameStructureAs(tree)

    And("the log contains what happened")
    events should contain theSameElementsInOrderAs (Seq(
      "'split-start'",
      "'start-SingleBucket'", "'end-SingleBucket'",
      "'start-GlobalOrder'", "'end-GlobalOrder'",
      "'start-DataframeSorter'", "'end-DataframeSorter'",
      "'start-DataframeSink'", "'end-DataframeSink'",
      "'split-done'",
      "'split-cleanup-end'"
    ))
  }
}