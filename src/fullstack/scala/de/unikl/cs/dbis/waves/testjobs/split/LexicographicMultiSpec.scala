package de.unikl.cs.dbis.waves.testjobs.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeMatchers

import de.unikl.cs.dbis.waves.partitions.{PartitionTree, TreeNode, Spill, Bucket}
import de.unikl.cs.dbis.waves.sort.LexicographicSorter

import org.apache.spark.sql.types.StructType

class LexicographicMultiSpec extends WavesSpec
  with SplitFixture
  with PartitionTreeMatchers {

  "The LexicographicMulti Split job" when {
    "not using schema modifications" should {
      behave like split({
        LexicographicMulti.main(args)
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
    val buckets = (0 until 8).map(_ => Bucket())
    val manualShape = buckets.tail.foldLeft(buckets.head: TreeNode.AnyNode[String])((partitioned, spill) => Spill(partitioned, spill))
    val tree = new PartitionTree(inputSchema, LexicographicSorter, manualShape)
    partitionSchema should haveTheSameStructureAs(tree)

    And("the log contains what happened")
    val (cardinalityEvents, otherEvents) = events.partition(_ == "'done-cardinalities'")
    cardinalityEvents should have length (8)
    otherEvents should contain theSameElementsInOrderAs (Seq(
      "'read-dataframe'",
      "'split-start'",
      "'start-EvenBuckets'", "'end-EvenBuckets'",
      "'start-FlatShapeBuilder'", "'end-FlatShapeBuilder'",
      "'start-LocalOrder'", "'end-LocalOrder'",
      "'start-DataframeSorter'", "'end-DataframeSorter'",
      "'start-Finalizer'", "'end-Finalizer'",
      "'start-DataframeSink'", "'end-DataframeSink'",
      "'metadata-bucketCount'",
      "'split-done'",
      "'metadata-bytesize'",
      "'metadata-treeLocation'"
    ))
  }
}