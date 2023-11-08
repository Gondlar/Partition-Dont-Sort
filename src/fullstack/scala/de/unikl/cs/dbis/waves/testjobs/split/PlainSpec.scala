package de.unikl.cs.dbis.waves.testjobs.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeMatchers

import de.unikl.cs.dbis.waves.partitions.{PartitionTree, Bucket}
import de.unikl.cs.dbis.waves.sort.NoSorter

import org.apache.spark.sql.types.StructType

class PlainSpec extends WavesSpec
  with SplitFixture
  with PartitionTreeMatchers {

  "The Plain Split job" when {
    "not using schema modifications" should {
      behave like split({
        Plain.main(args)
      }, specificTests, enforceSingleFilesPerPartition = false)
    }
  }

  def specificTests(
    inputSchema: StructType,
    partitionSchema: PartitionTree[String],
    events: Seq[String],
    data: Seq[String]
  ) = {
    And("the partition tree has the correct shape")
    val manualShape = Bucket("spill")
    val tree = new PartitionTree(inputSchema, NoSorter, manualShape)
    partitionSchema should haveTheSameStructureAs(tree)

    And("the log contains what happened")
    events should contain theSameElementsInOrderAs (Seq(
      "'read-dataframe'",
      "'split-start'",
      "'split-done'",
      "'metadata-bytesize'",
      "'metadata-treeLocation'"
    ))
  }
}