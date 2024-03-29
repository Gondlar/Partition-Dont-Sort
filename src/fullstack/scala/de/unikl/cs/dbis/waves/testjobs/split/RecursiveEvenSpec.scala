package de.unikl.cs.dbis.waves.testjobs.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.partitions.PartitionTree
import de.unikl.cs.dbis.waves.sort.NoSorter

import org.apache.spark.sql.types.StructType

class RecursiveEvenSpec extends WavesSpec
  with SplitFixture {

  "The RecursiveEven Split job" when {
    "not using schema modifications" should {
      behave like split({
        RecursiveEven.main(args)
      }, specificTests)
    }
    "using schema modifications" should {
      behave like split({
        RecursiveEven.main(args :+ "modifySchema=true")
      }, specificTests, usesSchemaModifications = true)
    }
  }

  def specificTests(
    inputSchema: StructType,
    partitionSchema: PartitionTree[String],
    events: Seq[String],
    data: Seq[String]
  ) = {
    // The schema is not deterministic if two columns have the same score so
    // we can't test a specific structure

    And("the correct sorter is used")
    partitionSchema.sorter should equal (NoSorter)

    And("the log contains what happened")
    events should contain allOf (
      "'read-dataframe'",
      "'split-start'",
      "'split-done'",
      "'metadata-bytesize'",
      "'metadata-treeLocation'"
    )
  }
}