package de.unikl.cs.dbis.waves.testjobs.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.partitions.PartitionTree
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.sort.NoSorter

import org.apache.spark.sql.types.StructType

class ModelGiniSpec extends WavesSpec
  with SplitFixture {

  "The ModelGini Split job" when {
    "not using schema modifications" should {
      behave like split({
        ModelGini.main(args)
      }, specificTests)
    }
    "using schema modifications" should {
      behave like split({
        ModelGini.main(args :+ "modifySchema=true")
      }, specificTests, usesSchemaModifications = true)
    }
  }

  def specificTests(
    inputSchema: StructType,
    partitionSchema: PartitionTree[String],
    events: Seq[String],
    data: Seq[String]
  ) = {
    And("the partition schema has the correct shape")
    // The order is non-deterministic if multiple splits have the same gini score
    partitionSchema.buckets should have length (8)

    And("the correct sorter is used")
    partitionSchema.sorter should equal (NoSorter)

    And("the log contains what happened")
    events should contain allOf (
      "'split-start'",
      "'start-CalculateRSIGraph'", "'end-CalculateRSIGraph'",
      "'start-ModelGini'", "'end-ModelGini'",
      "'start-ShuffleByShape'", "'end-ShuffleByShape'",
      "'start-PrioritySink'", "'writer-chosen'", "'end-PrioritySink'",
      "'split-done'",
      "'split-cleanup-end'"
    )
  }
}