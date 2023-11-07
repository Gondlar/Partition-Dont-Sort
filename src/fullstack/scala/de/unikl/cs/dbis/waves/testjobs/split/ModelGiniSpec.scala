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
      }, specificTests(false))
    }
    "using schema modifications" should {
      behave like split({
        ModelGini.main(args :+ "modifySchema=true")
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
    // The order is non-deterministic if multiple splits have the same gini score
    partitionSchema.buckets should have length (11)

    And("the correct sorter is used")
    partitionSchema.sorter should equal (NoSorter)

    And("the log contains what happened")
    events should contain theSameElementsInOrderAs (Seq(
      "'read-dataframe'",
      "'split-start'",
      "'start-CalculateTotalFingerprint'",
        "'parameter-sampler'",
        "'parameter-fingerprintPruning'",
        "'metadata-fingerprintCount'",
      "'end-CalculateTotalFingerprint'",
      "'start-ModelGini'",
        "'parameter-maxSize'",
        "'parameter-minSize'",
        "'parameter-useColumnSplits'",
        "'parameter-useSearchSpacePruning'",
      "'end-ModelGini'",
      "'start-ShuffleByShape'", "'end-ShuffleByShape'"
    ) ++ (if (!modifySchema) Seq.empty else Seq(
      "'start-SchemaModifier'", "'end-SchemaModifier'",
      "'start-Finalizer'", "'end-Finalizer'",
    )) ++ Seq(
      "'start-PrioritySink'", "'writer-chosen'", "'end-PrioritySink'",
      "'metadata-bucketCount'",
      "'split-done'",
      "'metadata-bytesize'"
    ))
  }
}