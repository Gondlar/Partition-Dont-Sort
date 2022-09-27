package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture

import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import de.unikl.cs.dbis.waves.util.PathKey

class CollectBucketMetadataVisitorSpec extends WavesSpec
  with PartitionTreeFixture {

  "A CollectBucketMetadataVisitor" when {
    "visiting a Bucket" should {
      "return the metadata it started with" in {
        Given("A visitor with its initial metadata")
        val metadata = PartitionMetadata(Seq.empty, Seq(PathKey("d")))
        val visitor = new CollectBucketMetadataVisitor[String](metadata)

        When("it visits a bucket")
        bucket.accept(visitor)

        Then("it finds the metadata")
        visitor.result should contain theSameElementsAs (Seq(metadata))
      }
    }
    "visiting a split" should {
      "return the metadata for the split's children" in {
        Given("A visitor with its initial metadata")
        val metadata = PartitionMetadata(Seq.empty, Seq(PathKey("d")))
        val visitor = new CollectBucketMetadataVisitor[String](metadata)

        When("it visits a split")
        split.accept(visitor)

        Then("it finds the childrens metadata")
        val absentMetadata = metadata.clone()
        absentMetadata.addAbsent(split.key)
        val presentMetadata = metadata.clone()
        presentMetadata.addPresent(split.key)
        visitor.result should contain theSameElementsInOrderAs (Seq(absentMetadata, presentMetadata))
      }
    }
    "visiting a spill" should {
      "return the metadata for the spill's children" in {
        Given("A visitor with its initial metadata")
        val metadata = PartitionMetadata(Seq.empty, Seq(PathKey("d")))
        val visitor = new CollectBucketMetadataVisitor[String](metadata)

        When("it visits a split")
        spill.accept(visitor)

        Then("it finds the childrens metadata")
        val absentMetadata = metadata.clone()
        absentMetadata.addAbsent(split.key)
        val presentMetadata = metadata.clone()
        presentMetadata.addPresent(split.key)
        visitor.result should contain theSameElementsInOrderAs (Seq(metadata, absentMetadata, presentMetadata))
      }
    }
  }
}