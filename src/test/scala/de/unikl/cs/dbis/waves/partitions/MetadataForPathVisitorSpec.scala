package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture

class MetadataForPathVisitorSpec extends WavesSpec
  with PartitionTreeFixture {

  "A KnownKeysForPathVisitor" should {
    "find present keys" in {
      val visitor = new MetadataForPathVisitor[String](Seq(Present))
      split.accept(visitor)
      val metadata = visitor.getMetadata
      metadata.getPresent should contain theSameElementsAs (Seq(split.key))
      metadata.getAbsent shouldBe empty
    }
    "find absent keys" in {
      val visitor = new MetadataForPathVisitor[String](Seq(Absent))
      split.accept(visitor)
      val metadata = visitor.getMetadata
      metadata.getAbsent should contain theSameElementsAs (Seq(split.key))
      metadata.getPresent shouldBe empty
    }
    "find nothing in empty paths" in {
      val visitor = new MetadataForPathVisitor[String](Seq.empty)
      split.accept(visitor)
      val metadata = visitor.getMetadata
      metadata.getAbsent shouldBe empty
      metadata.getPresent shouldBe empty
    }
    "throw an error for invalid paths" in {
      val visitor = new MetadataForPathVisitor[String](Seq(Partitioned))
      an [InvalidPathException] shouldBe thrownBy (split.accept(visitor))
    }
    "handle non-split nodes" in {
      val visitor = new MetadataForPathVisitor[String](Seq(Partitioned, Present))
      spill.accept(visitor)
      val metadata = visitor.getMetadata
      metadata.getPresent should contain theSameElementsAs (Seq(split.key))
      metadata.getAbsent shouldBe empty
    }
  }
}