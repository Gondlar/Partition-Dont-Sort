package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture

class MetadataForPathVisitorSpec extends WavesSpec
  with PartitionTreeFixture {

  "A MetadataForPathVisitor" should {
    "find present keys" in {
      val visitor = new MetadataForPathVisitor[String](Seq(Present))
      split.accept(visitor)
      visitor.result should equal (PartitionMetadata(Seq(split.key), Seq.empty, Seq(Present)))
    }
    "find absent keys" in {
      val visitor = new MetadataForPathVisitor[String](Seq(Absent))
      split.accept(visitor)
      visitor.result should equal (PartitionMetadata(Seq.empty, Seq(split.key), Seq(Absent)))
    }
    "find nothing in empty paths" in {
      val visitor = new MetadataForPathVisitor[String](Seq.empty)
      split.accept(visitor)
      visitor.result should equal (PartitionMetadata())
    }
    "throw an error for invalid paths" in {
      val visitor = new MetadataForPathVisitor[String](Seq(Partitioned))
      an [InvalidPathException] shouldBe thrownBy (split.accept(visitor))
    }
    "handle non-split nodes" in {
      val visitor = new MetadataForPathVisitor[String](Seq(Partitioned, Present))
      spill.accept(visitor)
      visitor.result should equal (PartitionMetadata(Seq(split.key), Seq.empty, Seq(Partitioned, Present)))
    }
  }
}