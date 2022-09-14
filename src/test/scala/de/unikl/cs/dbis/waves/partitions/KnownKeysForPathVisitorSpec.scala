package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture

class KnownKeysForPathVisitorSpec extends WavesSpec
  with PartitionTreeFixture {

  "A KnownKeysForPathVisitor" should {
    "find present keys" in {
      val visitor = new KnownKeysForPathVisitor[String](Seq(Present))
      split.accept(visitor)
      visitor.present should contain theSameElementsAs (Seq(split.key))
      visitor.absent shouldBe empty
    }
    "find absent keys" in {
      val visitor = new KnownKeysForPathVisitor[String](Seq(Absent))
      split.accept(visitor)
      visitor.absent should contain theSameElementsAs (Seq(split.key))
      visitor.present shouldBe empty
    }
    "find nothing in empty paths" in {
      val visitor = new KnownKeysForPathVisitor[String](Seq.empty)
      split.accept(visitor)
      visitor.absent shouldBe empty
      visitor.present shouldBe empty
    }
    "throw an error for invalid paths" in {
      val visitor = new KnownKeysForPathVisitor[String](Seq(Partitioned))
      an [InvalidPathException] shouldBe thrownBy (split.accept(visitor))
    }
    "handle non-split nodes" in {
      val visitor = new KnownKeysForPathVisitor[String](Seq(Partitioned, Present))
      spill.accept(visitor)
      visitor.present should contain theSameElementsAs (Seq(split.key))
      visitor.absent shouldBe empty
    }
  }
}