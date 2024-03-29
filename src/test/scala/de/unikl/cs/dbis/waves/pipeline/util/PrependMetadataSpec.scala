package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.WavesSpec
import org.scalatest.Inspectors._

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.partitions.Present

class PrependMetadataSpec extends WavesSpec with PipelineStateFixture {

  "The PrependMetadata Step" should {
    "always be supported" in {
      (PrependMetadata(null) supports dummyState) shouldBe (true)
    }
    "prepend metadata" in {
      val initialState = dummyState
      
      val metadata = PartitionMetadata(Seq(PathKey("foo")), Seq(), Seq(Present))
      val intermediate = PrependMetadata(metadata).run(initialState)
      KnownMetadata(intermediate) should equal (metadata)

      val result = PrependMetadata(metadata).run(intermediate)
      KnownMetadata(result) should equal (PartitionMetadata(Seq(PathKey("foo")), Seq(), Seq(Present, Present)))
    }
  }
}
