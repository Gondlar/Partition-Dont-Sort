package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.util.UniformColumnMetadata
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._

import de.unikl.cs.dbis.waves.util.Versions
import de.unikl.cs.dbis.waves.util.Leaf
import de.unikl.cs.dbis.waves.util.TotalFingerprint

class CalculateTotalFngerprintSpec extends WavesSpec
  with DataFrameFixture with PipelineStateFixture {
    import CalculateTotalFngerprintSpec._

  "The CalculateTotalFingerprint Step" can {
    "calculate the VersionedStructure for a DataFrame" when {
      "the DataFrame is non-empty" in {
        CalculateTotalFingerprint.fromDataFrame(df, schema) should equal (graphForDf)
      }
      "the DataFrame is empty" in {
        CalculateTotalFingerprint.fromDataFrame(emptyDf, schema) should equal (
          TotalFingerprint(
            IndexedSeq("a", "b", "b.c", "b.d", "e"),
            Set(
              (IndexedSeq(false, false, false, false, false), 0L),
            ),
            IndexedSeq(None, None, None, None),
            IndexedSeq("a", "b.c", "b.d", "e")
          )
        )
      }
    }
  }
  it should {
    "always be supported" in {
      (CalculateTotalFingerprint() supports dummyState) shouldBe (true)
    }
    "calculate the correct VersionedStructure for the state" in {
      When("we apply the CalculateVersionedStructure step")
      val result = CalculateTotalFingerprint()(dummyDfState)

      Then("the correct VersionedStructure is stored")
      (StructureMetadata isDefinedIn result) shouldBe (true)
      StructureMetadata(result) should equal (graphForDf)
    }
  }
}

object CalculateTotalFngerprintSpec {
  val graphForDf = TotalFingerprint(
    IndexedSeq("a", "b", "b.c", "b.d", "e"),
    Set(
      (IndexedSeq(true, true, true, true, true), 1L),
      (IndexedSeq(true, true, true, false, true), 1L),
      (IndexedSeq(true, false, false, false, true), 2L),
      (IndexedSeq(false, true, true, true, true), 1L),
      (IndexedSeq(false, true, true, false, true), 1L),
      (IndexedSeq(false, false, false, false, true), 2L),
    ),
    IndexedSeq(Some(UniformColumnMetadata(5,5,1)), Some(UniformColumnMetadata(1,1,1)), Some(UniformColumnMetadata(5,5,1)), Some(UniformColumnMetadata(42,42,1))),
    IndexedSeq("a", "b.c", "b.d", "e")
  )
}