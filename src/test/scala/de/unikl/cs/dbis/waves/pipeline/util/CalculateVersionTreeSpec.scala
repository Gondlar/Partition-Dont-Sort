package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.split.recursive.ColumnMetadata
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._

import de.unikl.cs.dbis.waves.util.Versions
import de.unikl.cs.dbis.waves.util.Leaf

class CalculateVersionTreeSpec extends WavesSpec
  with DataFrameFixture with PipelineStateFixture {
    import CalculateVersionTreeSpec._

  "The CalculateVersionedStructure Step" can {
    "calculate the VersionedStructure for a DataFrame" when {
      "the DataFrame is non-empty" in {
        CalculateVersionTree.fromDataFrame(df, schema) should equal (graphForDf)
      }
      "the DataFrame is empty" in {
        CalculateVersionTree.fromDataFrame(emptyDf, schema) should equal (
          Versions(
            IndexedSeq("a", "b", "e"),
            IndexedSeq(
              Leaf.empty,
              Versions(
                IndexedSeq("c", "d"),
                IndexedSeq(
                  Leaf.empty,
                  Leaf.empty
                ),
                Seq((IndexedSeq(false, false), 1d))
              ),
              Leaf.empty
            ),
            Seq((IndexedSeq(false, false, false), 1d))
          )
        )
      }
    }
  }
  it should {
    "always be supported" in {
      (CalculateVersionTree supports dummyState) shouldBe (true)
    }
    "calculate the correct VersionedStructure for the state" in {
      When("we apply the CalculateVersionedStructure step")
      val result = CalculateVersionTree(dummyDfState)

      Then("the correct VersionedStructure is stored")
      (StructureMetadata isDefinedIn result) shouldBe (true)
      StructureMetadata(result) should equal (graphForDf)
    }
  }
}

object CalculateVersionTreeSpec {
  val graphForDf = Versions(
    IndexedSeq("a", "b", "e"),
    IndexedSeq(
      Leaf(Some(ColumnMetadata(5,5,1))),
      Versions(
        IndexedSeq("c", "d"),
        IndexedSeq(
          Leaf(Some(ColumnMetadata(1,1,1))),
          Leaf(Some(ColumnMetadata(5,5,1)))
        ),
        Seq( (IndexedSeq(true, true), .5)
           , (IndexedSeq(true, false), .5)
           )
      ),
      Leaf(Some(ColumnMetadata(42,42,1)))
    ),
    Seq( (IndexedSeq(true, true, true), .25)
       , (IndexedSeq(true, false, true), .25)
       , (IndexedSeq(false, true, true), .25)
       , (IndexedSeq(false, false, true), .25)
       )
  )
}