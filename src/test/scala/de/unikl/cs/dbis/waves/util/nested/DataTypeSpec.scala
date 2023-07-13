package de.unikl.cs.dbis.waves.util.nested

import schemas._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SchemaFixture
import de.unikl.cs.dbis.waves.util.PathKey
import org.apache.spark.sql.types.{StructType, DataType}

/**
  * Test implicit behaviour added to [[DataType]]
  */
class DataTypeSpec extends WavesSpec 
    with SchemaFixture {

    "A DataType" when {
        "being a leaf" should {
            "have leaf count 1" in {
                schema.fields(0).dataType.leafCount should equal (1)
            }
            "have node count 1" in {
                schema.fields(0).dataType.nodeCount should equal (1)
            }
            "have no optional nodes" in {
                schema.fields(0).dataType.optionalNodeCount should equal (0)
            }
            "have no optional leafs" in {
                schema.fields(0).dataType.optionalLeafCount should equal (0)
            }
        }
        "being a struct" should {
            "have the correct leaf count" in {
                schema.leafCount should equal (4)
            }
            "have the correct node count" in {
                schema.nodeCount should equal (6)
            }
            "have the correct optional node count" in {
                schema.optionalNodeCount should equal (3)
            }
            "have the correct optional leaf count" in {
                schema.optionalLeafCount should equal (3)
            }
        }
    }
    "A StructType" can {
      "mark a path as present" when {
        "the path goes to a leaf" in {
          val result = schema.withPresent(PathKey("b.d"))
          val b = result.fields(result.fieldIndex("b"))
          b should not be 'nullable
          val bData = b.dataType.asInstanceOf[StructType]
          val d = bData.fields(bData.fieldIndex("d"))
          d should not be 'nullable
        }
        "the path goes to an inner node" in {
          val result = schema.withPresent(PathKey("b"))
          val b = result.fields(result.fieldIndex("b"))
          b should not be 'nullable
          val bData = b.dataType.asInstanceOf[StructType]
          val d = bData.fields(bData.fieldIndex("d"))
          d shouldBe 'nullable
        }
      }
      "lists its root-to-leaf paths in left-to-right order" in {
        schema.leafPaths should contain theSameElementsInOrderAs (Seq(PathKey("a"), PathKey("b.c"), PathKey("b.d"), PathKey("e")))
      }
    }
    it should {
      "fail to mark a path as present" when {
        "the path is too long" in {
          an [IllegalArgumentException] shouldBe thrownBy (schema.withPresent(PathKey("b.d.f")))
        }
        "the path is invalid" in {
          an [IllegalArgumentException] shouldBe thrownBy (schema.withPresent(PathKey("foo")))
        }
      }
    }
}