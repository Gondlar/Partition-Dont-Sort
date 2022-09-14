package de.unikl.cs.dbis.waves.util.nested

import schemas._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SchemaFixture
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
}