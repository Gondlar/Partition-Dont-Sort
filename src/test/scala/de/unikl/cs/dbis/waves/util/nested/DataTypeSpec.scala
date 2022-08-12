package de.unikl.cs.dbis.waves.util.nested

import schemas._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.Schema
import org.apache.spark.sql.types.{StructType, DataType}

/**
  * Test implicit behaviour added to [[DataType]]
  */
class DataTypeSpec extends WavesSpec 
    with Schema {

    "A DataType" when {
        "being a leaf" should {
            "have leaf count 1" in {
                schema.fields(0).dataType.leafCount should equal (1)
            }
        }
        "being a struct" should {
            "have the correct leaf count" in {
                schema.leafCount should equal (4)
            }
        }
    }
}