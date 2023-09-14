package de.unikl.cs.dbis.waves.util.operators

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture
import de.unikl.cs.dbis.waves.PartitionTreeFixture

import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

class FindBucketSpec extends WavesSpec
    with DataFrameFixture with PartitionTreeFixture {

    "The FindBucket Column Function" can {
      "find the correct bucket value" in {
        val resDf = df.select(findBucket(split, schema))
        resDf.schema should equal (StructType(Seq(StructField("bucket", StringType, false))))
        resDf.collect().map(row => row.getString(0)) should contain theSameElementsInOrderAs (Seq(
          "bar2", "baz2", "baz2", "baz2", "bar2", "baz2", "baz2", "baz2"
        ))
      }
    }
    
}