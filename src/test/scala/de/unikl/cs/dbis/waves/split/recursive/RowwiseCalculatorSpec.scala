package de.unikl.cs.dbis.waves.split.recursive

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.util.PathKey

import org.apache.spark.sql.types.StructType
import de.unikl.cs.dbis.waves.DataFrameFixture

class RowwiseCalculatorSpec extends WavesSpec
  with DataFrameFixture {

  "An RowwiseCalculator" should {
      "find the correct PartitionMetric" in {
        val calc = RowwiseCalculator()
        val (size, present, switch) = calc.calc(df) 

        size should equal(8)
        present should equal (new ObjectCounter(Array(0, 0, 2)))
        switch should equal (new ObjectCounter(Array(0, 4, 2)))
      }
      "work with empty DataFrames" in {
        val calc = RowwiseCalculator()
        val (size, present, switch) = calc.calc(emptyDf)

        size should equal (0)
        present should equal (new ObjectCounter(Array(0,0,0)))
        switch should equal (new ObjectCounter(Array(0,0,0)))
      }
      "find the correct paths" in {
        val calc = RowwiseCalculator()
        calc.paths(df).toSeq should equal (Seq(
          PathKey("a"), PathKey("b"), PathKey("b.d")
        ))
      }
  }
}