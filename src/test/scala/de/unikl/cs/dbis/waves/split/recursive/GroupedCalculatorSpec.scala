package de.unikl.cs.dbis.waves.split.recursive

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.Schema
import de.unikl.cs.dbis.waves.Spark
import de.unikl.cs.dbis.waves.util.PathKey

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.{DataFrame => DataFrameFixture}
import de.unikl.cs.dbis.waves.util.operators.PresenceGrouper

class GroupedCalculatorSpec extends WavesSpec
  with DataFrameFixture {

  private var groups: DataFrame = null

  override def beforeEach() = {
    super.beforeEach()
    groups = PresenceGrouper.group(df)
  }

  "An GroupedCalculator" should {
      "find the correct PartitionMetric" in {
        val calc = GroupedCalculator(schema)
        val (size, present, switch) = calc.calc(groups) 

        size should equal(8)
        present should equal (new ObjectCounter(Array(0, 0, 2)))
        switch should equal (new ObjectCounter(Array(1, 8, 3)))
      }
      "work with empty DataFrames" in {
        val calc = GroupedCalculator(schema)
        val (size, present, switch) = calc.calc(PresenceGrouper.group(emptyDf))

        size should equal (0)
        present should equal (new ObjectCounter(Array(0,0,0)))
        switch should equal (new ObjectCounter(Array(0,0,0)))
      }
      "find the correct paths" in {
        val calc = GroupedCalculator(schema)
        calc.paths(groups).toSeq should equal (Seq(
          PathKey("a"), PathKey("b"), PathKey("b.d")
        ))
      }
  }
}