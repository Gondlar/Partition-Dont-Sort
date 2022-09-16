package de.unikl.cs.dbis.waves.util.operators

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import org.apache.spark.sql.functions.typedLit

class NullGrouperSpec extends WavesSpec
  with DataFrameFixture {

  "A NullGrouper " should {
    "do nothing when grouping" in {
      NullGrouper(df) should equal (df)
    }
    "match the groups to the correct rows" in {
      Given("two buckets")
      val g1 = df.limit(3)
      val g2 = df.except(g1)

      When("we match them")
      val result = NullGrouper.matchAll(Seq(g1, g2), df)

      Then("the rows should be matched correctly")
      val mc = NullGrouper.matchColumn
      val expected = g1.withColumn(mc, typedLit(0)).union(g2.withColumn(mc, typedLit(1)))
      result.collect should contain theSameElementsAs (expected.collect)
    }
    "find all data belonging to a bucket" in {
      val g1 = df.limit(3)
      NullGrouper.find(g1, df).collect should contain theSameElementsAs (g1.collect())
    }
    "order the data by the grouped data" in {
      val g1 = df.limit(3).orderBy("b.c")
      val sortedDf = NullGrouper.sort(g1, df)
      sortedDf.rdd.getNumPartitions should equal (1)
      sortedDf.collect should contain theSameElementsInOrderAs (g1.collect)
    }
    "come up with the correct count" in {
      NullGrouper.count(df) should equal (8)
    }
  }
}