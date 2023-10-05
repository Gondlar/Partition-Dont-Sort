package de.unikl.cs.dbis.waves.util.operators

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import org.apache.spark.sql.functions.col
import scala.collection.mutable.Map
import org.apache.spark.sql.Row

class CollectSetWithCountAggregatorSpec extends WavesSpec {

  "The CollectSetWithCountAggregator" can {
    "produce the zero element" in {
      agg.zero should equal (Map.empty)
    }
    "perform a reduction" when {
      "the element is null" in {
        agg.reduce(Map.empty, null) should equal (Map.empty)
      }
      "the element is not null" in {
        agg.reduce(Map.empty, 5) should equal (Map((5, 1)))
      }
      "the element already exists in the map" in {
        agg.reduce(Map((5, 1)), 5) should equal (Map((5, 2)))
      }
    }
    "perform a merge" in {
      agg.merge(Map((3,1), (5,1)), Map((5,2), (7,1))) should equal (Map((3,1), (5,3), (7,1)))
    }
    "finalize the aggregation" in {
      agg.finish(Map((5,2))) should contain theSameElementsAs (Seq((5,2)))
    }
  }

  def agg = new CollectSetWithCountAggregator[Any]
}

class CollectSetWithCountSpec extends WavesSpec with DataFrameFixture {
  "The collect_set_with_count Column Function" should {
    "return all distinct values and their count" when {
      "there are null values" in {
        val rows = df.agg(collect_set_with_count[Integer].apply(col("a"))).collect
        rows should have length (1)
        val result = rows(0)
        result.schema.size should equal (1)
        val data = result.getSeq[Row](0).map(row => (row.getInt(0), row.getLong(1)))
        data should contain theSameElementsAs (Seq((5, 4)))
      }
      "there are no null values" in {
        val rows = df.agg(collect_set_with_count[Int].apply(col("e"))).collect
        rows should have length (1)
        val result = rows(0)
        result.schema.size should equal (1)
        val data = result.getSeq[Row](0).map(row => (row.getInt(0), row.getLong(1)))
        data should contain theSameElementsAs (Seq((42, 8)))
      }
      "the dataframe is empty" in {
        val rows = emptyDf.agg(collect_set_with_count[Int].apply(col("e"))).collect
        rows should have length (1)
        val result = rows(0)
        result.schema.size should equal (1)
        result.getSeq[Row](0) shouldBe empty
      }
    }
  }
}