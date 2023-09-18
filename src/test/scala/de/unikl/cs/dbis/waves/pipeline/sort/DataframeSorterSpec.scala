package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._

class DataframeSorterSpec extends WavesSpec
  with DataFrameFixture with PipelineStateFixture {

  "The DataframeSorter Step" when {
    "no buckets are defined" should {
      "not be supported no matter which orders are defined" in {
        Given("a no orders")
        (DataframeSorter supports dummyState) shouldBe (false)
        
        Given("a local order")
        val withLocal = BucketSortorders(dummyState) = Seq()
        (DataframeSorter supports withLocal) shouldBe (false)
        
        Given("a global order")
        val withGlobal = GlobalSortorder(dummyState) = Seq()
        (DataframeSorter supports withGlobal) shouldBe (false)
        
        Given("a both orders")
        val withBoth = GlobalSortorder(withLocal) = Seq()
        (DataframeSorter supports withBoth) shouldBe (false)
      }
    }
    "buckets and a global order are defined" should {
      "be supported" in {
        val state = GlobalSortorder(dummyState) = Seq()
        val withBuckets = Buckets(state) = Seq()
        (DataframeSorter supports withBuckets) shouldBe (true)
      }
      "sort using the global order" in {
        val state = GlobalSortorder(dummyDfState) = Seq(col("a").asc)
        val withBuckets = Buckets(state) = Seq(df, df)
        val result = DataframeSorter.run(withBuckets)
        
        (Buckets isDefinedIn result) shouldBe (true)
        val sorted = Buckets(result)
        sorted.length shouldEqual (2)
        sorted(0).collect should contain theSameElementsInOrderAs (df.sort(col("a").asc).collect())
        sorted(1).collect should contain theSameElementsInOrderAs (df.sort(col("a").asc).collect())
      }
    }
    "buckets and a local order are defined" should {
      "be supported" in {
        val state = BucketSortorders(dummyState) = Seq()
        val withBuckets = Buckets(state) = Seq()
        (DataframeSorter supports withBuckets) shouldBe (true)
      }
      "sort using the local order" in {
        val state = BucketSortorders(dummyDfState) = Seq(Seq(col("a").asc), Seq(col("b.d").asc))
        val withBuckets = Buckets(state) = Seq(df, df)
        val result = DataframeSorter.run(withBuckets)
        
        (Buckets isDefinedIn result) shouldBe (true)
        val sorted = Buckets(result)
        sorted.length shouldEqual (2)
        sorted(0).collect should contain theSameElementsInOrderAs (df.sort(col("a").asc).collect())
        sorted(1).collect should contain theSameElementsInOrderAs (df.sort(col("b.d").asc).collect())
      }
    }
    "buckets and both orders are defined" should {
      "be supported" in {
        val state = BucketSortorders(dummyState) = Seq()
        val withGlobal = GlobalSortorder(state) = Seq()
        val withBuckets = Buckets(withGlobal) = Seq()
        (DataframeSorter supports withBuckets) shouldBe (true)
      }
      "sort using the local order" in {
        val state = BucketSortorders(dummyDfState) = Seq(Seq(col("a").asc), Seq(col("b.d").asc))
        val withGlobal = GlobalSortorder(state) = Seq(col("e").desc)
        val withBuckets = Buckets(withGlobal) = Seq(df, df)
        val result = DataframeSorter.run(withBuckets)
        
        (Buckets isDefinedIn result) shouldBe (true)
        val sorted = Buckets(result)
        sorted.length shouldEqual (2)
        sorted(0).collect should contain theSameElementsInOrderAs (df.sort(col("a").asc).collect())
        sorted(1).collect should contain theSameElementsInOrderAs (df.sort(col("b.d").asc).collect())
      }
    }
  }
}
