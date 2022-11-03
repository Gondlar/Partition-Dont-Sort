package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.util.operators.NullGrouper
import de.unikl.cs.dbis.waves.util.operators.PresenceGrouper
import de.unikl.cs.dbis.waves.util.operators.DefinitionLevelGrouper

import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col,monotonically_increasing_id}

class IntermediateDataSpec extends WavesSpec
  with DataFrameFixture {

  "An IntermediateData object" can {
    "be constructed from a DataFrame" in {
      val data = IntermediateData.fromRaw(df)
      data.grouper should equal (NullGrouper)
      data.source should equal (df)
      data.groups should equal (df)
      data should not be 'grouped
    }
    "be converted to DataFrame" in {
      val data = IntermediateData(df.filter(col("b.d").isNotNull), NullGrouper, df)
      data.toDF.collect() should contain theSameElementsAs (df.filter(col("b.d").isNotNull).collect)
    }
    "regroup its data" in {
      val data = IntermediateData(df.filter(col("b.d").isNotNull), NullGrouper, df)
      val grouped = data.group(PresenceGrouper)
      grouped shouldBe 'grouped
      grouped.source.collect should contain theSameElementsAs (data.groups.collect())
      grouped.grouper should equal (PresenceGrouper)
      grouped.groups.collect() should contain theSameElementsAs (Seq(
                        Row(WrappedArray.make(Array(true, true, true)), 1),
                        Row(WrappedArray.make(Array(false, true, true)), 1)
                    ))
    }
    "transform its data" in {
      val data = IntermediateData.fromRaw(df)
      val trans = IntermediateData.fromRaw(emptyDf)
      data.transform(_ => trans) should equal (trans)
    }
    "return the source schema" in {
      val data = IntermediateData.fromRaw(df).group(PresenceGrouper)
      data.sourceSchema should equal (df.schema)
    }
    "return the grouped schema" in {
      val data = IntermediateData.fromRaw(df).group(PresenceGrouper)
      data.groupSchema should equal (PresenceGrouper(df).schema)
    }
  }
  it should {
    "contain the same data after regrouping back and forth" in {
      val dfs = (0 to 1).map{v => df.filter(monotonically_increasing_id.mod(2) === v)}
      val result = dfs.map(IntermediateData.fromRaw(_).group(DefinitionLevelGrouper).toDF)
      result.map(_.collect()).reduce(_ ++ _) should contain theSameElementsAs (df.collect())
    }
  }
}
