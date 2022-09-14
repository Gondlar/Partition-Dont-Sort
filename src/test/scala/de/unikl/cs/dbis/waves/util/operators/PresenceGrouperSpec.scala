package de.unikl.cs.dbis.waves.util.operators

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.util.nested.schemas._
import org.apache.spark.sql.functions.col

class PresenceGrouperSpec extends WavesSpec
  with DataFrameFixture {

  "The PresenceGrouper " should {
    "call its value column presence" in {
      PresenceGrouper.GROUP_COLUMN.toString should startWith ("presence")
    }
    "calculate the correct presence" in {
      val data = df.select(PresenceGrouper(schema))
                    .collect()
                    .map(row => row.getSeq[Int](row.fieldIndex(PresenceGrouper.GROUP_COLUMN)))
      data should contain theSameElementsAs (Seq( Seq(true, true, true)
                                                , Seq(true, true, false)
                                                , Seq(true, false, false)
                                                , Seq(true, false, false)
                                                , Seq(false, true, true)
                                                , Seq(false, true, false)
                                                , Seq(false, false, false)
                                                , Seq(false, false, false))
      )
      forAll (data) { row =>
        row should have length (schema.optionalNodeCount)
      }
    }
  }
}