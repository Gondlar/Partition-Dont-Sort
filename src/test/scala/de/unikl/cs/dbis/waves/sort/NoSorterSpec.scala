package de.unikl.cs.dbis.waves.sort

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.util.operators.NullGrouper

class NoSorterSpec extends WavesSpec
  with DataFrameFixture {

  "The NoSorter" should {
    "not sort" in {
      NoSorter.grouper should equal (NullGrouper)
      NoSorter.sort(df).collect() should contain theSameElementsInOrderAs df.collect()
    }
  }
}
