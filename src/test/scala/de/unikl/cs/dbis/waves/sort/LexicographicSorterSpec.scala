package de.unikl.cs.dbis.waves.sort

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.util.operators.DefinitionLevelGrouper

class LexicographicSorterSpec extends WavesSpec
  with DataFrameFixture {

  "The LexicographicSorter" should {
    "sort lexicographically" in {
      val grouped = LexicographicSorter.grouper(df)
      val data = LexicographicSorter.sort(grouped).collect().map(row =>
        row.getSeq[Int](row.fieldIndex(DefinitionLevelGrouper.GROUP_COLUMN))
      )
      data should contain theSameElementsInOrderAs (Seq( Seq(0, 0, 0)
                                                       , Seq(0, 1, 1)
                                                       , Seq(0, 1, 2)
                                                       , Seq(1, 0, 0)
                                                       , Seq(1, 1, 1)
                                                       , Seq(1, 1, 2)
      ))
    }
  }
}
