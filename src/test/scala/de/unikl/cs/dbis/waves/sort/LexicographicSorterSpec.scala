package de.unikl.cs.dbis.waves.sort

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.util.operators.DefinitionLevelGrouper
import de.unikl.cs.dbis.waves.partitions.PartitionTree
import de.unikl.cs.dbis.waves.split.IntermediateData

class LexicographicSorterSpec extends WavesSpec
  with DataFrameFixture {

  "The LexicographicSorter" should {
    "sort lexicographically" in {
      val grouped = IntermediateData.fromRaw(df).group(LexicographicSorter.grouper)
      val data = LexicographicSorter.sort(grouped).groups.collect().map(row =>
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
    "have a name" in {
      LexicographicSorter.name should equal ("lexicographic")
    }
    "be convertable to JSON and back" in {
      import PartitionTree._
      
      val json = LexicographicSorter.toJson
      json should equal ("\"lexicographic\"")
      PartitionTree.sorterFromJson(json) should equal (LexicographicSorter)
    }
  }
}
