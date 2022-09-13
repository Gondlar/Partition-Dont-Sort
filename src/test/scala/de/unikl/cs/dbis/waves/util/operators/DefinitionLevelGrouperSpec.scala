package de.unikl.cs.dbis.waves.util.operators

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrame

class DefinitionLevelGrouperSpec extends WavesSpec
  with DataFrame {

  "The DefinitionLevelGrouper" should {
    "call its value column definition levels" in {
      DefinitionLevelGrouper.GROUP_COLUMN.toString should startWith ("definition_levels")
    }
    "calculate the correct definition levels" in {
      val data = df.select(DefinitionLevelGrouper(schema))
                    .collect()
                    .map(row => row.getSeq[Int](row.fieldIndex(DefinitionLevelGrouper.GROUP_COLUMN)))
      data should contain theSameElementsAs (Seq( Seq(1, 1, 2)
                                                , Seq(1, 1, 1)
                                                , Seq(1, 0, 0)
                                                , Seq(1, 0, 0)
                                                , Seq(0, 1, 2)
                                                , Seq(0, 1, 1)
                                                , Seq(0, 0, 0)
                                                , Seq(0, 0, 0))
      )
    }
  }
}