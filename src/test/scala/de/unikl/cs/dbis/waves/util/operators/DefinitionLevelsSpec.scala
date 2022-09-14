package de.unikl.cs.dbis.waves.util.operators

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.util.nested.schemas._
import org.apache.spark.sql.functions.col

class DefinitionLevelsSpec extends WavesSpec
    with DataFrameFixture {

    "The DefinitionLevels function" when {
        "applied via col" should {
            "calculate the correct definition levels" in {
                val data = df.select(definitionLevels(schema))
                             .collect()
                             .map(row => row.getSeq[Int](row.fieldIndex("definition_levels")))
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
            "calculate the correct presence" in {
                val data = df.select(presence(schema))
                             .collect()
                             .map(row => row.getSeq[Int](row.fieldIndex("presence")))
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
    
}