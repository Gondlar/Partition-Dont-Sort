package de.unikl.cs.dbis.waves.util.operators

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrame

import org.apache.spark.sql.functions.col

class DefinitionLevelsSpec extends WavesSpec
    with DataFrame {

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
        }
    }
    
}