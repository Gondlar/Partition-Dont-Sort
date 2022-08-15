package de.unikl.cs.dbis.waves.util

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Column

package object operators {
    def definitionLevels(schema : StructType) : Column
        = DefinitionLevels.definitionLevels(schema, false, None).as("definition_levels")
    
    def presence(schema : StructType) : Column
        = DefinitionLevels.presence(schema, false, None).as("presence")

    def addX(col: Column, value: Int): Column = new Column(AddX(col.expr, value))
}
