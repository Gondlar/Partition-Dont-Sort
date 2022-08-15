package de.unikl.cs.dbis.waves.util

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Column

package object operators {

    /**
      * Determine each row's definition levels for all optional leaf nodes in
      * the schema.
      * 
      * This column function works on [[Row]]s that follow the given schema and
      * returns an array of integers containing the definition levels in the
      * same order as they appear in the schema.
      *
      * @param schema the schema
      * @return the column
      */
    def definitionLevels(schema : StructType) : Column
        = DefinitionLevels.definitionLevels(schema, false, None).as("definition_levels")
    
    /**
      * Determine the presence of all recursively optional nodes in the schema.
      *
      * This column functoon works on [[Row]]s that follow the given schema and
      * returns an array of Booleans which are true if the node if present in
      * that row. The array lists nodes in pre-order. 
      *
      * @param schema the schema
      * @return the column
      */
    def presence(schema : StructType) : Column
        = DefinitionLevels.presence(schema, false, None).as("presence")

    /**
      * Add a specified value to every integer in an array
      *
      * @param col the column cotaining the array to modify
      * @param value the value to add
      * @return the column containing the result
      */
    def addX(col: Column, value: Int): Column = new Column(AddX(col.expr, value))
}
