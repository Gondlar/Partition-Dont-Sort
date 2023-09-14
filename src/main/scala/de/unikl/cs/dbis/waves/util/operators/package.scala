package de.unikl.cs.dbis.waves.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{col,struct,udf}
import org.apache.spark.sql.Column

import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode
import de.unikl.cs.dbis.waves.partitions.visitors.operations._

import scala.reflect.runtime.universe.TypeTag

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
    def definitionLevels(schema : StructType) : Column = DefinitionLevelGrouper(schema).alias("definition_levels")
    
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
    def presence(schema : StructType) : Column = PresenceGrouper(schema).alias("presence")

    /**
      * Add a specified value to every integer in an array
      *
      * @param col the column cotaining the array to modify
      * @param value the value to add
      * @return the column containing the result
      */
    def addX(col: Column, value: Int): Column = new Column(AddX(col.expr, value))

    /**
      * For each row, find the correct bucket in a PartitionTree and retrieve
      * their respective payloads
      *
      * @param tree the tree to search. Its payloads must be encodable by spark
      * @param schema the schema of the data. If the schema does not for the
      *               DataFrame, the query will fail
      */
    def findBucket[Payload : TypeTag](tree: AnyNode[Payload], schema: StructType) = {
      val map = udf((row: Row) => tree.bucketFor(row, schema).data)
        .asNonNullable()
      map(struct(schema.fields.map(foo => col(foo.name)):_*))
        .as("bucket")
    }
}
