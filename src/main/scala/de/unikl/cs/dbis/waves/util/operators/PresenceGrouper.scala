package de.unikl.cs.dbis.waves.util.operators

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{concat,typedLit,when,col}
import org.apache.spark.sql.types.{StructType, DataType, StructField}

import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.nested.schemas._

/**
  * Determine the presence of all recursively optional nodes in the schema.
  *
  * This grouper provides a column function works on [[Row]]s that follow the
  * given schema and returns an array of Booleans which are true if the
  * respective node is present in that row. The array lists nodes in pre-order. 
  */
object PresenceGrouper extends AbstractGrouper(TempColumn("presence")) {
  override def apply(schema: StructType): Column
    = presence(schema, None).as(GROUP_COLUMN)

  private def presence(schema : DataType, pathContext : Option[PathKey]) : Column = schema match {
    case StructType(fields) => concat(fields.map(presence(_, pathContext)):_*)
    case _ => typedLit(Array.empty[Boolean])
  }

  private def presence(field : StructField, pathContext : Option[PathKey]) : Column = {
    val newPathContext = pathContext :+ field.name
    val rec = presence(field.dataType, newPathContext)
    if (field.nullable) {
      when(col(newPathContext.toSpark).isNull, typedLit(Array.fill(field.dataType.optionalNodeCount+1)(false)))
        .otherwise(concat(typedLit((Array(true))), rec))
    } else rec
  }
}
