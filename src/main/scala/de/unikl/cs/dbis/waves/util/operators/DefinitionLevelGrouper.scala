package de.unikl.cs.dbis.waves.util.operators

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{concat,typedLit,when,col}
import org.apache.spark.sql.types.{StructType, DataType, StructField}

import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.nested.schemas._


/**
  * Determine each row's definition levels for all optional leaf nodes in
  * the schema.
  * 
  * This grouper works on [[Row]]s that follow the given schema and
  * returns an array of integers containing the definition levels in the
  * same order as they appear in the schema.
  */
object DefinitionLevelGrouper extends Grouper(TempColumn("definition_levels")) {
  override def apply(schema: StructType): Column
    = definitionLevels(schema, false, None).as(GROUP_COLUMN)
  
  private def definitionLevels(schema : DataType, absentContext : Boolean, pathContext : Option[PathKey]) : Column = schema match {
      case StructType(fields) => concat(fields.map(definitionLevels(_, absentContext, pathContext)):_*)
      case _ if (absentContext) => typedLit(Array(0))
      case _ => typedLit(Array.empty[Int])
  }

  private def definitionLevels(field : StructField, absentContext : Boolean, pathContext : Option[PathKey]) : Column = {
      val deeper = pathContext :+ field.name // We know deeper is Some(_)
      val child = definitionLevels(field.dataType, absentContext || field.nullable, deeper)
      if (!field.nullable) return child
      when(col(deeper.toSpark).isNull, Array.fill(field.dataType.leafCount)(0))
          .otherwise(addX(child, 1))
  }
}
