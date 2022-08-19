package de.unikl.cs.dbis.waves.util.operators

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{typedLit,col,concat,when}
import org.apache.spark.sql.types.{StructType, StructField, DataType}

import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.nested.schemas._
import de.unikl.cs.dbis.waves.util.nested.DataTypeVisitor

object DefinitionLevels {

    private[operators] def definitionLevels(schema : DataType, absentContext : Boolean, pathContext : Option[PathKey]) : Column = schema match {
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

    private[operators] def presence(schema : DataType, pathContext : Option[PathKey]) : Column = schema match {
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