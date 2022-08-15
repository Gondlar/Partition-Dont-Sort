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
}