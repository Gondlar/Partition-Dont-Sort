package de.unikl.cs.dbis.waves.parquet

import org.apache.parquet.io.api.{GroupConverter,Converter}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

class RowConverter private (
    val defaults : Array[Any],
    val children : Array[LocalSchemaConverter],
    val schema : StructType
) extends GroupConverter with LocalSchemaConverter {
    private var push : (Row) => Unit = null

    override def setPush(push: Any => Unit): Unit = {
        this.push = push
    }

    private var row : Array[Any] = null

    override def getConverter(index: Int): Converter = children(index)

    override def start(): Unit = {
        row = defaults.clone()
    }

    override def end(): Unit = {
        assert(push != null, "Parent must set push function")
        push(new GenericRowWithSchema(row, schema)) //TODO type
    }
  
}

object RowConverter {
    def apply(globalSchema : StructType) : RowConverter = {
        val defaults = Array.fill[Any](globalSchema.length)(null)
        val childConverters = globalSchema.map(
            field => LocalSchemaConverter.converterFor(field.dataType)
        ).toArray
        val result = new RowConverter(defaults, childConverters, globalSchema)
        for (index <- 0 to childConverters.length-1) {
            result.children(index).setPush(v => result.row(index) = v )
        }
        result
    }
}
