package de.unikl.cs.dbis.waves.parquet

import org.apache.parquet.io.api.Converter
import org.apache.spark.sql.types.{DataType,StructType,ArrayType}
import org.apache.commons.lang.NotImplementedException

trait LocalSchemaConverter extends Converter {
    def setPush(push : (Any) => Unit) : Unit
}

object LocalSchemaConverter {
    def converterFor(globalType: DataType) : LocalSchemaConverter = {
        globalType.typeName match {
            case "struct" => RowConverter(globalType.asInstanceOf[StructType])
            case "string" => new StringConverter()
            case "long" => new LongConverter()
            case "double" => new DoubleConverter()
            case "boolean" => new BooleanConverter()
            case "array" => ArrayConverter(globalType.asInstanceOf[ArrayType])
            case other => throw new NotImplementedException(other) //TODO implement missing types
        }
    }
}
