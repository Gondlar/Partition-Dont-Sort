package de.unikl.cs.dbis.waves.parquet

import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.io.api.Binary

abstract class LocalSchemaPrimitiveConverter[T] extends PrimitiveConverter with LocalSchemaConverter {
    protected var push : (T) => Unit = null

    override def setPush(push: Any => Unit): Unit = {
        this.push = push
    }
}

class LongConverter extends LocalSchemaPrimitiveConverter[Long] {
    override def addLong(value: Long): Unit = push(value)
}

class DoubleConverter extends LocalSchemaPrimitiveConverter[Double] {
    override def addDouble(value: Double): Unit = push(value)
}

class BooleanConverter extends LocalSchemaPrimitiveConverter[Boolean] {
    override def addBoolean(value: Boolean): Unit = push(value)
}

class StringConverter extends LocalSchemaPrimitiveConverter[String] {
    override def addBinary(value: Binary): Unit = push(value.toStringUsingUTF8())
}
