package de.unikl.cs.dbis.waves.parquet

import org.apache.parquet.io.api.{GroupConverter,Converter}

import org.apache.spark.sql.types.{ArrayType,DataType}
import org.apache.spark.sql.Row
import org.apache.commons.lang.NotImplementedException

import scala.collection.mutable.ArrayBuffer

class ArrayConverter[T] private (
    private val innerConverter : ArrayConverter.InnerArrayConverter[T]
) extends GroupConverter with LocalSchemaConverter {
    protected var push : (Iterable[T]) => Unit = null

    override def setPush(push: Any => Unit): Unit = {
        this.push = push
    }

    private val contents = ArrayBuffer.empty[T]
    innerConverter.setPush(v => contents += v.asInstanceOf[T])

    override def getConverter(index: Int): Converter = {
        assert(index == 0)
        innerConverter
    }

    override def start(): Unit = contents.clear()

    override def end(): Unit = push(contents.toSeq)
}

object ArrayConverter {
    def apply(globalSchema : ArrayType) : LocalSchemaConverter = {
        val elementType = globalSchema.elementType
        val childConverter = LocalSchemaConverter.converterFor(elementType)
        elementType.typeName match {
            case "string" => apply[String](childConverter)
            case "long" => apply[Long](childConverter)
            case "double" => apply[Double](childConverter)
            case "boolean" => apply[Boolean](childConverter)
            case "struct" => apply[Row](childConverter)
            case "array" => apply[Iterable[_]](childConverter)
            case other => throw new NotImplementedException(other) //TODO implement missing types
        }
    }

    private def apply[T](childConverter : LocalSchemaConverter) : ArrayConverter[T] = {
        val innerConverter = new ArrayConverter.InnerArrayConverter[T](childConverter)
        new ArrayConverter[T](innerConverter)
    }

    private class InnerArrayConverter[T](
        val child : LocalSchemaConverter
    ) extends GroupConverter with LocalSchemaConverter {
        protected var push : (Any) => Unit = null

        override def setPush(push: Any => Unit): Unit = {
            this.push = push
        }

        var content : Option[T] = None
        child.setPush(v => content = Some(v.asInstanceOf[T]))

        override def getConverter(index: Int): Converter = {
            assert(index == 0)
            child
        }

        override def start(): Unit = {
            content = None
        }

        override def end(): Unit = push(content match {
            case Some(value) => value
            case None => null
        })
    }
}
